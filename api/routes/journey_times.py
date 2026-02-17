"""GET /journey-times — latest, per-site, history, and congestion."""

from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query, Request

from ..models import (
    JourneyTimeListResponse, JourneyTimeDetail,
    JourneyTimeHistoryResponse, CongestionResponse,
)

router = APIRouter(prefix="/journey-times", tags=["journey-times"])

RESOLUTION_TABLE = {
    "raw": "journey_times_raw",
    "5m": "journey_times_5m",
    "15m": "journey_times_15m",
    "1h": "journey_times_1h",
}

SORT_FIELDS = {
    "delay_sec": "(jt.duration_sec - jt.ref_duration_sec)",
    "delay_ratio": "(jt.duration_sec / NULLIF(jt.ref_duration_sec, 0))",
    "duration_sec": "jt.duration_sec",
    "quality": "jt.quality",
    "site_id": "jt.site_id",
}


def _parse_bbox(bbox: str):
    parts = [float(x) for x in bbox.split(",")]
    if len(parts) != 4:
        raise HTTPException(400, "bbox must be lat1,lon1,lat2,lon2")
    return parts


def _build_site_filter(bbox=None, road=None, site_id=None):
    conditions = []
    params = []
    idx = 1

    if bbox:
        lat1, lon1, lat2, lon2 = _parse_bbox(bbox)
        conditions.append(
            f"s.geom && ST_MakeEnvelope(${idx}, ${idx+1}, ${idx+2}, ${idx+3}, 4326)"
        )
        params.extend([lon1, lat1, lon2, lat2])
        idx += 4

    if road:
        conditions.append(f"s.road = ${idx}")
        params.append(road)
        idx += 1

    if site_id:
        conditions.append(f"jt.site_id = ${idx}")
        params.append(site_id)
        idx += 1

    return conditions, params, idx


def _parse_sort(sort: str | None, allowed: dict, default: str) -> str:
    """Parse sort parameter into SQL ORDER BY clause."""
    if not sort:
        return default

    desc = sort.startswith("-")
    field = sort.lstrip("-")

    if field not in allowed:
        raise HTTPException(400, f"sort must be one of: {', '.join(allowed.keys())} (prefix with - for descending)")

    direction = "DESC" if desc else "ASC"
    return f"{allowed[field]} {direction}"


def _jt_row_to_dict(r, include_meta=False):
    duration = float(r["duration_sec"]) if r["duration_sec"] is not None else None
    ref_duration = float(r["ref_duration_sec"]) if r["ref_duration_sec"] is not None else None

    result = {
        "site_id": r["site_id"],
        "timestamp": r["timestamp"].isoformat(),
        "duration_sec": duration,
        "ref_duration_sec": ref_duration,
        "delay_sec": round(duration - ref_duration, 2) if duration and ref_duration else None,
        "delay_ratio": round(duration / ref_duration, 3) if duration and ref_duration else None,
    }

    if "accuracy" in r.keys():
        result["accuracy"] = float(r["accuracy"]) if r["accuracy"] is not None else None
    if "quality" in r.keys():
        result["quality"] = float(r["quality"]) if r["quality"] is not None else None
    if "input_values" in r.keys():
        result["input_values"] = r["input_values"]

    if include_meta:
        result["name"] = r.get("name")
        result["road"] = r.get("road")
        result["lat"] = r.get("lat")
        result["lon"] = r.get("lon")

    return result


@router.get("", summary="Latest journey times", response_model=JourneyTimeListResponse, description="""
Latest journey time snapshot — duration, free-flow baseline, delay, and quality per segment. At least one filter required.

Sort: `?sort=delay_ratio` or `?sort=-delay_ratio` (prefix `-` for desc). Fields: `delay_sec`, `delay_ratio`, `duration_sec`, `quality`, `site_id`.

Use `min_quality` to filter out low-confidence readings.
""")
async def list_journey_times(
    request: Request,
    bbox: str | None = Query(None, description="Bounding box: lat1,lon1,lat2,lon2"),
    road: str | None = Query(None, description="Road name (e.g. A28, N201)"),
    site_id: str | None = Query(None, description="Specific segment ID"),
    min_quality: float | None = Query(None, ge=0, le=100, description="Minimum NDW quality score (0-100)"),
    sort: str | None = Query(None, description="Sort field (prefix with - for desc): delay_sec, delay_ratio, duration_sec, quality, site_id"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    if not any([bbox, road, site_id]):
        raise HTTPException(400, "At least one filter required: bbox, road, or site_id")

    order_by = _parse_sort(sort, SORT_FIELDS, "jt.site_id")
    pool = request.app.state.pool
    r = request.app.state.redis

    # Read latest timestamp from Redis (set by ingest service every cycle)
    ts_str = await r.get("jt:timestamp")
    if not ts_str:
        raise HTTPException(503, "No journey time data available yet")
    latest_ts = datetime.fromisoformat(ts_str)

    async with pool.acquire() as conn:

        conditions, params, idx = _build_site_filter(bbox, road, site_id)
        conditions.append(f"jt.timestamp = ${idx}")
        params.append(latest_ts)
        idx += 1

        if min_quality is not None:
            conditions.append(f"jt.quality >= ${idx}")
            params.append(min_quality)
            idx += 1

        where = "WHERE " + " AND ".join(conditions)

        total = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM journey_times_raw jt
            JOIN sites s ON s.site_id = jt.site_id
            {where}
            """,
            *params,
        )

        rows = await conn.fetch(
            f"""
            SELECT jt.site_id, jt.timestamp,
                   jt.duration_sec, jt.ref_duration_sec,
                   jt.accuracy, jt.quality, jt.input_values,
                   s.name, s.road, ST_Y(s.geom) AS lat, ST_X(s.geom) AS lon
            FROM journey_times_raw jt
            JOIN sites s ON s.site_id = jt.site_id
            {where}
            ORDER BY {order_by}
            LIMIT ${idx} OFFSET ${idx+1}
            """,
            *params, limit, offset,
        )

    return {
        "total_count": total,
        "limit": limit,
        "offset": offset,
        "data": [_jt_row_to_dict(r, include_meta=True) for r in rows],
    }


@router.get("/congestion", summary="Current congestion", response_model=CongestionResponse, description="""
Segments where `delay_ratio >= threshold` (default 1.5 = 50% slower than free-flow), sorted worst-first. At least one filter required.
""")
async def congestion(
    request: Request,
    bbox: str | None = Query(None, description="Bounding box: lat1,lon1,lat2,lon2"),
    road: str | None = Query(None, description="Road name (e.g. A2, A28)"),
    site_id: str | None = Query(None, description="Specific segment ID"),
    threshold: float = Query(1.5, ge=1.0, description="Minimum delay_ratio (1.5 = 50% slower than free-flow)"),
    min_quality: float | None = Query(None, ge=0, le=100, description="Minimum NDW quality score"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
):
    if not any([bbox, road, site_id]):
        raise HTTPException(400, "At least one filter required: bbox, road, or site_id")

    pool = request.app.state.pool
    r = request.app.state.redis

    ts_str = await r.get("jt:timestamp")
    if not ts_str:
        raise HTTPException(503, "No journey time data available yet")
    latest_ts = datetime.fromisoformat(ts_str)

    async with pool.acquire() as conn:
        conditions, params, idx = _build_site_filter(bbox, road, site_id)
        conditions.append(f"jt.timestamp = ${idx}")
        params.append(latest_ts)
        idx += 1

        # Only segments with valid durations and delay above threshold
        conditions.append("jt.duration_sec IS NOT NULL")
        conditions.append("jt.ref_duration_sec IS NOT NULL")
        conditions.append("jt.ref_duration_sec > 0")
        conditions.append(f"(jt.duration_sec / jt.ref_duration_sec) >= ${idx}")
        params.append(threshold)
        idx += 1

        if min_quality is not None:
            conditions.append(f"jt.quality >= ${idx}")
            params.append(min_quality)
            idx += 1

        where = "WHERE " + " AND ".join(conditions)

        total = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM journey_times_raw jt
            JOIN sites s ON s.site_id = jt.site_id
            {where}
            """,
            *params,
        )

        rows = await conn.fetch(
            f"""
            SELECT jt.site_id, jt.timestamp,
                   jt.duration_sec, jt.ref_duration_sec, jt.quality,
                   s.name, s.road, ST_Y(s.geom) AS lat, ST_X(s.geom) AS lon
            FROM journey_times_raw jt
            JOIN sites s ON s.site_id = jt.site_id
            {where}
            ORDER BY (jt.duration_sec / jt.ref_duration_sec) DESC
            LIMIT ${idx}
            """,
            *params, limit,
        )

    data = []
    for row in rows:
        duration = float(row["duration_sec"])
        ref = float(row["ref_duration_sec"])
        data.append({
            "site_id": row["site_id"],
            "name": row["name"],
            "timestamp": row["timestamp"].isoformat(),
            "duration_sec": duration,
            "ref_duration_sec": ref,
            "delay_sec": round(duration - ref, 2),
            "delay_ratio": round(duration / ref, 3),
            "quality": float(row["quality"]) if row["quality"] is not None else None,
            "road": row["road"],
            "lat": row["lat"],
            "lon": row["lon"],
        })

    return {
        "total_count": total,
        "threshold": threshold,
        "data": data,
    }


@router.get("/{site_id}", summary="Current journey time for one segment", response_model=JourneyTimeDetail, description="""
Latest journey time for a single segment, including computed delay.
""")
async def get_journey_time(request: Request, site_id: str):
    pool = request.app.state.pool

    async with pool.acquire() as conn:
        site_row = await conn.fetchrow(
            "SELECT name FROM sites WHERE site_id = $1", site_id
        )
        row = await conn.fetchrow(
            """
            SELECT site_id, timestamp, duration_sec, ref_duration_sec,
                   accuracy, quality, input_values
            FROM journey_times_raw
            WHERE site_id = $1
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            site_id,
        )

    if row is None:
        raise HTTPException(404, f"No journey time data for site {site_id}")

    result = _jt_row_to_dict(row)
    result["name"] = site_row["name"] if site_row else None
    return result


@router.get("/{site_id}/history", summary="Journey time history", response_model=JourneyTimeHistoryResponse, description="""
Historical journey times at multiple resolutions: `raw` (7d), `5m` (30d), `15m` (90d), `1h` (forever). Defaults to last hour at raw.
""")
async def get_journey_time_history(
    request: Request,
    site_id: str,
    start: datetime | None = Query(None, alias="from", description="Start time (ISO 8601). Default: 1 hour ago"),
    end: datetime | None = Query(None, alias="to", description="End time (ISO 8601). Default: now"),
    resolution: str = Query("raw", description="Time resolution: `raw`, `5m`, `15m`, or `1h`"),
    min_quality: float | None = Query(None, ge=0, le=100, description="Minimum NDW quality score"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum data points"),
):
    if resolution not in RESOLUTION_TABLE:
        raise HTTPException(400, f"resolution must be one of: {', '.join(RESOLUTION_TABLE.keys())}")

    table = RESOLUTION_TABLE[resolution]
    is_raw = resolution == "raw"
    time_col = "timestamp" if is_raw else "bucket"

    now = datetime.now(timezone.utc)
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=1)

    pool = request.app.state.pool

    if is_raw:
        quality_filter = ""
        params = [site_id, start, end, limit]
        if min_quality is not None:
            quality_filter = "AND quality >= $5"
            params.append(min_quality)

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT site_id, timestamp, duration_sec, ref_duration_sec,
                       accuracy, quality, input_values
                FROM {table}
                WHERE site_id = $1 AND timestamp >= $2 AND timestamp <= $3
                {quality_filter}
                ORDER BY timestamp DESC
                LIMIT $4
                """,
                *params,
            )

        return {
            "site_id": site_id,
            "resolution": resolution,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "count": len(rows),
            "data": [_jt_row_to_dict(r) for r in rows],
        }
    else:
        quality_filter = ""
        params = [site_id, start, end, limit]
        if min_quality is not None:
            quality_filter = "AND avg_quality >= $5"
            params.append(min_quality)

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT site_id, bucket AS timestamp,
                       avg_duration_sec AS duration_sec,
                       avg_ref_duration_sec AS ref_duration_sec,
                       avg_quality AS quality
                FROM {table}
                WHERE site_id = $1 AND bucket >= $2 AND bucket <= $3
                {quality_filter}
                ORDER BY bucket DESC
                LIMIT $4
                """,
                *params,
            )

        return {
            "site_id": site_id,
            "resolution": resolution,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "count": len(rows),
            "data": [_jt_row_to_dict(r) for r in rows],
        }
