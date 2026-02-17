"""GET /speeds — latest, per-site, and history."""

from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query, Request

from ..models import SpeedListResponse, SpeedSiteAggregate, SpeedSiteLanes, SpeedHistoryResponse

router = APIRouter(prefix="/speeds", tags=["speeds"])

RESOLUTION_TABLE = {
    "raw": "speeds_raw",
    "5m": "speeds_5m",
    "15m": "speeds_15m",
    "1h": "speeds_1h",
}

RESOLUTION_COLUMNS = {
    "raw": ("speed_kmh", "flow_veh_hr", "timestamp"),
    "5m": ("avg_speed_kmh", "avg_flow_veh_hr", "bucket"),
    "15m": ("avg_speed_kmh", "avg_flow_veh_hr", "bucket"),
    "1h": ("avg_speed_kmh", "avg_flow_veh_hr", "bucket"),
}

SORT_FIELDS = {
    "speed_kmh": "speed_kmh",
    "flow_veh_hr": "flow_veh_hr",
    "site_id": "sp.site_id",
}


def _parse_bbox(bbox: str):
    parts = [float(x) for x in bbox.split(",")]
    if len(parts) != 4:
        raise HTTPException(400, "bbox must be lat1,lon1,lat2,lon2")
    return parts  # lat1, lon1, lat2, lon2


def _build_site_filter(bbox=None, road=None, site_id=None):
    """Build WHERE clause fragments for site filtering."""
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
        conditions.append(f"sp.site_id = ${idx}")
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


@router.get("", summary="Latest speeds", response_model=SpeedListResponse, description="""
Latest speed snapshot — one aggregated reading per site (avg speed across lanes, summed flow). At least one filter required.

Sort: `?sort=speed_kmh` or `?sort=-speed_kmh` (prefix `-` for desc). Fields: `speed_kmh`, `flow_veh_hr`, `site_id`.

NDW speed data has no quality score — use journey-time endpoints for quality filtering.
""")
async def list_speeds(
    request: Request,
    bbox: str | None = Query(None, description="Bounding box: lat1,lon1,lat2,lon2"),
    road: str | None = Query(None, description="Road name (e.g. A2, A28)"),
    site_id: str | None = Query(None, description="Specific site ID"),
    sort: str | None = Query(None, description="Sort field (prefix with - for desc): speed_kmh, flow_veh_hr, site_id"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    if not any([bbox, road, site_id]):
        raise HTTPException(400, "At least one filter required: bbox, road, or site_id")

    order_by = _parse_sort(sort, SORT_FIELDS, "sp.site_id")
    pool = request.app.state.pool
    r = request.app.state.redis

    # Read latest timestamp from Redis (set by ingest service every cycle)
    ts_str = await r.get("speeds:timestamp")
    if not ts_str:
        raise HTTPException(503, "No speed data available yet")
    latest_ts = datetime.fromisoformat(ts_str)

    async with pool.acquire() as conn:

        conditions, params, idx = _build_site_filter(bbox, road, site_id)
        conditions.append(f"sp.timestamp = ${idx}")
        params.append(latest_ts)
        idx += 1

        where = "WHERE " + " AND ".join(conditions)

        # Count total matching sites
        total = await conn.fetchval(
            f"""
            SELECT COUNT(DISTINCT sp.site_id)
            FROM speeds_raw sp
            JOIN sites s ON s.site_id = sp.site_id
            {where}
            """,
            *params,
        )

        # Get aggregated speeds per site with pagination
        rows = await conn.fetch(
            f"""
            SELECT sp.site_id,
                   s.name,
                   sp.timestamp,
                   AVG(sp.speed_kmh) AS speed_kmh,
                   SUM(sp.flow_veh_hr) AS flow_veh_hr,
                   s.road, ST_Y(s.geom) AS lat, ST_X(s.geom) AS lon
            FROM speeds_raw sp
            JOIN sites s ON s.site_id = sp.site_id
            {where}
            GROUP BY sp.site_id, sp.timestamp, s.name, s.road, s.geom
            ORDER BY {order_by}
            LIMIT ${idx} OFFSET ${idx+1}
            """,
            *params, limit, offset,
        )

    return {
        "total_count": total,
        "limit": limit,
        "offset": offset,
        "data": [
            {
                "site_id": r["site_id"],
                "name": r["name"],
                "timestamp": r["timestamp"].isoformat(),
                "speed_kmh": round(float(r["speed_kmh"]), 1) if r["speed_kmh"] is not None else None,
                "flow_veh_hr": int(r["flow_veh_hr"]) if r["flow_veh_hr"] is not None else None,
                "road": r["road"],
                "lat": r["lat"],
                "lon": r["lon"],
            }
            for r in rows
        ],
    }


@router.get("/{site_id}", summary="Current speed at one site", response_model=SpeedSiteAggregate | SpeedSiteLanes, description="""
Latest speed for a single sensor. Returns aggregate by default; use `?detail=lanes` for per-lane breakdown.
""")
async def get_speed(
    request: Request,
    site_id: str,
    detail: str | None = Query(None, description="Set to `lanes` for per-lane breakdown"),
):
    pool = request.app.state.pool
    r = request.app.state.redis

    # Get site name
    async with pool.acquire() as conn:
        site_row = await conn.fetchrow(
            "SELECT name FROM sites WHERE site_id = $1", site_id
        )

    site_name = site_row["name"] if site_row else None

    # Try Redis for the latest timestamp, fall back to DB
    ts_str = await r.get("speeds:timestamp")
    if ts_str:
        latest_ts = datetime.fromisoformat(ts_str)
    else:
        async with pool.acquire() as conn:
            latest_ts = await conn.fetchval(
                "SELECT timestamp FROM speeds_raw WHERE site_id = $1 ORDER BY timestamp DESC LIMIT 1",
                site_id,
            )
        if latest_ts is None:
            raise HTTPException(404, f"No speed data for site {site_id}")

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT lane, speed_kmh, flow_veh_hr
            FROM speeds_raw
            WHERE site_id = $1 AND timestamp = $2
            ORDER BY lane
            """,
            site_id, latest_ts,
        )

    if not rows:
        raise HTTPException(404, f"No speed data for site {site_id}")

    if detail == "lanes":
        return {
            "site_id": site_id,
            "name": site_name,
            "timestamp": latest_ts.isoformat(),
            "lanes": [
                {
                    "lane": r["lane"],
                    "speed_kmh": float(r["speed_kmh"]) if r["speed_kmh"] is not None else None,
                    "flow_veh_hr": int(r["flow_veh_hr"]) if r["flow_veh_hr"] is not None else None,
                }
                for r in rows
            ],
        }

    speeds = [float(r["speed_kmh"]) for r in rows if r["speed_kmh"] is not None]
    flows = [int(r["flow_veh_hr"]) for r in rows if r["flow_veh_hr"] is not None]

    return {
        "site_id": site_id,
        "name": site_name,
        "timestamp": latest_ts.isoformat(),
        "speed_kmh": round(sum(speeds) / len(speeds), 1) if speeds else None,
        "flow_veh_hr": sum(flows) if flows else None,
    }


@router.get("/{site_id}/history", summary="Speed history", response_model=SpeedHistoryResponse, description="""
Historical speeds at multiple resolutions: `raw` (7d), `5m` (30d), `15m` (90d), `1h` (forever). Defaults to last hour at raw.
""")
async def get_speed_history(
    request: Request,
    site_id: str,
    start: datetime | None = Query(None, alias="from", description="Start time (ISO 8601). Default: 1 hour ago"),
    end: datetime | None = Query(None, alias="to", description="End time (ISO 8601). Default: now"),
    resolution: str = Query("raw", description="Time resolution: `raw`, `5m`, `15m`, or `1h`"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum data points"),
):
    if resolution not in RESOLUTION_TABLE:
        raise HTTPException(400, f"resolution must be one of: {', '.join(RESOLUTION_TABLE.keys())}")

    table = RESOLUTION_TABLE[resolution]
    speed_col, flow_col, time_col = RESOLUTION_COLUMNS[resolution]

    now = datetime.now(timezone.utc)
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=1)

    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT {time_col} AS timestamp, lane,
                   {speed_col} AS speed_kmh, {flow_col} AS flow_veh_hr
            FROM {table}
            WHERE site_id = $1 AND {time_col} >= $2 AND {time_col} <= $3
            ORDER BY {time_col} DESC
            LIMIT $4
            """,
            site_id, start, end, limit,
        )

    return {
        "site_id": site_id,
        "resolution": resolution,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "count": len(rows),
        "data": [
            {
                "timestamp": r["timestamp"].isoformat(),
                "lane": r["lane"],
                "speed_kmh": float(r["speed_kmh"]) if r["speed_kmh"] is not None else None,
                "flow_veh_hr": int(r["flow_veh_hr"]) if r["flow_veh_hr"] is not None else None,
            }
            for r in rows
        ],
    }
