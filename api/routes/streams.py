"""SSE streaming: GET /speeds/stream and GET /journey-times/stream."""

import json

from fastapi import APIRouter, HTTPException, Query, Request
from sse_starlette.sse import EventSourceResponse

router = APIRouter(tags=["streams"])


async def _get_matching_site_ids(pool, bbox=None, road=None, site_id=None):
    """Resolve filters to a set of site_ids."""
    if site_id:
        return {site_id}

    conditions = []
    params = []
    idx = 1

    if bbox:
        parts = [float(x) for x in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(400, "bbox must be lat1,lon1,lat2,lon2")
        lat1, lon1, lat2, lon2 = parts
        conditions.append(
            f"ST_Within(geom, ST_MakeEnvelope(${idx}, ${idx+1}, ${idx+2}, ${idx+3}, 4326))"
        )
        params.extend([lon1, lat1, lon2, lat2])
        idx += 4

    if road:
        conditions.append(f"road = ${idx}")
        params.append(road)
        idx += 1

    if not conditions:
        return None

    where = "WHERE " + " AND ".join(conditions)
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT site_id FROM sites {where}", *params)

    return {r["site_id"] for r in rows}


def _expand_speed_payload(raw_data):
    """Expand compact speed pub/sub format to full JSON.

    Compact: [{"s": site_id, "t": ts, "l": [[lane, speed, flow], ...]}, ...]
    Full: [{"site_id": ..., "timestamp": ..., "lanes": [{"lane": ..., "speed_kmh": ..., "flow_veh_hr": ...}]}, ...]
    """
    result = []
    for entry in raw_data:
        result.append({
            "site_id": entry["s"],
            "timestamp": entry["t"],
            "lanes": [
                {"lane": l[0], "speed_kmh": l[1], "flow_veh_hr": l[2]}
                for l in entry["l"]
            ],
        })
    return result


def _expand_jt_payload(raw_data):
    """Expand compact journey time pub/sub format to full JSON.

    Compact: {"t": ts, "d": [[site_id, duration, ref_duration, delay, quality], ...]}
    Full: [{"site_id": ..., "timestamp": ..., "duration_sec": ..., ...}, ...]
    """
    ts = raw_data["t"]
    result = []
    for row in raw_data["d"]:
        site_id, duration, ref_duration, delay, quality = row
        result.append({
            "site_id": site_id,
            "timestamp": ts,
            "duration_sec": duration,
            "ref_duration_sec": ref_duration,
            "delay_sec": delay,
            "quality": quality,
        })
    return result


@router.get("/speeds/stream")
async def speed_stream(
    request: Request,
    bbox: str | None = Query(None),
    road: str | None = Query(None),
    site_id: str | None = Query(None),
):
    """SSE stream of speed updates, filtered by bbox/road/site_id."""
    if not any([bbox, road, site_id]):
        raise HTTPException(400, "At least one filter required: bbox, road, or site_id")

    pool = request.app.state.pool
    matching_ids = await _get_matching_site_ids(pool, bbox, road, site_id)

    async def event_generator():
        r = request.app.state.redis
        pubsub = r.pubsub()
        await pubsub.subscribe("speeds")
        try:
            async for message in pubsub.listen():
                if await request.is_disconnected():
                    break
                if message["type"] != "message":
                    continue

                raw_data = json.loads(message["data"])
                data = _expand_speed_payload(raw_data)

                if matching_ids is not None:
                    data = [d for d in data if d["site_id"] in matching_ids]

                if data:
                    yield {"event": "speeds", "data": json.dumps(data)}
        finally:
            await pubsub.unsubscribe("speeds")
            await pubsub.aclose()

    return EventSourceResponse(event_generator())


@router.get("/journey-times/stream")
async def journey_time_stream(
    request: Request,
    bbox: str | None = Query(None),
    road: str | None = Query(None),
    site_id: str | None = Query(None),
    min_quality: float | None = Query(None, ge=0, le=100),
):
    """SSE stream of journey time updates, filtered by bbox/road/site_id."""
    if not any([bbox, road, site_id]):
        raise HTTPException(400, "At least one filter required: bbox, road, or site_id")

    pool = request.app.state.pool
    matching_ids = await _get_matching_site_ids(pool, bbox, road, site_id)

    async def event_generator():
        r = request.app.state.redis
        pubsub = r.pubsub()
        await pubsub.subscribe("journey-times")
        try:
            async for message in pubsub.listen():
                if await request.is_disconnected():
                    break
                if message["type"] != "message":
                    continue

                raw_data = json.loads(message["data"])
                data = _expand_jt_payload(raw_data)

                if matching_ids is not None:
                    data = [d for d in data if d["site_id"] in matching_ids]

                if min_quality is not None:
                    data = [d for d in data if d.get("quality") is not None and d["quality"] >= min_quality]

                if data:
                    yield {"event": "journey-times", "data": json.dumps(data)}
        finally:
            await pubsub.unsubscribe("journey-times")
            await pubsub.aclose()

    return EventSourceResponse(event_generator())
