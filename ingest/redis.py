"""Publish ingest snapshots to Redis for the API layer (pub/sub only)."""

import json
import logging

import redis.asyncio as aioredis

from .config import REDIS_URL

log = logging.getLogger(__name__)

_redis = None


async def get_redis():
    global _redis
    if _redis is None and REDIS_URL:
        _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


async def close_redis():
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None


async def publish_speeds(rows):
    """Publish speed snapshot to Redis pub/sub for SSE/WS subscribers.

    rows: list of (timestamp, site_id, lane, speed_kmh, flow_veh_hr)
    """
    r = await get_redis()
    if r is None:
        return

    # Group by site_id
    by_site = {}
    ts = None
    for timestamp, site_id, lane, speed, flow in rows:
        if ts is None:
            ts = timestamp.isoformat()
        if site_id not in by_site:
            by_site[site_id] = {"s": site_id, "t": ts, "l": []}
        by_site[site_id]["l"].append([lane, speed, flow])

    payload = json.dumps(list(by_site.values()), separators=(",", ":"))
    await r.set("speeds:timestamp", ts or "")
    await r.publish("speeds", payload)

    log.debug(f"Published {len(by_site)} speed sites to Redis pub/sub")


async def publish_journey_times(rows):
    """Publish journey time snapshot to Redis pub/sub for SSE/WS subscribers.

    rows: list of (timestamp, site_id, duration, ref_duration, accuracy, quality, input_values)
    """
    r = await get_redis()
    if r is None:
        return

    data = []
    ts = None
    for timestamp, site_id, duration, ref_duration, accuracy, quality, input_values in rows:
        if ts is None:
            ts = timestamp.isoformat()
        delay = round(duration - ref_duration, 2) if duration and ref_duration else None
        data.append([site_id, duration, ref_duration, delay, quality])

    payload = json.dumps({"t": ts, "d": data}, separators=(",", ":"))
    await r.set("jt:timestamp", ts or "")
    await r.publish("journey-times", payload)

    log.debug(f"Published {len(data)} journey time segments to Redis pub/sub")
