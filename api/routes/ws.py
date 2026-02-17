"""WebSocket /ws — bidirectional multi-feed subscriptions."""

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from .streams import _expand_speed_payload, _expand_jt_payload

router = APIRouter(tags=["websocket"])
log = logging.getLogger(__name__)

CHANNEL_EXPANDERS = {
    "speeds": _expand_speed_payload,
    "journey-times": _expand_jt_payload,
}


async def _resolve_site_ids(pool, filters):
    """Resolve bbox/road/site_id filters to a set of matching site_ids."""
    site_id = filters.get("site_id")
    if site_id:
        return {site_id}

    bbox = filters.get("bbox")
    road = filters.get("road")

    if not bbox and not road:
        return None  # no filter = all sites

    conditions = []
    params = []
    idx = 1

    if bbox:
        parts = [float(x) for x in bbox.split(",")]
        if len(parts) != 4:
            return set()
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

    where = "WHERE " + " AND ".join(conditions)
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT site_id FROM sites {where}", *params)

    return {r["site_id"] for r in rows}


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    pool = websocket.app.state.pool
    r = websocket.app.state.redis

    # Track active subscriptions: {"speeds": set_of_site_ids, "journey-times": set_of_site_ids}
    subscriptions = {}
    pubsub = r.pubsub()
    listener_task = None

    async def redis_listener():
        """Listen to Redis pub/sub and forward matching messages to the client."""
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                channel = message["channel"]
                if channel not in subscriptions:
                    continue

                raw_data = json.loads(message["data"])
                expander = CHANNEL_EXPANDERS.get(channel)
                data = expander(raw_data) if expander else raw_data

                matching_ids = subscriptions[channel]
                if matching_ids is not None:
                    data = [d for d in data if d["site_id"] in matching_ids]

                if data:
                    await websocket.send_json({
                        "event": channel,
                        "data": data,
                    })
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("WebSocket Redis listener error")

    try:
        while True:
            msg = await websocket.receive_json()

            if msg.get("subscribe"):
                channel = msg["subscribe"]
                if channel not in ("speeds", "journey-times"):
                    await websocket.send_json({"error": "Unknown channel. Use 'speeds' or 'journey-times'"})
                    continue

                matching_ids = await _resolve_site_ids(pool, msg)
                subscriptions[channel] = matching_ids
                await pubsub.subscribe(channel)

                # Start listener if not running
                if listener_task is None or listener_task.done():
                    listener_task = asyncio.create_task(redis_listener())

                await websocket.send_json({
                    "subscribed": channel,
                    "filter_count": len(matching_ids) if matching_ids else "all",
                })

            elif msg.get("unsubscribe"):
                channel = msg["unsubscribe"]
                if channel in subscriptions:
                    del subscriptions[channel]
                    await pubsub.unsubscribe(channel)
                    await websocket.send_json({"unsubscribed": channel})

            else:
                await websocket.send_json({"error": "Send {subscribe: 'speeds'} or {unsubscribe: 'speeds'}"})

    except WebSocketDisconnect:
        pass
    finally:
        if listener_task and not listener_task.done():
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass
        await pubsub.unsubscribe()
        await pubsub.aclose()
