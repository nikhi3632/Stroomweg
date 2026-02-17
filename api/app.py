"""Stroomweg API — real-time Netherlands traffic data."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from .db import get_pool, close_pool
from .models import HealthResponse
from .redis import get_redis, close_redis
from .routes import sites, speeds, journey_times, streams, ws

DESCRIPTION = """
Live traffic speeds, journey times, and sensor data from **99,324 measurement sites**
across the Netherlands, updated every 60 seconds from [NDW](https://opendata.ndw.nu) open data feeds.

**Rate limits:** 60 req/min (advisory, not enforced). No auth required.
"""

tags_metadata = [
    {
        "name": "sites",
        "description": "99,324 sensor locations with coordinates, road names, and equipment type.",
    },
    {
        "name": "speeds",
        "description": "Real-time and historical per-lane speed (km/h) and flow (veh/hr) from ~20,000 sensors.",
    },
    {
        "name": "journey-times",
        "description": "Real-time and historical travel times, delay, and quality for ~79,000 route segments.",
    },
    {
        "name": "streams",
        "description": "Server-Sent Events (SSE) for live speed and journey time updates every 60s.",
    },
    {
        "name": "health",
        "description": "Service health and data freshness.",
    },
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await get_pool()
    app.state.redis = await get_redis()
    yield
    await close_redis()
    await close_pool()


app = FastAPI(
    title="Stroomweg",
    summary="Real-time Netherlands Traffic Intelligence API",
    description=DESCRIPTION,
    version="0.1.0",
    openapi_tags=tags_metadata,
    lifespan=lifespan,
    license_info={"name": "MIT"},
)

app.include_router(sites.router)
app.include_router(speeds.router)
app.include_router(journey_times.router)
app.include_router(streams.router)
app.include_router(ws.router)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "name": "Stroomweg",
        "description": "Real-time Netherlands Traffic Intelligence API",
        "version": "0.1.0",
        "data": {
            "sites": 99324,
            "speed_sensors": "~20,000 updated every 60s",
            "journey_time_segments": "~79,000 updated every 60s",
        },
        "endpoints": {
            "sites": "/sites",
            "speeds": "/speeds",
            "journey_times": "/journey-times",
            "speed_stream_sse": "/speeds/stream",
            "journey_time_stream_sse": "/journey-times/stream",
            "websocket": "/ws",
            "health": "/health",
            "docs": "/docs",
        },
        "source": "NDW (Nationale Databank Wegverkeersgegevens) — opendata.ndw.nu",
    }


@app.get("/health", tags=["health"], response_model=HealthResponse)
async def health():
    """Check service health, database/Redis connectivity, and data freshness."""
    pool = app.state.pool
    r = app.state.redis

    db_ok = False
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_ok = True
    except Exception:
        pass

    redis_ok = False
    try:
        await r.ping()
        redis_ok = True
    except Exception:
        pass

    speed_ts = None
    jt_ts = None
    try:
        speed_ts = await r.get("speeds:timestamp")
        jt_ts = await r.get("jt:timestamp")
    except Exception:
        pass

    return {
        "status": "healthy" if db_ok and redis_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "redis": "connected" if redis_ok else "disconnected",
        "last_speed_update": speed_ts or None,
        "last_jt_update": jt_ts or None,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
