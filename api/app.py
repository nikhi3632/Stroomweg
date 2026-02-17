"""Stroomweg API — real-time Netherlands traffic data."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from .db import get_pool, close_pool
from .redis import get_redis, close_redis
from .routes import sites, speeds, journey_times, streams, ws


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app.state.pool = await get_pool()
    app.state.redis = await get_redis()
    yield
    # Shutdown
    await close_redis()
    await close_pool()


app = FastAPI(
    title="Stroomweg",
    description="Real-time Netherlands traffic intelligence API",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(sites.router)
app.include_router(speeds.router)
app.include_router(journey_times.router)
app.include_router(streams.router)
app.include_router(ws.router)


@app.get("/health")
async def health():
    pool = app.state.pool
    r = app.state.redis

    # DB check
    db_ok = False
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_ok = True
    except Exception:
        pass

    # Redis check
    redis_ok = False
    try:
        await r.ping()
        redis_ok = True
    except Exception:
        pass

    # Data freshness from DB
    speed_ts = None
    jt_ts = None
    try:
        async with pool.acquire() as conn:
            speed_ts = await conn.fetchval("SELECT MAX(timestamp) FROM speeds_raw")
            jt_ts = await conn.fetchval("SELECT MAX(timestamp) FROM journey_times_raw")
    except Exception:
        pass

    return {
        "status": "healthy" if db_ok and redis_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "redis": "connected" if redis_ok else "disconnected",
        "last_speed_update": speed_ts.isoformat() if speed_ts else None,
        "last_jt_update": jt_ts.isoformat() if jt_ts else None,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
