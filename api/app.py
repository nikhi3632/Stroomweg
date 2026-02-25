"""Stroomweg API — real-time Netherlands traffic data."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from .db import get_pool, close_pool
from .models import HealthResponse
from .redis import get_redis, close_redis
from .routes import sites, speeds, journey_times, streams, ws

RATE_LIMIT = 60  # requests per window
RATE_WINDOW = 60  # seconds

RATE_LIMIT_SKIP = frozenset({"/", "/health", "/docs", "/openapi.json", "/redoc"})


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in RATE_LIMIT_SKIP or request.url.path.startswith("/ws"):
            return await call_next(request)

        ip = request.client.host if request.client else "unknown"
        key = f"ratelimit:{ip}"

        try:
            r = request.app.state.redis
            count = await r.incr(key)
            if count == 1:
                await r.expire(key, RATE_WINDOW)
            ttl = await r.ttl(key)
        except Exception:
            return await call_next(request)

        headers = {
            "X-RateLimit-Limit": str(RATE_LIMIT),
            "X-RateLimit-Remaining": str(max(0, RATE_LIMIT - count)),
            "X-RateLimit-Reset": str(max(0, ttl)),
        }

        if count > RATE_LIMIT:
            return JSONResponse(
                status_code=429,
                content={"detail": f"Rate limit exceeded. {RATE_LIMIT} requests per minute allowed."},
                headers={**headers, "Retry-After": str(max(0, ttl))},
            )

        response = await call_next(request)
        for k, v in headers.items():
            response.headers[k] = v
        return response

DESCRIPTION = """
Live traffic speeds, journey times, and sensor data from **99,324 measurement sites**
across the Netherlands, updated every 60 seconds from [NDW](https://opendata.ndw.nu) open data feeds.

**Rate limits:** 60 req/min per IP. No auth required.
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

app.add_middleware(RateLimitMiddleware)

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
