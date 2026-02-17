"""Stroomweg API — real-time Netherlands traffic data."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from .db import get_pool, close_pool
from .redis import get_redis, close_redis
from .routes import sites, speeds, journey_times, streams, ws

DESCRIPTION = """
Live traffic speeds, journey times, and sensor data from **99,324 measurement sites**
across the Netherlands, updated every 60 seconds from [NDW](https://opendata.ndw.nu) open data feeds.

## Data Sources

| Feed | Update Interval | Coverage |
|------|----------------|----------|
| **Traffic Speeds** | 60s | ~20,000 sensor sites with per-lane speed (km/h) and flow (vehicles/hr) |
| **Journey Times** | 60s | ~79,000 route segments with travel duration, free-flow baseline, and delay |
| **Sensor Metadata** | Daily | 99,324 sites with coordinates, road name, lane count, equipment type |

## Quick Start

```bash
# Get all speed sensors on the A2 highway
curl "https://stroomweg-api-production.up.railway.app/speeds?road=A2"

# Get per-lane detail for a specific sensor
curl "https://stroomweg-api-production.up.railway.app/speeds/RWS01_MONIBAS_0161hrr0346ra?detail=lanes"

# Get journey times in Amsterdam (bounding box)
curl "https://stroomweg-api-production.up.railway.app/journey-times?bbox=52.3,4.8,52.4,5.0"

# Stream live speed updates via SSE
curl "https://stroomweg-api-production.up.railway.app/speeds/stream?road=A2"
```

## Filtering

All list endpoints support these filters (at least one required on speeds/journey-times):

| Parameter | Description | Example |
|-----------|-------------|---------|
| `bbox` | Bounding box: `lat1,lon1,lat2,lon2` | `52.3,4.8,52.4,5.0` |
| `road` | Road name | `A28`, `N201` |
| `site_id` | Specific sensor ID | `RWS01_MONIBAS_0161hrr0346ra` |

## Historical Data

History endpoints support multiple resolutions with automatic rollup:

| Resolution | Retention | Description |
|-----------|-----------|-------------|
| `raw` | 7 days | Full 60-second readings |
| `5m` | 30 days | 5-minute averages |
| `15m` | 90 days | 15-minute averages |
| `1h` | Forever | Hourly averages |

## Real-time Streaming

- **SSE**: `/speeds/stream` and `/journey-times/stream` — server-sent events, one update per minute
- **WebSocket**: `/ws` — subscribe to multiple feeds on one connection with JSON messages
"""

tags_metadata = [
    {
        "name": "sites",
        "description": "Discover sensor locations. 99,324 measurement sites across the Netherlands with coordinates, road names, and equipment type.",
    },
    {
        "name": "speeds",
        "description": "Real-time and historical traffic speeds. Per-lane speed (km/h) and vehicle flow (vehicles/hr) from ~20,000 sensor sites, updated every 60 seconds.",
    },
    {
        "name": "journey-times",
        "description": "Real-time and historical travel times. Duration, free-flow baseline, delay, and quality scores for ~79,000 route segments.",
    },
    {
        "name": "streams",
        "description": "Server-Sent Events (SSE) for live updates. Open a persistent connection and receive filtered speed or journey time updates every 60 seconds.",
    },
    {
        "name": "websocket",
        "description": "WebSocket endpoint for bidirectional real-time streaming. Subscribe and unsubscribe to multiple feeds on a single connection.",
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


@app.get("/health", tags=["health"])
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
