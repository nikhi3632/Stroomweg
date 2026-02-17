# Stroomweg

Real-time traffic intelligence API for the Netherlands. Ingests live speed and journey time data from **99,324 sensor sites** across every Dutch highway, updated every 60 seconds.

**Live API:** https://stroomweg-api-production.up.railway.app

**Interactive docs:** https://stroomweg-api-production.up.railway.app/docs

> *Stroomweg* is Dutch for "expressway."

## Try it

```bash
# Speeds on the A2 highway
curl "https://stroomweg-api-production.up.railway.app/speeds?road=A2"

# Journey times in Amsterdam (bounding box)
curl "https://stroomweg-api-production.up.railway.app/journey-times?bbox=52.3,4.8,52.4,5.0"

# Live speed stream (Server-Sent Events)
curl -N "https://stroomweg-api-production.up.railway.app/speeds/stream?road=A2"
```

## Architecture

```
              NDW Open Data
              (DATEX II XML)
                    |
                    |  every 60s
                    v
            +--------------+            +--------------+
            |    Ingest    | --publish->|    Redis     |
            |   Service    |            |  Pub/Sub     |
            +------+-------+            +------+-------+
                   |                           |
                   | insert                    | push
                   v                           v
            +--------------+  SELECT    +--------------+
            | TimescaleDB  | ---------> |   FastAPI    |
            |  + PostGIS   |            | REST/SSE/WS  |
            |              |            +------+-------+
            | raw>5m>15m>1h|                   |
            +--------------+                   v
                                           Clients
```

**Ingest service** polls two NDW DATEX II XML feeds every 60 seconds, parses them with streaming `lxml.iterparse`, and batch-inserts into TimescaleDB hypertables. Each cycle also publishes a compact JSON snapshot to Redis pub/sub for real-time streaming clients.

**API service** serves REST endpoints from TimescaleDB, with SSE and WebSocket streams powered by Redis pub/sub. PostGIS enables spatial queries (bounding box filtering). TimescaleDB continuous aggregates automatically roll up raw data into 5m, 15m, and 1h resolutions with configurable retention policies.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /speeds` | Latest speed snapshot (avg across lanes, per site) |
| `GET /speeds/{site_id}` | Current speed at one sensor, optional `?detail=lanes` |
| `GET /speeds/{site_id}/history` | Historical speeds (raw / 5m / 15m / 1h resolution) |
| `GET /journey-times` | Latest journey times with delay computation |
| `GET /journey-times/{site_id}` | Current journey time for one segment |
| `GET /journey-times/{site_id}/history` | Historical journey times (multi-resolution) |
| `GET /sites` | Search 99,324 sensor sites by road name or bounding box |
| `GET /sites/{site_id}` | Full details for a single site |
| `GET /speeds/stream` | SSE stream of live speed updates |
| `GET /journey-times/stream` | SSE stream of live journey time updates |
| `WS /ws` | WebSocket with JSON subscribe/unsubscribe protocol |
| `GET /health` | Service health, DB/Redis connectivity, data freshness |

### Filtering

All list endpoints support these filters (at least one required on speeds/journey-times):

| Parameter | Example | Description |
|-----------|---------|-------------|
| `road` | `A2`, `N201` | Road name |
| `bbox` | `52.3,4.8,52.4,5.0` | Bounding box: lat1,lon1,lat2,lon2 |
| `site_id` | `RWS01_MONIBAS_0161hrr0346ra` | Specific sensor ID |

### Historical Resolutions

| Resolution | Retention | Use case |
|-----------|-----------|----------|
| `raw` | 7 days | Full 60-second granularity |
| `5m` | 30 days | Dashboard charts |
| `15m` | 90 days | Trend analysis |
| `1h` | Forever | Long-term patterns |

### WebSocket Protocol

```jsonc
// Connect
wss://stroomweg-api-production.up.railway.app/ws

// Subscribe to speeds on a specific road
{"subscribe": "speeds", "road": "A2"}
// → {"subscribed": "speeds", "filter_count": 142}

// Subscribe to journey times in a bbox
{"subscribe": "journey-times", "bbox": "52.3,4.8,52.4,5.0"}
// → {"subscribed": "journey-times", "filter_count": 38}

// Unsubscribe
{"unsubscribe": "speeds"}
// → {"unsubscribed": "speeds"}
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| API framework | FastAPI + Uvicorn |
| Database | TimescaleDB (hypertables, continuous aggregates, retention policies) |
| Spatial queries | PostGIS (GiST indexes, bounding box filtering) |
| Real-time messaging | Redis pub/sub |
| Streaming | Server-Sent Events (sse-starlette) + WebSocket |
| Data source | NDW DATEX II XML (gzipped, parsed with lxml iterparse) |
| Deployment | Railway (two services from one repo) |
| Language | Python 3.14+, fully async (asyncpg, httpx, redis-py) |

## Project Structure

```
Stroomweg/
├── api/                        # FastAPI application
│   ├── app.py                  # App setup, lifespan, health, landing page
│   ├── config.py               # Environment variables
│   ├── db.py                   # asyncpg connection pool
│   ├── redis.py                # Redis connection
│   └── routes/
│       ├── sites.py            # GET /sites, GET /sites/{id}
│       ├── speeds.py           # GET /speeds, history, per-site
│       ├── journey_times.py    # GET /journey-times, history, per-site
│       ├── streams.py          # SSE endpoints
│       └── ws.py               # WebSocket endpoint
│
├── ingest/                     # Data ingestion service
│   ├── main.py                 # Polling loop (60s cycle)
│   ├── config.py               # URLs and environment
│   ├── db.py                   # asyncpg connection pool
│   ├── redis.py                # Publish to Redis pub/sub
│   ├── reference.py            # Parse measurement.xml.gz → sites table
│   ├── speeds.py               # Parse trafficspeed.xml.gz → speeds_raw
│   └── journey_times.py        # Parse traveltime.xml.gz → journey_times_raw
│
├── scripts/                    # Database management
│   ├── migrate.py              # Schema: tables, hypertables, aggregates, policies
│   ├── db_count.py             # Row counts
│   ├── db_reset.py             # Drop and recreate (destructive)
│   ├── db_delete.py            # Drop all tables (destructive)
│   ├── db_url.py               # Print connection string for psql
│   └── ingest_status.py        # Check ingest service status
│
├── Makefile                    # Development and deployment commands
├── requirements.txt            # Python dependencies
└── railway.json                # Railway deployment config
```

## Database Schema

Raw readings land in TimescaleDB **hypertables** (`speeds_raw`, `journey_times_raw`), which are automatically chunked by time. **Continuous aggregates** roll these up into `5m`, `15m`, and `1h` buckets. **Retention policies** drop old data automatically (raw after 7 days, 5m after 30 days, 15m after 90 days, 1h kept forever).

The `sites` table holds 99,324 rows with PostGIS geometry and GiST/B-tree indexes for spatial and road-name queries.

## Data Source

All data comes from [NDW](https://opendata.ndw.nu/) (Nationale Databank Wegverkeersgegevens), the Netherlands' national traffic data warehouse. Three DATEX II XML feeds are consumed:

| Feed | Size | Content |
|------|------|---------|
| `measurement.xml.gz` | ~35 MB | Site metadata: coordinates, road name, lanes, equipment |
| `trafficspeed.xml.gz` | ~7 MB | Per-lane speed (km/h) and vehicle flow (veh/hr) |
| `traveltime.xml.gz` | ~15 MB | Route segment duration, free-flow baseline, delay, quality |

## Running Locally

**Prerequisites:** Python 3.14+, a [TimescaleDB](https://www.timescale.com/) instance with PostGIS, and a [Redis](https://redis.io/) instance.

```bash
# Clone and install
git clone https://github.com/nikhi3632/Stroomweg.git
cd Stroomweg
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your DATABASE_URL and REDIS_URL

# Run database migrations
make migrate

# Start the API (development mode with hot reload)
make api

# In another terminal, start the ingest service
python -m ingest.main
```

## Deployment

The project runs as two Railway services deployed from one repository:

| Service | Start Command | Purpose |
|---------|--------------|---------|
| `Stroomweg` | `python -u -m ingest.main` | Ingest service (polls NDW every 60s) |
| `Stroomweg-API` | `uvicorn api.app:app --host 0.0.0.0 --port ${PORT:-8000}` | REST API + streaming |

Each service gets its own `NIXPACKS_START_CMD` environment variable on Railway.

```bash
# Deploy
make api-start      # Deploy API service
make ingest-start   # Deploy ingest service

# Monitor
make api-logs       # View API logs
make ingest-logs    # View ingest logs
make db-count       # Check row counts
```

## License

MIT
