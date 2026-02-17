"""Create all tables, hypertables, continuous aggregates, and policies."""

import asyncio
import os
import sys

import asyncpg
from dotenv import load_dotenv

load_dotenv()

MIGRATIONS = [
    # Extensions
    "CREATE EXTENSION IF NOT EXISTS postgis",
    "CREATE EXTENSION IF NOT EXISTS timescaledb",

    # Sites table (regular PostgreSQL — reference data)
    """
    CREATE TABLE IF NOT EXISTS sites (
        site_id TEXT PRIMARY KEY,
        name TEXT,
        road TEXT,
        lanes SMALLINT,
        equipment TEXT,
        direction TEXT,
        geom GEOMETRY(Point, 4326),
        municipality TEXT,
        has_speed BOOLEAN DEFAULT FALSE,
        has_travel_time BOOLEAN DEFAULT FALSE,
        index_mapping JSONB,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_sites_geom ON sites USING GIST (geom)",
    "CREATE INDEX IF NOT EXISTS idx_sites_road ON sites (road)",
    "CREATE INDEX IF NOT EXISTS idx_sites_municipality ON sites (municipality)",

    # speeds_raw hypertable
    """
    CREATE TABLE IF NOT EXISTS speeds_raw (
        timestamp TIMESTAMPTZ NOT NULL,
        site_id TEXT NOT NULL,
        lane SMALLINT NOT NULL,
        speed_kmh REAL,
        flow_veh_hr INTEGER,
        UNIQUE (timestamp, site_id, lane)
    )
    """,
    "SELECT create_hypertable('speeds_raw', 'timestamp', chunk_time_interval => INTERVAL '6 hours', if_not_exists => TRUE)",
    "CREATE INDEX IF NOT EXISTS idx_speeds_site_time ON speeds_raw (site_id, timestamp DESC)",

    # journey_times_raw hypertable
    """
    CREATE TABLE IF NOT EXISTS journey_times_raw (
        timestamp TIMESTAMPTZ NOT NULL,
        site_id TEXT NOT NULL,
        duration_sec REAL,
        ref_duration_sec REAL,
        accuracy REAL,
        quality REAL,
        input_values INTEGER,
        UNIQUE (timestamp, site_id)
    )
    """,
    "SELECT create_hypertable('journey_times_raw', 'timestamp', chunk_time_interval => INTERVAL '3 hours', if_not_exists => TRUE)",
    "CREATE INDEX IF NOT EXISTS idx_jt_site_time ON journey_times_raw (site_id, timestamp DESC)",

    # Continuous aggregates — speeds
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS speeds_5m
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('5 minutes', timestamp) AS bucket,
           site_id, lane,
           AVG(speed_kmh) AS avg_speed_kmh,
           AVG(flow_veh_hr) AS avg_flow_veh_hr
    FROM speeds_raw
    GROUP BY bucket, site_id, lane
    WITH NO DATA
    """,
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS speeds_15m
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('15 minutes', timestamp) AS bucket,
           site_id, lane,
           AVG(speed_kmh) AS avg_speed_kmh,
           AVG(flow_veh_hr) AS avg_flow_veh_hr
    FROM speeds_raw
    GROUP BY bucket, site_id, lane
    WITH NO DATA
    """,
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS speeds_1h
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 hour', timestamp) AS bucket,
           site_id, lane,
           AVG(speed_kmh) AS avg_speed_kmh,
           AVG(flow_veh_hr) AS avg_flow_veh_hr
    FROM speeds_raw
    GROUP BY bucket, site_id, lane
    WITH NO DATA
    """,

    # Continuous aggregates — journey times
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS journey_times_5m
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('5 minutes', timestamp) AS bucket,
           site_id,
           AVG(duration_sec) AS avg_duration_sec,
           AVG(ref_duration_sec) AS avg_ref_duration_sec,
           AVG(quality) AS avg_quality
    FROM journey_times_raw
    GROUP BY bucket, site_id
    WITH NO DATA
    """,
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS journey_times_15m
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('15 minutes', timestamp) AS bucket,
           site_id,
           AVG(duration_sec) AS avg_duration_sec,
           AVG(ref_duration_sec) AS avg_ref_duration_sec,
           AVG(quality) AS avg_quality
    FROM journey_times_raw
    GROUP BY bucket, site_id
    WITH NO DATA
    """,
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS journey_times_1h
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 hour', timestamp) AS bucket,
           site_id,
           AVG(duration_sec) AS avg_duration_sec,
           AVG(ref_duration_sec) AS avg_ref_duration_sec,
           AVG(quality) AS avg_quality
    FROM journey_times_raw
    GROUP BY bucket, site_id
    WITH NO DATA
    """,

    # Refresh policies
    "SELECT add_continuous_aggregate_policy('speeds_5m', start_offset => INTERVAL '15 minutes', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE)",
    "SELECT add_continuous_aggregate_policy('speeds_15m', start_offset => INTERVAL '1 hour', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE)",
    "SELECT add_continuous_aggregate_policy('speeds_1h', start_offset => INTERVAL '4 hours', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 hour', if_not_exists => TRUE)",
    "SELECT add_continuous_aggregate_policy('journey_times_5m', start_offset => INTERVAL '15 minutes', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE)",
    "SELECT add_continuous_aggregate_policy('journey_times_15m', start_offset => INTERVAL '1 hour', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE)",
    "SELECT add_continuous_aggregate_policy('journey_times_1h', start_offset => INTERVAL '4 hours', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 hour', if_not_exists => TRUE)",

    # Retention policies
    "SELECT add_retention_policy('speeds_raw', INTERVAL '7 days', if_not_exists => TRUE)",
    "SELECT add_retention_policy('speeds_5m', INTERVAL '30 days', if_not_exists => TRUE)",
    "SELECT add_retention_policy('speeds_15m', INTERVAL '90 days', if_not_exists => TRUE)",
    "SELECT add_retention_policy('journey_times_raw', INTERVAL '7 days', if_not_exists => TRUE)",
    "SELECT add_retention_policy('journey_times_5m', INTERVAL '30 days', if_not_exists => TRUE)",
    "SELECT add_retention_policy('journey_times_15m', INTERVAL '90 days', if_not_exists => TRUE)",
]


async def migrate(show_status=False):
    url = os.environ["DATABASE_URL"]
    conn = await asyncpg.connect(url)

    if show_status:
        for table in ["sites", "speeds_raw", "journey_times_raw"]:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=$1)", table
            )
            print(f"  {table}: {'exists' if exists else 'missing'}")
        for view in ["speeds_5m", "speeds_15m", "speeds_1h", "journey_times_5m", "journey_times_15m", "journey_times_1h"]:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM timescaledb_information.continuous_aggregates WHERE view_name=$1)", view
            )
            print(f"  {view}: {'exists' if exists else 'missing'}")
        await conn.close()
        return

    print("Running migrations...")
    for i, sql in enumerate(MIGRATIONS, 1):
        label = sql.strip()[:60].replace("\n", " ")
        try:
            await conn.execute(sql)
            print(f"  [{i}/{len(MIGRATIONS)}] OK  {label}")
        except Exception as e:
            print(f"  [{i}/{len(MIGRATIONS)}] ERR {label}")
            print(f"    {e}")
            await conn.close()
            sys.exit(1)

    await conn.close()
    print(f"Done. {len(MIGRATIONS)} migrations applied.")


if __name__ == "__main__":
    if "--status" in sys.argv:
        asyncio.run(migrate(show_status=True))
    else:
        asyncio.run(migrate())
