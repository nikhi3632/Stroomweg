"""Check the last ingest timestamps and data freshness."""

import asyncio
import os
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv

load_dotenv()


async def ingest_status():
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    now = datetime.now(timezone.utc)

    for table, label in [("speeds_raw", "Speeds"), ("journey_times_raw", "Journey times")]:
        try:
            latest = await conn.fetchval(f"SELECT MAX(timestamp) FROM {table}")
            if latest:
                age = (now - latest).total_seconds()
                print(f"  {label}: last data at {latest.isoformat()} ({age:.0f}s ago)")
            else:
                print(f"  {label}: no data")
        except Exception:
            print(f"  {label}: table missing")

    sites = await conn.fetchval("SELECT COUNT(*) FROM sites")
    print(f"  Sites loaded: {sites}")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(ingest_status())
