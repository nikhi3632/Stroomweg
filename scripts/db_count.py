"""Show row counts for all tables."""

import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

TABLES = [
    "sites",
    "speeds_raw",
    "journey_times_raw",
    "speeds_5m",
    "speeds_15m",
    "speeds_1h",
    "journey_times_5m",
    "journey_times_15m",
    "journey_times_1h",
]


async def db_count():
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])

    for table in TABLES:
        try:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table:.<30} {count:>12,}")
        except Exception:
            print(f"  {table:.<30} {'(missing)':>12}")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(db_count())
