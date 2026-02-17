"""Drop all Stroomweg tables and views."""

import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DROPS = [
    # Continuous aggregates first (depend on hypertables)
    "DROP MATERIALIZED VIEW IF EXISTS speeds_5m CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS speeds_15m CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS speeds_1h CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS journey_times_5m CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS journey_times_15m CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS journey_times_1h CASCADE",
    # Hypertables
    "DROP TABLE IF EXISTS speeds_raw CASCADE",
    "DROP TABLE IF EXISTS journey_times_raw CASCADE",
    # Regular tables
    "DROP TABLE IF EXISTS sites CASCADE",
]


async def db_delete():
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    confirm = input("Drop all tables? (y/N): ")
    if confirm.lower() != "y":
        print("Aborted.")
        await conn.close()
        return

    for sql in DROPS:
        await conn.execute(sql)
        print(f"  {sql}")

    await conn.close()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(db_delete())
