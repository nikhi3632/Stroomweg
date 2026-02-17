"""Drop and recreate all tables."""

import asyncio
import os
import sys

import asyncpg
from dotenv import load_dotenv

load_dotenv()

# Import from sibling scripts
sys.path.insert(0, os.path.dirname(__file__))
from db_delete import DROPS
from migrate import MIGRATIONS


async def db_reset():
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])

    confirm = input("Drop and recreate all tables? (y/N): ")
    if confirm.lower() != "y":
        print("Aborted.")
        await conn.close()
        return

    print("Dropping...")
    for sql in DROPS:
        await conn.execute(sql)
        print(f"  {sql}")

    print("Recreating...")
    for i, sql in enumerate(MIGRATIONS, 1):
        label = sql.strip()[:60].replace("\n", " ")
        await conn.execute(sql)
        print(f"  [{i}/{len(MIGRATIONS)}] {label}")

    await conn.close()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(db_reset())
