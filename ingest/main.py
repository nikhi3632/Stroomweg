"""Stroomweg ingest service — polls NDW feeds every 60s, writes to TimescaleDB."""

import asyncio
import logging
import signal
import time

import httpx

from .config import POLL_INTERVAL
from .db import get_pool, close_pool
from .redis import get_redis, close_redis, publish_speeds, publish_journey_times
from .reference import load_reference_data
from .speeds import ingest_speeds
from .journey_times import ingest_journey_times

log = logging.getLogger("stroomweg")


async def run():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    log.info("Starting Stroomweg ingest service")
    pool = await get_pool()

    # Load reference data (measurement.xml.gz → sites table)
    index_mappings = await load_reference_data(pool)
    log.info(f"Loaded index mappings for {len(index_mappings)} speed sites")

    # Graceful shutdown
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    # Polling loop
    async with httpx.AsyncClient() as client:
        while not stop.is_set():
            cycle_start = time.monotonic()

            try:
                speed_rows, jt_rows = await asyncio.gather(
                    ingest_speeds(pool, index_mappings, client),
                    ingest_journey_times(pool, client),
                )

                # Publish to Redis (non-blocking, best-effort)
                try:
                    await asyncio.gather(
                        publish_speeds(speed_rows),
                        publish_journey_times(jt_rows),
                    )
                except Exception:
                    log.exception("Redis publish failed (non-fatal)")

                elapsed = time.monotonic() - cycle_start

                # Query total row counts for confidence
                async with pool.acquire() as conn:
                    speed_total = await conn.fetchval("SELECT COUNT(*) FROM speeds_raw")
                    jt_total = await conn.fetchval("SELECT COUNT(*) FROM journey_times_raw")

                log.info(
                    f"Cycle: +{len(speed_rows)} speeds, +{len(jt_rows)} jt in {elapsed:.1f}s "
                    f"| total: {speed_total:,} speeds, {jt_total:,} jt"
                )
            except Exception:
                elapsed = time.monotonic() - cycle_start
                log.exception(f"Cycle failed after {elapsed:.1f}s")

            # Sleep until next cycle (skip if we're already late)
            elapsed = time.monotonic() - cycle_start
            sleep_time = max(0, POLL_INTERVAL - elapsed)
            if sleep_time == 0:
                log.warning(f"Cycle took {elapsed:.1f}s (>{POLL_INTERVAL}s), no sleep")

            try:
                await asyncio.wait_for(stop.wait(), timeout=sleep_time)
                break  # stop was set
            except asyncio.TimeoutError:
                pass  # normal — timeout means time to poll again

    await close_redis()
    await close_pool()
    log.info("Shutdown complete")


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
