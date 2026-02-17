"""Fetch traveltime.xml.gz and insert journey times into journey_times_raw."""

import gzip
import io
import logging
from datetime import datetime, timezone

import httpx
from lxml import etree

from .config import TRAVELTIME_URL

log = logging.getLogger(__name__)

NS = "{http://datex2.eu/schema/2/2_0}"


async def ingest_journey_times(pool, client):
    """Fetch one snapshot and insert into journey_times_raw.

    Returns the number of rows inserted.
    """
    resp = await client.get(TRAVELTIME_URL, timeout=60)
    resp.raise_for_status()

    data = gzip.decompress(resp.content)
    rows = []

    for _, elem in etree.iterparse(io.BytesIO(data), events=("end",), tag=f"{NS}siteMeasurements"):
        site_ref = elem.find(f"{NS}measurementSiteReference")
        time_el = elem.find(f"{NS}measurementTimeDefault")
        if site_ref is None or time_el is None:
            elem.clear()
            continue

        site_id = site_ref.get("id")
        timestamp = datetime.fromisoformat(time_el.text.replace("Z", "+00:00"))

        # Travel time data is always at index 1
        mv = elem.find(f"{NS}measuredValue")
        if mv is None:
            elem.clear()
            continue

        inner = mv.find(f"{NS}measuredValue")
        if inner is None:
            elem.clear()
            continue

        basic = inner.find(f"{NS}basicData")
        if basic is None:
            elem.clear()
            continue

        # Actual travel time
        tt = basic.find(f"{NS}travelTime")
        if tt is None:
            elem.clear()
            continue

        duration_el = tt.find(f"{NS}duration")
        duration = float(duration_el.text) if duration_el is not None else None
        if duration is not None and duration == -1:
            duration = None

        accuracy = float(tt.get("accuracy")) if tt.get("accuracy") else None
        quality = float(tt.get("supplierCalculatedDataQuality")) if tt.get("supplierCalculatedDataQuality") else None
        input_values = int(tt.get("numberOfInputValuesUsed")) if tt.get("numberOfInputValuesUsed") else None

        # Reference (free-flow) duration from extension
        ref_duration = None
        ref_tt = inner.find(
            f".//{NS}measuredValueExtension//{NS}basicDataReferenceValue//{NS}travelTimeData//{NS}duration"
        )
        if ref_tt is not None:
            ref_duration = float(ref_tt.text)
            if ref_duration == -1:
                ref_duration = None

        rows.append((timestamp, site_id, duration, ref_duration, accuracy, quality, input_values))
        elem.clear()

    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO journey_times_raw (timestamp, site_id, duration_sec, ref_duration_sec,
                                           accuracy, quality, input_values)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (timestamp, site_id) DO NOTHING
            """,
            rows,
        )

    return len(rows)
