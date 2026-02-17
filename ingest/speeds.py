"""Fetch trafficspeed.xml.gz and insert per-lane speeds into speeds_raw."""

import gzip
import io
import logging
from datetime import datetime, timezone

import httpx
from lxml import etree

from .config import TRAFFICSPEED_URL

log = logging.getLogger(__name__)

NS = "{http://datex2.eu/schema/2/2_0}"
XSI_TYPE = "{http://www.w3.org/2001/XMLSchema-instance}type"

# Map lane names from measurement.xml to lane numbers
# lane1-9 → 1-9, allLanesCompleteCarriageway → 0, hardShoulder → skip
LANE_MAP = {f"lane{i}": i for i in range(1, 10)}
LANE_MAP["allLanesCompleteCarriageway"] = 0


async def ingest_speeds(pool, index_mappings, client):
    """Fetch one snapshot and insert into speeds_raw.

    Returns the number of rows inserted.
    """
    resp = await client.get(TRAFFICSPEED_URL, timeout=60)
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

        if site_id not in index_mappings:
            elem.clear()
            continue

        mapping = index_mappings[site_id]

        # Build index → value lookup from all measuredValue elements
        values = {}
        for mv in elem.findall(f"{NS}measuredValue"):
            idx = int(mv.get("index"))
            inner = mv.find(f"{NS}measuredValue")
            if inner is None:
                continue
            basic = inner.find(f"{NS}basicData")
            if basic is None:
                continue

            xsi_type = basic.get(XSI_TYPE)
            if xsi_type == "TrafficFlow":
                rate_el = basic.find(f".//{NS}vehicleFlowRate")
                if rate_el is not None:
                    values[idx] = int(rate_el.text)
            elif xsi_type == "TrafficSpeed":
                speed_el = basic.find(f".//{NS}speed")
                if speed_el is not None:
                    values[idx] = float(speed_el.text)

        # Extract per-lane anyVehicle values using index_mapping
        for lane_name, indexes in mapping.items():
            lane_num = LANE_MAP.get(lane_name)
            if lane_num is None:
                continue  # skip hardShoulder and unknown lanes

            flow_idx = indexes.get("flow_index")
            speed_idx = indexes.get("speed_index")

            raw_flow = values.get(flow_idx) if flow_idx else None
            raw_speed = values.get(speed_idx) if speed_idx else None

            # -1 means no data — store as null
            flow_val = raw_flow if raw_flow is not None and raw_flow != -1 else None
            speed_val = raw_speed if raw_speed is not None and raw_speed != -1 else None

            rows.append((timestamp, site_id, lane_num, speed_val, flow_val))

        elem.clear()

    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO speeds_raw (timestamp, site_id, lane, speed_kmh, flow_veh_hr)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (timestamp, site_id, lane) DO NOTHING
            """,
            rows,
        )

    return rows
