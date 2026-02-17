"""Load reference data from measurement.xml.gz into the sites table."""

import gzip
import io
import json
import logging

import httpx
from lxml import etree

from .config import MEASUREMENT_URL

log = logging.getLogger(__name__)

NS = "{http://datex2.eu/schema/2/2_0}"


def parse_site_record(elem):
    """Parse a measurementSiteRecord element into a site dict."""
    site_id = elem.get("id")
    if not site_id:
        return None

    # Name
    name_el = elem.find(f".//{NS}measurementSiteName//{NS}value")
    name = name_el.text if name_el is not None and name_el.text else None

    # Road (first token of name, e.g. "N457" from "N457 hmp 4.75 Re")
    road = name.split()[0] if name else None

    # Lanes
    lanes_el = elem.find(f"{NS}measurementSiteNumberOfLanes")
    lanes = int(lanes_el.text) if lanes_el is not None else None

    # Equipment type
    equip_el = elem.find(f".//{NS}measurementEquipmentTypeUsed//{NS}value")
    equipment = equip_el.text if equip_el is not None else None

    # Direction
    side_el = elem.find(f"{NS}measurementSide")
    direction = side_el.text if side_el is not None else None

    # Coordinates
    lat_el = elem.find(f".//{NS}latitude")
    lon_el = elem.find(f".//{NS}longitude")
    lat = float(lat_el.text) if lat_el is not None else None
    lon = float(lon_el.text) if lon_el is not None else None

    # Index mapping: find anyVehicle indexes per lane for flow and speed
    index_mapping = {}
    for char in elem.findall(f"{NS}measurementSpecificCharacteristics"):
        idx = int(char.get("index"))
        inner = char.find(f"{NS}measurementSpecificCharacteristics")
        if inner is None:
            continue

        vtype = inner.find(f".//{NS}vehicleType")
        if vtype is None or vtype.text != "anyVehicle":
            continue

        lane_el = inner.find(f"{NS}specificLane")
        type_el = inner.find(f"{NS}specificMeasurementValueType")
        if lane_el is None or type_el is None:
            continue

        lane_name = lane_el.text
        mtype = type_el.text

        if lane_name not in index_mapping:
            index_mapping[lane_name] = {}

        if mtype == "trafficFlow":
            index_mapping[lane_name]["flow_index"] = idx
        elif mtype == "trafficSpeed":
            index_mapping[lane_name]["speed_index"] = idx

    return {
        "site_id": site_id,
        "name": name,
        "road": road,
        "lanes": lanes,
        "equipment": equipment,
        "direction": direction,
        "lat": lat,
        "lon": lon,
        "index_mapping": index_mapping,
    }


async def load_reference_data(pool):
    """Download measurement.xml.gz and populate the sites table.

    Returns a dict of {site_id: index_mapping} for use by the speed ingest.
    """
    log.info("Downloading measurement.xml.gz...")
    async with httpx.AsyncClient() as client:
        resp = await client.get(MEASUREMENT_URL, timeout=120)
        resp.raise_for_status()

    log.info("Parsing measurement.xml.gz...")
    data = gzip.decompress(resp.content)
    sites = []
    for _, elem in etree.iterparse(io.BytesIO(data), events=("end",), tag=f"{NS}measurementSiteRecord"):
        site = parse_site_record(elem)
        if site:
            sites.append(site)
        elem.clear()

    log.info(f"Parsed {len(sites)} sites, upserting to database...")

    rows = [
        (
            s["site_id"], s["name"], s["road"], s["lanes"],
            s["equipment"], s["direction"], s["lon"], s["lat"],
            json.dumps(s["index_mapping"]),
        )
        for s in sites
    ]

    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO sites (site_id, name, road, lanes, equipment, direction,
                               geom, index_mapping, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6,
                    ST_SetSRID(ST_MakePoint($7, $8), 4326), $9::jsonb, NOW())
            ON CONFLICT (site_id) DO UPDATE SET
                name = EXCLUDED.name,
                road = EXCLUDED.road,
                lanes = EXCLUDED.lanes,
                equipment = EXCLUDED.equipment,
                direction = EXCLUDED.direction,
                geom = EXCLUDED.geom,
                index_mapping = EXCLUDED.index_mapping,
                updated_at = NOW()
            """,
            rows,
        )

    log.info(f"Upserted {len(sites)} sites")

    # Return index mappings for use by the speed ingest
    return {s["site_id"]: s["index_mapping"] for s in sites if s["index_mapping"]}
