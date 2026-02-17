import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.environ.get("REDIS_URL")

NDW_BASE = "https://opendata.ndw.nu"
MEASUREMENT_URL = f"{NDW_BASE}/measurement.xml.gz"
TRAFFICSPEED_URL = f"{NDW_BASE}/trafficspeed.xml.gz"
TRAVELTIME_URL = f"{NDW_BASE}/traveltime.xml.gz"

POLL_INTERVAL = 60  # seconds
