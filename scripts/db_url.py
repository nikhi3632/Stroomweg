"""Print DATABASE_URL for use with psql."""

import os
from dotenv import load_dotenv

load_dotenv()
print(os.environ["DATABASE_URL"])
