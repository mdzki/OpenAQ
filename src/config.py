import os

API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")

# Default parameters for Warsaw
radius = int(os.getenv("radius", 12000))
coordinates = os.getenv("coordinates", "52.2297700,21.0117800")
limit = int(os.getenv("limit", 1000))
if not API_KEY:
    raise ValueError("API_KEY must be set in the environment variables.")

backfill_days = int(os.getenv("backfill_days", 7))
incremental = os.getenv("INCREMENTAL", "True").lower() == "true"
