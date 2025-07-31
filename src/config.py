from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.openaq.org/v3"

# ✅ stałe współrzędne dla Warszawy
radius = 12000
coordinates = "52.2297700,21.0117800"
limit = 1000
