import time
import requests
from requests.structures import CaseInsensitiveDict
from config import BASE_URL, API_KEY
from typing import Mapping

HEADERS = {"X-API-Key": API_KEY}


def handle_rate_limit(headers: Mapping[str, str]):
    remaining = int(headers.get("X-Ratelimit-Remaining", 1))
    reset = int(headers.get("X-Ratelimit-Reset", "59"))
    print(f"[RateLimit] Remaining: {remaining}, Reset: {reset}")
    if remaining == 0:
        print(f"[RateLimit] Sleeping for {reset}s. Reset in {reset} seconds...")
        time.sleep(reset)


def get_data(
    endpoint: str = "", params: dict | None = None, full_url: str | None = None
) -> tuple[dict, dict]:
    url = full_url if full_url else f"{BASE_URL}/{endpoint}"
    print(f"[REQUEST] {url} with params: {params}")

    response = requests.get(url, headers=HEADERS, params=params)

    handle_rate_limit(response.headers)

    if response.status_code not in [200, 429]:
        raise RuntimeError(f"API error {response.status_code}: {response.text}")

    return response.json(), dict(response.headers)
