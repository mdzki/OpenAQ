import requests
from requests.structures import CaseInsensitiveDict
from config import BASE_URL, API_KEY

HEADERS = {"X-API-Key": API_KEY}


def get_data(
    endpoint: str = "", params: dict | None = None, full_url: str | None = None
) -> tuple[dict, dict]:
    url = full_url if full_url else f"{BASE_URL}/{endpoint}"
    print(f"[REQUEST] {url} with params: {params}")

    response = requests.get(url, headers=HEADERS, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"API error {response.status_code}: {response.text}")

    return response.json(), dict(response.headers)

