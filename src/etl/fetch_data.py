# etl/fetch_data.py
from datetime import datetime, timezone
import re
from typing import Iterator, Tuple, Optional
from api_client import get_data


def parse_found_count(found: str) -> int:
    """Parse the 'found' count which can be '>1000' or a number"""
    if isinstance(found, int):
        return found
    if found.startswith(">"):
        return int(found[1:])  # Return the number part
    try:
        return int(found)
    except (ValueError, TypeError):
        return 0  # Fallback for unexpected formats


def fetch_measurements_for_sensor(
    sensor_id: int, datetime_from: Optional[datetime] = None, per_page: int = 1000
) -> Iterator[tuple[list[dict], int]]:
    """Generator that yields (measurements_page, total_count)"""
    page = 1

    while True:
        params = {"limit": per_page, "page": page, "include_total": True}

        if datetime_from:
            params["datetime_from"] = datetime_from.astimezone(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        data, _ = get_data(f"sensors/{sensor_id}/measurements", params)
        results = data.get("results", [])
        found = data.get("meta", {}).get("found", 0)
        total_found = parse_found_count(found)

        yield results, total_found

        if len(results) < per_page or page >= 10000:
            break

        page += 1
