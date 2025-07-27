from api_client import get_data
from rate_limit import handle_rate_limit
from config import BASE_URL, base_coordination_params


def fetch_all_locations(per_page=1000, max_pages=None, warsaw_only=True):
    page = 1
    results = []

    while True:
        params = base_coordination_params.copy()
        params["page"] = page

        data, headers = get_data("locations", params)
        handle_rate_limit(headers)

        chunk = data.get("results", [])
        if not chunk:
            break

        results.extend(chunk)

        if len(chunk) < per_page:
            break

        if max_pages and page >= max_pages:
            break

        page += 1

    return results


def fetch_measurements_for_sensor(sensor_id: int, per_page=500, max_pages=None):
    page = 1
    while True:
        if max_pages and page > max_pages:
            break

        params = {"page": page, "limit": per_page}
        url = f"{BASE_URL}/sensors/{sensor_id}/measurements"

        try:
            data, headers = get_data(full_url=url, params=params)
            handle_rate_limit(headers)

            chunk = data.get("results", [])
            if not chunk:
                break

            yield chunk

            if len(chunk) < per_page:
                break

            page += 1

        except Exception as e:
            raise RuntimeError(f"Page {page} failed: {e}")
