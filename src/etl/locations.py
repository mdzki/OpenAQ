import time
from typing import Optional
from config import radius, coordinates, limit
from etl.api_client import get_data
from db.insert import (
    insert_instruments,
    insert_locations,
    insert_parameters,
    insert_sensors,
)
from db.logging import log_etl_step
from etl.parse_data import (
    parse_instruments,
    parse_locations,
    parse_parameters_from_sensors,
    parse_sensors,
)


def process_locations(conn, params):
    page = 1
    while True:
        params["page"] = page
        data, headers = get_data("locations", params)

        locations = parse_locations(data)
        if not locations:
            break

        for loc in locations:
            insert_locations(conn.cursor(), loc)
        conn.commit()

        if len(locations) < params["limit"]:
            break
        page += 1


def fetch_all_locations(max_pages: Optional[int] = None) -> list[dict]:
    """Fetch all locations with pagination"""
    locations = []
    page = 1

    while True:
        params = {
            "limit": limit,
            "page": page,
            "radius": radius,
            "coordinates": coordinates,
        }

        data, _ = get_data("locations", params)
        locations.extend(data["results"])

        if len(data["results"]) < limit or (max_pages and page >= max_pages):
            break

        page += 1

    return locations


def fetch_and_insert_locations(conn):
    start_time = time.time()
    print("📍 Fetching locations from API...")
    raw_locations = fetch_all_locations(max_pages=None)
    parsed_locations = [parse_locations(loc) for loc in raw_locations]
    insert_locations(conn.cursor(), parsed_locations)
    inserted = len(parsed_locations)

    instruments_total = 0
    parameters_total = 0
    sensors_total = 0

    for raw in raw_locations:
        location_id = raw["id"]
        instr = parse_instruments(location_id, raw.get("instruments", []))
        insert_instruments(conn.cursor(), instr)
        instruments_total += len(instr)

        param = parse_parameters_from_sensors(raw.get("sensors", []))
        insert_parameters(conn.cursor(), param)
        parameters_total += len(param)

        sens = parse_sensors(location_id, raw.get("sensors", []))
        insert_sensors(conn.cursor(), sens)
        sensors_total += len(sens)

    conn.commit()
    print(
        f"✅ Inserted {inserted} locations, {instruments_total} instruments, {parameters_total} parameters, {sensors_total} sensors\n"
    )
    log_etl_step(
        conn,
        step="locations",
        status="ok",
        message="Fetched and inserted locations",
        loaded=inserted,
        failed=0,
        expected=str(inserted),
        duration_seconds=int(time.time() - start_time)
    )
