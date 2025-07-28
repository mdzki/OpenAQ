from api_client import get_data
from parse_data import parse_locations
from db.insert import insert_location
from rate_limit import handle_rate_limit


def process_locations(conn, params):
    page = 1
    while True:
        params["page"] = page
        data, headers = get_data("locations", params)
        handle_rate_limit(headers)

        locations = parse_locations(data)
        if not locations:
            break

        for loc in locations:
            insert_location(conn.cursor(), loc)
        conn.commit()

        if len(locations) < params["limit"]:
            break
        page += 1
