def parse_locations(data: dict) -> list[dict]:
    return data.get("results", [])


def parse_measurements(data: dict) -> list[dict]:
    return data.get("results", [])
