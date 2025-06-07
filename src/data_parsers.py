def parse_location(raw: dict) -> dict:
    return {
        "id": raw["id"],
        "name": raw.get("name"),
        "city": raw.get("locality"),
        "country": raw.get("country", {}).get("name"),
        "latitude": raw["coordinates"]["latitude"],
        "longitude": raw["coordinates"]["longitude"],
    }


def parse_instruments(location_id: int, raw: list[dict]) -> list[tuple]:
    return [(instr["id"], instr["name"], location_id) for instr in raw]


def parse_parameters_from_sensors(raw_sensors: list[dict]) -> list[dict]:
    seen = set()
    parameters = []
    for sensor in raw_sensors:
        param = sensor["parameter"]
        param_id = param["id"]
        if param_id not in seen:
            seen.add(param_id)
            parameters.append(
                {
                    "id": param_id,
                    "name": param["name"],
                    "display_name": param["displayName"],
                    "units": param["units"],
                }
            )
    return parameters


def parse_sensors(location_id: int, raw_sensors: list[dict]) -> list[dict]:
    return [
        {
            "id": sensor["id"],
            "name": sensor["name"],
            "location_id": location_id,
            "parameter_id": sensor["parameter"]["id"],
        }
        for sensor in raw_sensors
    ]


def parse_measurements(sensor_id: int, raw_measurements: list[dict]) -> list[dict]:
    return [
        {
            "sensor_id": sensor_id,
            "value": m.get("value"),
            "start_utc": m.get("period", {}).get("datetimeFrom", {}).get("utc"),
            "end_utc": m.get("period", {}).get("datetimeTo", {}).get("utc"),
            "interval": m.get("period", {}).get("interval"),
            "label": m.get("period", {}).get("label"),
        }
        for m in raw_measurements
    ]
