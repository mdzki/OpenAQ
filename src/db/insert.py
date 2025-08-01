def insert_locations(c, locations: list[dict]):
    for loc in locations:
        c.execute(
            """
            INSERT OR IGNORE INTO locations (id, name, city, country, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                loc["id"],
                loc["name"],
                loc["city"],
                loc["country"],
                loc["latitude"],
                loc["longitude"],
            ),
        )


def insert_instruments(c, instruments: list[tuple]):
    for instr_id, name, location_id in instruments:
        c.execute(
            "INSERT OR IGNORE INTO instruments (id, name) VALUES (?, ?)",
            (instr_id, name),
        )
        c.execute(
            "INSERT OR IGNORE INTO location_instruments (location_id, instrument_id) VALUES (?, ?)",
            (location_id, instr_id),
        )


def insert_parameters(c, parameters: list[dict]):
    for p in parameters:
        c.execute(
            """
            INSERT OR IGNORE INTO parameters (id, name, display_name, units)
            VALUES (?, ?, ?, ?)
        """,
            (p["id"], p["name"], p["display_name"], p["units"]),
        )


def insert_sensors(c, sensors: list[dict]):
    for s in sensors:
        c.execute(
            """
            INSERT OR IGNORE INTO sensors (id, name, location_id, parameter_id)
            VALUES (?, ?, ?, ?)
        """,
            (s["id"], s["name"], s["location_id"], s["parameter_id"]),
        )


def insert_measurements(c, measurements: list[dict]):
    for m in measurements:
        c.execute(
            """
            INSERT OR IGNORE INTO measurements (sensor_id, value, start_utc, end_utc, interval, label)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                m["sensor_id"],
                m["value"],
                m["start_utc"],
                m["end_utc"],
                m["interval"],
                m["label"],
            ),
        )
