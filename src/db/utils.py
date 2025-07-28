def get_existing_location_ids(conn):
    c = conn.cursor()
    c.execute("SELECT id FROM locations")
    return [row[0] for row in c.fetchall()]


def get_sensor_ids_with_measurements(conn):
    c = conn.cursor()
    c.execute("SELECT DISTINCT sensor_id FROM measurements")
    return set(row[0] for row in c.fetchall())


def get_sensors_from_db(conn):
    c = conn.cursor()
    c.execute("SELECT id, location_id, parameter_id, name FROM sensors")
    return [
        {"id": row[0], "location_id": row[1], "parameter_id": row[2], "name": row[3]}
        for row in c.fetchall()
    ]


def get_latest_measurement_times(conn):
    """Get the latest start_utc for each sensor_id"""
    c = conn.cursor()
    c.execute(
        """
        SELECT sensor_id, MAX(start_utc) as latest_time 
        FROM measurements 
        GROUP BY sensor_id
    """
    )
    return {row["sensor_id"]: row["latest_time"] for row in c.fetchall()}
