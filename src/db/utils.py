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
    return {row[0]: row[1] for row in c.fetchall()}


def get_measurement_count_for_sensor(conn, sensor_id: int) -> int:
    """Get count of measurements for a specific sensor"""
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM measurements WHERE sensor_id = ?", (sensor_id,))
    return c.fetchone()[0]


def get_measurement_count(conn) -> int:
    """Get total count of measurements in the database"""
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM measurements")
    return c.fetchone()[0]
