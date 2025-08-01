def create_tables(conn):
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY,
            name TEXT,
            city TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS instruments (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS location_instruments (
            location_id INTEGER,
            instrument_id INTEGER,
            PRIMARY KEY (location_id, instrument_id),
            FOREIGN KEY (location_id) REFERENCES locations(id),
            FOREIGN KEY (instrument_id) REFERENCES instruments(id)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS parameters (
            id INTEGER PRIMARY KEY,
            name TEXT,
            display_name TEXT,
            units TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS sensors (
            id INTEGER PRIMARY KEY,
            name TEXT,
            location_id INTEGER,
            parameter_id INTEGER,
            FOREIGN KEY (location_id) REFERENCES locations(id),
            FOREIGN KEY (parameter_id) REFERENCES parameters(id)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id INTEGER,
            value REAL,
            start_utc TEXT,
            end_utc TEXT,
            interval TEXT,
            label TEXT,
            FOREIGN KEY (sensor_id) REFERENCES sensors(id),
            UNIQUE (sensor_id, start_utc, end_utc, interval)
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            step TEXT,
            status TEXT,
            message TEXT,
            loaded INTEGER,
            failed INTEGER,
            expected TEXT,
            load_date TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.commit()
