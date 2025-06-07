def log_etl_step(conn, step, status, message="", loaded=0, skipped=0):
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO etl_logs (step, status, message, records_loaded, records_skipped)
        VALUES (?, ?, ?, ?, ?)
        """,
        (step, status, message, loaded, skipped),
    )
    conn.commit()
