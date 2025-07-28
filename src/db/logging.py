# db/logging.py
def log_etl_step(
    conn,
    step: str,
    status: str,
    message: str,
    loaded: int,
    skipped: int,
    failed: int,
    expected: str,
):
    """Enhanced logging with more detailed tracking"""
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO etl_logs (
            step, 
            status, 
            message, 
            loaded, 
            skipped, 
            failed,
            expected, 
            load_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (step, status, message, loaded, skipped, failed, expected),
    )
    conn.commit()
