# main.py
from db.connection import get_connection
from db.schema import create_tables
from db.utils import get_existing_location_ids
from etl.measurements import fetch_and_insert_measurements
from etl.locations import fetch_and_insert_locations  # Assuming similar structure


def run_etl(incremental: bool = True, backfill_days: int = 7):
    """Main ETL workflow"""
    conn = get_connection()
    create_tables(conn)

    try:
        # Location handling
        if not get_existing_location_ids(conn):
            fetch_and_insert_locations(conn)
        else:
            print("📍 Locations exist - skipping fetch")

        # Measurement handling
        loaded, skipped, failed = fetch_and_insert_measurements(
            conn, incremental=incremental, backfill_days=backfill_days
        )
        print(f"ETL Complete. Loaded: {loaded}, Skipped: {skipped}, Failed: {failed}")

    finally:
        conn.close()


if __name__ == "__main__":
    run_etl(incremental=True, backfill_days=7)
