from datetime import datetime, timezone, timedelta
from typing import Optional
from db.connection import get_connection
from db.utils import (
    get_latest_measurement_times,
    get_sensors_from_db,
    get_measurement_count_for_sensor,
)
from db.insert import insert_measurements
from db.logging import log_etl_step
from etl.fetch_data import fetch_measurements_for_sensor
from etl.parse_data import parse_measurements


def format_openaq_datetime(dt: Optional[datetime]) -> Optional[str]:
    """Format datetime for OpenAQ API (YYYY-MM-DDTHH:MM:SSZ)"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def process_sensor_measurements(
    conn, sensor: dict, datetime_from: Optional[datetime] = None
) -> tuple[int, bool]:
    """
    Process all measurements for a single sensor
    Returns tuple of (loaded_count, is_complete)
    """
    loaded = 0
    is_complete = True
    sensor_id = sensor["id"]

    try:
        current_count = get_measurement_count_for_sensor(conn, sensor_id)
        # Pass the datetime object directly, not the formatted string
        for measurements, total_found in fetch_measurements_for_sensor(
            sensor_id, datetime_from=datetime_from
        ):
            if not measurements:
                continue

            parsed = parse_measurements(sensor_id, measurements)
            insert_measurements(conn.cursor(), parsed)
            conn.commit()
            loaded += len(parsed)

            # Determine completeness:
            # - If we got a full page (1000), assume there might be more data
            # - If we got <1000, we've reached the end
            is_complete = len(measurements) < 1000

            # Special case: if total_found is exact and we've matched it
            if isinstance(total_found, int) and (current_count + loaded) >= total_found:
                is_complete = True

    except Exception as e:
        print(f"⚠️  Error processing sensor {sensor_id}: {e}")
        log_etl_step(
            conn,
            step=f"sensor_{sensor_id}",
            status="error",
            message=str(e),
            loaded=loaded,
            expected="Unknown (>" + str(loaded) if loaded >= 1000 else str(loaded),
            skipped=0,
            failed=1,
        )
        raise

    return loaded, is_complete


def fetch_and_insert_measurements(
    conn, incremental: bool = True, backfill_days: int = 7
) -> tuple[int, int, int]:
    """
    Main measurement loading function
    Returns tuple of (loaded_count, skipped_count, failed_count)
    """
    print("📊 Starting measurements ETL...")
    latest_times = get_latest_measurement_times(conn)
    sensors = get_sensors_from_db(conn)

    loaded_total = 0
    skipped_total = 0
    failed_total = 0

    for sensor in sensors:
        sensor_id = sensor["id"]

        # Determine cutoff for incremental loading
        datetime_from = None
        if incremental:
            if latest_time_str := latest_times.get(sensor_id):
                try:
                    # Clean datetime string (handle both Z and +00:00 formats)
                    clean_time_str = latest_time_str.replace("Z", "").split("+")[0]
                    datetime_from = datetime.fromisoformat(clean_time_str)
                except ValueError as e:
                    print(
                        f"⚠️  Invalid datetime format for sensor {sensor_id}: {latest_time_str}"
                    )
                    failed_total += 1
                    continue
            elif backfill_days:
                datetime_from = datetime.now(timezone.utc) - timedelta(
                    days=backfill_days
                )

        try:
            if incremental and not datetime_from:
                print(f"⏭️  Skipping sensor {sensor_id} (no new data)")
                skipped_total += 1
                continue

            print(f"📡 Processing sensor {sensor_id}...")
            loaded, is_complete = process_sensor_measurements(
                conn, sensor, datetime_from
            )
            loaded_total += loaded

            # Count as skipped if we didn't get all data (got a full page)
            if not is_complete:
                print(
                    f"⚠️  Possibly incomplete data for sensor {sensor_id} (got {loaded} measurements)"
                )
                skipped_total += 1

        except Exception as e:
            print(f"⚠️  Critical error processing sensor {sensor_id}: {e}")
            failed_total += 1
            conn.rollback()
            continue

    # Final summary
    print(
        f"""
✅ ETL Complete:
   - Loaded: {loaded_total} measurements
   - Possibly incomplete: {skipped_total} sensors
   - Failed: {failed_total} sensors
"""
    )
    log_etl_step(
        conn,
        step="measurements",
        status="ok",
        message="Completed measurements ETL",
        loaded=loaded_total,
        skipped=skipped_total,
        failed=failed_total,
        expected=str(loaded_total + skipped_total),
    )
    return loaded_total, skipped_total, failed_total
