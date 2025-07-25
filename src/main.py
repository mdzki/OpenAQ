from db.connection import get_connection
from db.schema import create_tables
from db.insert import (
    insert_locations,
    insert_instruments,
    insert_parameters,
    insert_sensors,
    insert_measurements,
)
from db.utils import (
    get_existing_location_ids,
    get_sensor_ids_with_measurements,
    get_sensors_from_db,
)
from db.logging import log_etl_step

from data_fetcher import fetch_all_locations, fetch_measurements_for_sensor
from data_parsers import (
    parse_location,
    parse_instruments,
    parse_parameters_from_sensors,
    parse_sensors,
    parse_measurements,
)


def fetch_and_insert_locations(conn):
    print("📍 Fetching locations from API...")
    raw_locations = fetch_all_locations(per_page=100, max_pages=None)
    parsed_locations = [parse_location(loc) for loc in raw_locations]
    insert_locations(conn.cursor(), parsed_locations)
    inserted = len(parsed_locations)

    instruments_total = 0
    parameters_total = 0
    sensors_total = 0

    for raw in raw_locations:
        location_id = raw["id"]
        instr = parse_instruments(location_id, raw.get("instruments", []))
        insert_instruments(conn.cursor(), instr)
        instruments_total += len(instr)

        param = parse_parameters_from_sensors(raw.get("sensors", []))
        insert_parameters(conn.cursor(), param)
        parameters_total += len(param)

        sens = parse_sensors(location_id, raw.get("sensors", []))
        insert_sensors(conn.cursor(), sens)
        sensors_total += len(sens)

    conn.commit()

    print(
        f"✅ Inserted {inserted} locations, {instruments_total} instruments, {parameters_total} parameters, {sensors_total} sensors\n"
    )

    log_etl_step(
        conn,
        step="locations",
        status="ok",
        message="Fetched and inserted locations",
        loaded=inserted,
    )
    log_etl_step(conn, step="instruments", status="ok", loaded=instruments_total)
    log_etl_step(conn, step="parameters", status="ok", loaded=parameters_total)
    log_etl_step(conn, step="sensors", status="ok", loaded=sensors_total)


def fetch_and_insert_measurements(conn):
    print("📊 Fetching measurements...")
    existing_measurements = get_sensor_ids_with_measurements(conn)
    sensors = get_sensors_from_db(conn)

    loaded_total = 0
    skipped_total = 0
    failed_total = 0

    for sensor in sensors:
        sensor_id = sensor["id"]
        if sensor_id in existing_measurements:
            print(f"⏭️  Skipping sensor {sensor_id} (already in measurements)")
            skipped_total += 1
            continue

        print(f"📡 Fetching measurements for sensor {sensor_id}...")

        measurements = []
        try:
            for page_chunk in fetch_measurements_for_sensor(
                sensor_id, per_page=1000, max_pages=1000
            ):
                parsed = parse_measurements(sensor_id, page_chunk)
                measurements.extend(parsed)

        except RuntimeError as e:
            print(f"⚠️  Partial failure for sensor {sensor_id}: {e}")
            failed_total += 1
            log_etl_step(
                conn, step=f"sensor_{sensor_id}", status="error", message=str(e)
            )

        if measurements:
            insert_measurements(conn.cursor(), measurements)
            conn.commit()
            loaded_total += len(measurements)
            print(
                f"✅ Inserted {len(measurements)} measurements for sensor {sensor_id}.\n"
            )
        else:
            print(f"⚠️  No measurements to insert for sensor {sensor_id}. Skipping.")
            skipped_total += 1

    print(
        f"✅ Summary: {loaded_total} loaded, {skipped_total} skipped, {failed_total} failed"
    )
    log_etl_step(
        conn,
        step="measurements",
        status="ok",
        loaded=loaded_total,
        skipped=skipped_total,
    )


def run_etl():
    conn = get_connection()
    create_tables(conn)

    if not get_existing_location_ids(conn):
        fetch_and_insert_locations(conn)
    else:
        print("📍 Locations already exist in DB — skipping location fetch.\n")

    fetch_and_insert_measurements(conn)
    conn.close()
    print("✅ ETL complete.")


if __name__ == "__main__":
    run_etl()
