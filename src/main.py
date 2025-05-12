from data_fetcher import fetch_all_locations, fetch_measurements_for_sensor

if __name__ == "__main__":
    locations = fetch_all_locations(per_page=5, max_pages=1)
    print(f"Fetched {len(locations)} locations")

    for loc in locations:
        print(f"Location {loc['id']} - sensors: {len(loc.get('sensors', []))}")

        for sensor in loc.get("sensors", []):
            sensor_id = sensor["id"]
            measurements = fetch_measurements_for_sensor(
                sensor_id, per_page=1000, max_pages=1
            )
            print(f"  Sensor {sensor_id} - {measurements} measurements")
