
from pathlib import Path
import csv


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "ghcn_d"
STATION_FILE = RAW_DATA_DIR / "ghcnd-stations.txt"
PILOT_IDS_FILE = RAW_DATA_DIR / "global_pilot_station_ids.txt"
OUTPUT_DIR = PROJECT_ROOT / "data" / "export"
OUTPUT_FILE = OUTPUT_DIR / "station_metadata.csv"


def main():
    # Check required input files
    for file_path in (STATION_FILE, PILOT_IDS_FILE):
        if not file_path.exists():
            raise FileNotFoundError(f"Required file not found: {file_path}")

    # Load the 100 selected pilot station IDs
    pilot_ids = {
        line.strip()
        for line in PILOT_IDS_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }

    if not pilot_ids:
        raise ValueError("No pilot station IDs found.")

    # Parse NOAA's fixed-width station metadata
    metadata = {}

    with STATION_FILE.open("r", encoding="utf-8", errors="replace") as station_file:
        for line in station_file:
            station_id = line[0:11].strip()

            if station_id not in pilot_ids:
                continue

            try:
                latitude = float(line[12:20])
                longitude = float(line[21:30])
                elevation = float(line[31:37])
            except ValueError:
                print(
                    f"WARNING: Could not parse coordinates/elevation "
                    f"for {station_id}; skipping this record."
                )
                continue

            station_name = line[41:71].strip()

            metadata[station_id] = {
                "station_id": station_id,
                "station_name": station_name,
                "lat": latitude,
                "lon": longitude,
                "elevation_m": elevation,
            }

    # Check metadata coverage
    missing_ids = sorted(pilot_ids - set(metadata))
    extra_ids = sorted(set(metadata) - pilot_ids)

    if missing_ids:
        raise ValueError(
            f"Missing metadata for {len(missing_ids)} pilot stations: "
            f"{missing_ids}"
        )

    if extra_ids:
        raise ValueError(
            f"Unexpected station IDs found: {extra_ids}"
        )

    if len(metadata) != len(pilot_ids):
        raise ValueError(
            "Metadata row count does not match the pilot station count."
        )

    # Export in a stable order
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "station_id",
        "station_name",
        "lat",
        "lon",
        "elevation_m",
    ]

    with OUTPUT_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for station_id in sorted(metadata):
            writer.writerow(metadata[station_id])

    print("Station metadata export completed successfully.")
    print(f"Pilot station IDs: {len(pilot_ids)}")
    print(f"Metadata records exported: {len(metadata)}")
    print(f"Missing metadata records: {len(missing_ids)}")
    print(f"Output file: {OUTPUT_FILE}")

    print("\nFirst five records:")
    with OUTPUT_FILE.open("r", encoding="utf-8", newline="") as output_file:
        reader = csv.DictReader(output_file)
        for index, row in enumerate(reader):
            if index >= 5:
                break
            print(row)


if __name__ == "__main__":
    main()