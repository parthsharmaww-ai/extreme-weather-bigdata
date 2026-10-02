
"""Select a geographically distributed pilot from eligible GHCN stations."""

from pathlib import Path
import math

ROOT = Path(__file__).resolve().parent.parent
STATION_FILE = ROOT / "data" / "raw" / "ghcn_d" / "ghcnd-stations.txt"
ELIGIBLE_FILE = ROOT / "data" / "raw" / "ghcn_d" / "eligible_station_ids.txt"
OUTPUT_FILE = ROOT / "data" / "raw" / "ghcn_d" / "global_pilot_station_ids.txt"

PILOT_SIZE = 100
REQUIRED_STATIONS = ["USW00094728", "USW00023174"]


def distance_km(a, b):
    """Great-circle distance using the haversine formula."""
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 6371.0 * 2 * math.asin(min(1.0, math.sqrt(h)))


def main():
    eligible = set(ELIGIBLE_FILE.read_text().split())
    stations = {}

    with STATION_FILE.open(encoding="ascii", errors="replace") as stream:
        for line in stream:
            station_id = line[0:11].strip()
            if station_id not in eligible:
                continue

            try:
                latitude = float(line[12:20])
                longitude = float(line[21:30])
            except ValueError:
                continue

            if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                stations[station_id] = (latitude, longitude)

    selected = [sid for sid in REQUIRED_STATIONS if sid in stations]

    if len(selected) != len(REQUIRED_STATIONS):
        missing = set(REQUIRED_STATIONS) - set(selected)
        raise RuntimeError(f"Required test stations not eligible: {missing}")

    remaining = set(stations) - set(selected)

    # Farthest-point selection favors geographic spread.
    while remaining and len(selected) < min(PILOT_SIZE, len(stations)):
        best_id = max(
            remaining,
            key=lambda sid: min(
                distance_km(stations[sid], stations[chosen])
                for chosen in selected
            ),
        )
        selected.append(best_id)
        remaining.remove(best_id)

    OUTPUT_FILE.write_text("\n".join(selected) + "\n", encoding="ascii")

    print(f"Eligible stations with coordinates: {len(stations):,}")
    print(f"Pilot stations selected: {len(selected)}")
    print(f"Output: {OUTPUT_FILE.relative_to(ROOT)}")
    print("First 10 selected station IDs:")
    for station_id in selected[:10]:
        lat, lon = stations[station_id]
        print(f"  {station_id}: {lat:.3f}, {lon:.3f}")


if __name__ == "__main__":
    main()