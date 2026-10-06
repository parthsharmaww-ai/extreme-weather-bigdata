"""
Build the station geography lookup: region_name, country and continent.

Reads:
- data/export/station_metadata.csv  (the stations in the export)
- data/raw/ghcn_d/ghcnd-stations.txt  (STATE column; fixed-width)
- reference/country_continent.csv  (NOAA country code -> country, continent)
- reference/subdivision_names.csv  (US state / Canadian province names)

Writes data/export/station_geography.csv with station_id, region_name,
country, continent. export_station_year_metrics.py joins it into Table A.

Rules:
- The country comes from the first two letters of the station ID, which is
  NOAA's own country code (not the ISO code: for example SF is South Africa
  and NG is Niger).
- region_name is the state or province for the United States and Canada, the
  only countries whose stations carry a usable STATE value. Everywhere else
  it is the country name.
- continent follows the country, using the UN geoscheme. Russia is split at
  60 degrees east: stations west of it are Europe, stations east of it Asia.
- A station whose country code is not in the reference table gets empty
  values and is listed in the report. Nothing is guessed.
"""

import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent

METADATA_PATH = PROJECT_ROOT / "data" / "export" / "station_metadata.csv"
STATIONS_PATH = PROJECT_ROOT / "data" / "raw" / "ghcn_d" / "ghcnd-stations.txt"
COUNTRY_PATH = PROJECT_ROOT / "reference" / "country_continent.csv"
SUBDIVISION_PATH = PROJECT_ROOT / "reference" / "subdivision_names.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "export" / "station_geography.csv"

SUBDIVISION_COUNTRIES = ("US", "CA")

# country code -> (longitude where the split happens, continent west, continent east)
SPLIT_BY_LONGITUDE = {"RS": (60.0, "Europe", "Asia")}

OUTPUT_COLUMNS = ["station_id", "region_name", "country", "continent"]


def read_states(path, station_ids):
    """STATE value (columns 39-40 of ghcnd-stations.txt) for the given stations."""
    wanted = set(station_ids)
    states = {}
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            station_id = line[0:11]
            if station_id in wanted:
                states[station_id] = line[38:40].strip()
    return states


def build_geography(metadata, states, countries, subdivisions):
    """Return the lookup table and a list of station IDs with unknown countries."""
    country_by_code = countries.set_index("country_code")
    region_by_state = {
        (row.country_code, row.state_code): row.region_name
        for row in subdivisions.itertuples()
    }

    rows = []
    unknown = []
    for station in metadata.itertuples():
        station_id = station.station_id
        code = station_id[:2]

        if code not in country_by_code.index:
            unknown.append(station_id)
            rows.append((station_id, None, None, None))
            continue

        country = country_by_code.loc[code, "country"]
        continent = country_by_code.loc[code, "continent"]

        if code in SPLIT_BY_LONGITUDE:
            boundary, west, east = SPLIT_BY_LONGITUDE[code]
            continent = west if station.lon < boundary else east

        region = country
        if code in SUBDIVISION_COUNTRIES:
            region = region_by_state.get(
                (code, states.get(station_id, "")), country
            )

        rows.append((station_id, region, country, continent))

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS), unknown


def main():
    for path in (METADATA_PATH, STATIONS_PATH, COUNTRY_PATH, SUBDIVISION_PATH):
        if not path.exists():
            print(f"ERROR: Required input not found: {path}")
            if path == STATIONS_PATH:
                print("Run src/download_data.py first.")
            return False

    metadata = pd.read_csv(METADATA_PATH)
    if not {"station_id", "lon"} <= set(metadata.columns):
        print("ERROR: station_metadata.csv needs station_id and lon columns.")
        return False
    if metadata["station_id"].duplicated().any():
        print("ERROR: duplicate station_id in station_metadata.csv.")
        return False

    countries = pd.read_csv(COUNTRY_PATH)
    subdivisions = pd.read_csv(SUBDIVISION_PATH)
    states = read_states(STATIONS_PATH, metadata["station_id"])

    geography, unknown = build_geography(
        metadata, states, countries, subdivisions
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    geography.to_csv(OUTPUT_PATH, index=False)

    print("=" * 65)
    print("STATION GEOGRAPHY")
    print("=" * 65)
    print(f"Stations: {len(geography):,}")
    print("\nStations by continent:")
    print(geography["continent"].fillna("(unknown)").value_counts().to_string())
    print(f"\nStations by country (top 10):")
    print(geography["country"].fillna("(unknown)").value_counts().head(10).to_string())

    with_state = geography["region_name"].ne(geography["country"]).sum()
    print(f"\nRegion taken from a state or province: {with_state:,}")
    print(f"Region set to the country name: {len(geography) - with_state - len(unknown):,}")

    if unknown:
        print(f"\nWARNING: {len(unknown)} station(s) have a country code that "
              f"is not in the reference table and were left empty:")
        print("  " + ", ".join(unknown[:20]))

    print(f"\nOutput: {OUTPUT_PATH}")
    return True


if __name__ == "__main__":
    if not main():
        sys.exit(1)
