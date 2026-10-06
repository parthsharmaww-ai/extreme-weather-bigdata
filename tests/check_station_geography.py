"""
Check src/build_station_geography.py on synthetic stations in NOAA's
ghcnd-stations.txt layout, and check the reference tables.

Run from the repository root:  python tests/check_station_geography.py
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
failures = []


def check(name, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {name} {detail}")
    if not condition:
        failures.append(name)


def noaa_line(station_id, lat, lon, state, name):
    """One line of ghcnd-stations.txt: ID 1-11, lat 13-20, lon 22-30, elevation
    32-37, state 39-40, name 42-71."""
    return f"{station_id:<11} {lat:8.4f} {lon:9.4f} {10.0:6.1f} {state:<2} {name:<30}"


# (station_id, lat, lon, STATE in the stations file, expected region, country, continent)
cases = [
    ("USW00094728", 40.7789, -73.9692, "NY", "New York", "United States", "North America"),
    ("USW00023174", 33.9381, -118.3867, "CA", "California", "United States", "North America"),
    ("US1UMX00001", 28.2, -177.4, "UM", "United States Minor Outlying Islands", "United States", "North America"),
    ("US1XX000001", 40.0, -100.0, "", "United States", "United States", "North America"),
    ("CA006158799", 43.7, -79.4, "ON", "Ontario", "Canada", "North America"),
    ("IN022021900", 28.583, 77.2, "", "India", "India", "Asia"),
    # NOAA codes differ from ISO codes: SF South Africa, NG Niger, NI Nigeria, AS Australia
    ("SFM00068816", -33.97, 18.6, "", "South Africa", "South Africa", "Africa"),
    ("NGM00061052", 13.48, 2.17, "", "Niger", "Niger", "Africa"),
    ("NI000065015", 9.0, 7.0, "", "Nigeria", "Nigeria", "Africa"),
    ("ASN00066062", -33.95, 151.18, "", "Australia", "Australia", "Oceania"),
    ("AQW00061705", -14.3, -170.7, "AS", "American Samoa", "American Samoa", "Oceania"),
    ("RQ1PRAB0001", 18.3, -66.1, "PR", "Puerto Rico", "Puerto Rico", "North America"),
    ("SPE00120512", 37.4, -5.9, "", "Spain", "Spain", "Europe"),
    # Russia is split at 60 degrees east
    ("RSM00027612", 55.8, 37.6, "", "Russia", "Russia", "Europe"),
    ("RSW00037201", 71.6, 128.9, "SA", "Russia", "Russia", "Asia"),
    # outside the reference table: nothing is guessed
    ("XX000000001", 0.0, 0.0, "", None, None, None),
]

print("Reference tables")
countries = pd.read_csv(ROOT / "reference" / "country_continent.csv")
subdivisions = pd.read_csv(ROOT / "reference" / "subdivision_names.csv")
check("219 NOAA country codes, none repeated",
      len(countries) == 219 and countries["country_code"].is_unique)
check("every code has two letters and a continent",
      countries["country_code"].str.len().eq(2).all() and countries["continent"].notna().all())
check("seven continents",
      set(countries["continent"]) == {"Africa", "Antarctica", "Asia", "Europe",
                                      "North America", "Oceania", "South America"})
check("52 US and 13 Canadian subdivisions",
      (subdivisions["country_code"] == "US").sum() == 52
      and (subdivisions["country_code"] == "CA").sum() == 13)

print("Geography script")
with tempfile.TemporaryDirectory() as tmp:
    project = Path(tmp)
    (project / "src").mkdir()
    shutil.copy(ROOT / "src" / "build_station_geography.py", project / "src")
    shutil.copytree(ROOT / "reference", project / "reference")
    raw = project / "data" / "raw" / "ghcn_d"
    raw.mkdir(parents=True)
    export = project / "data" / "export"
    export.mkdir(parents=True)

    in_file = [c for c in cases if c[0] != "US1XX000001"]   # this one is missing from the stations file
    (raw / "ghcnd-stations.txt").write_text(
        "\n".join(noaa_line(c[0], c[1], c[2], c[3], "TEST " + c[0]) for c in in_file) + "\n"
    )
    pd.DataFrame(
        [(c[0], c[1], c[2]) for c in cases], columns=["station_id", "lat", "lon"]
    ).to_csv(export / "station_metadata.csv", index=False)

    result = subprocess.run(
        [sys.executable, str(project / "src" / "build_station_geography.py")],
        capture_output=True, text=True, cwd=project,
    )
    check("script runs", result.returncode == 0,
          "" if result.returncode == 0 else "\n" + result.stdout[-1200:] + result.stderr[-1200:])
    out = pd.read_csv(export / "station_geography.csv").set_index("station_id")

    check("columns are station_id, region_name, country, continent",
          list(out.reset_index().columns) == ["station_id", "region_name", "country", "continent"])
    check("one row per station", len(out) == len(cases))
    for sid, _, _, state, region, country, continent in cases:
        row = out.loc[sid]
        got = tuple(None if pd.isna(v) else v for v in (row.region_name, row.country, row.continent))
        want = (region, country, continent)
        label = f"{sid} (state '{state}')"
        check(f"{label} -> {country}, {region}, {continent}", got == want,
              f"(got {got})" if got != want else "")
    check("unknown country is reported", "XX000000001" in result.stdout)

print()
if failures:
    print(f"FAILED: {len(failures)} check(s)")
    sys.exit(1)
print("All checks passed.")
