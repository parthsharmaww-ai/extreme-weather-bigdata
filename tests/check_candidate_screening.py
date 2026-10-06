"""
Check src/screen_candidate_stations.py on synthetic .dly files built to sit on
exact boundaries of the eligibility rules.

Run from the repository root:  python tests/check_candidate_screening.py
"""

import calendar
import datetime
import importlib.util
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location(
    "screen_candidate_stations", ROOT / "src" / "screen_candidate_stations.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

ELEMENTS = ("TMAX", "TMIN", "PRCP")
failures = []


def check(name, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {name} {detail}")
    if not condition:
        failures.append(name)


def ordinal(year, month, day):
    return datetime.date(year, month, day).timetuple().tm_yday


def dly_lines(station, years, missing=None, qflag=None, elements=ELEMENTS):
    """.dly lines in NOAA's layout. missing(element, year, month, day) -> bool."""
    lines = []
    for year in years:
        for element in elements:
            for month in range(1, 13):
                days = calendar.monthrange(year, month)[1]
                text = f"{station:<11}{year:4d}{month:02d}{element}"
                for day in range(1, 32):
                    if day > days or (missing and missing(element, year, month, day)):
                        text += "-9999" + "   "
                    else:
                        flag = qflag(element, year, month, day) if qflag else " "
                        text += f"{100:5d}" + " " + flag + "S"
                lines.append(text)
    return lines


ALL = range(1991, 2026)

print("Scoring (one station per boundary)")
cases = {
    #  name                     lines                                                       baseline_valid, window_valid, meets_80, baseline_eligible, trend
    "A_FULL": (dly_lines("A", ALL), (30, 35, True, True, True),
               "all days present in every year"),
    "B_STAGGER": (dly_lines("B", ALL, missing=lambda e, y, m, d: ordinal(y, m, d) % 12 == {"TMAX": 0, "TMIN": 1, "PRCP": 2}[e]),
                  (0, 0, True, False, False),
                  "each variable ~92% present but never all three on the same day"),
    "C_QFLAG": (dly_lines("C", ALL, qflag=lambda e, y, m, d: "G" if e == "PRCP" and ordinal(y, m, d) % 4 == 0 else " "),
                (0, 0, False, False, False),
                "quality-flagged rainfall values (25% of days) do not count"),
    "D_LEAP330": (dly_lines("D", ALL, missing=lambda e, y, m, d: y == 2020 and ordinal(y, m, d) <= 36),
                  (30, 35, True, True, True),
                  "330 of 366 days (90.16%) in a leap year is a valid year"),
    "D_LEAP329": (dly_lines("D", ALL, missing=lambda e, y, m, d: y == 2020 and ordinal(y, m, d) <= 37),
                  (29, 34, True, True, True),
                  "329 of 366 days (89.89%) is not a valid year"),
    "E_BOUND_OK": (dly_lines("E", [y for y in ALL if y not in {1991, 1992, 1993, 1994, 1995, 1997}]),
                   (24, 29, True, False, False),
                   "8,767 baseline days is just above the 80% line, but only 24 valid years"),
    "E_BOUND_FAIL": (dly_lines("E", [y for y in ALL if y not in {1991, 1992, 1993, 1994, 1995, 1996}]),
                     (24, 29, False, False, False),
                     "8,766 baseline days is just below the 80% line"),
    "F_30_YEARS": (dly_lines("F", range(1991, 2021)),
                   (30, 30, True, True, True), "exactly 30 valid years is trend eligible"),
    "G_29_YEARS": (dly_lines("G", range(1991, 2020)),
                   (29, 29, True, True, False), "29 valid years is not trend eligible"),
    "H_NO_TMIN": (dly_lines("H", ALL, elements=("TMAX", "PRCP")),
                  (0, 0, False, False, False), "a variable that is never recorded"),
}
for name, (lines, expected, note) in cases.items():
    r = module.screen_lines(lines)
    got = (r["baseline_valid_years"], r["valid_years_1991_2025"], bool(r["meets_80_all_variables"]),
           bool(r["baseline_eligible"]), bool(r["enough_years_for_trend"]))
    check(f"{name}: {note}", got == expected, f"(got {got}, expected {expected})" if got != expected else "")

print("Selection and install (throw-away project)")


def inventory_line(station, lat, lon, element, first, last):
    return f"{station:<11} {lat:8.4f} {lon:9.4f} {element} {first:4d} {last:4d}"


# station, lat, lon, years present, missing rule
stations = {
    "USW00011111": (45.0, -75.0, ALL, None),                                   # best in its cell
    "USW00022222": (45.5, -75.2, range(1991, 2022), None),                     # 60 km from the best: too close
    "USW00033333": (42.0, -72.0, range(1991, 2024), None),                     # 400 km away: selected
    "USW00044444": (47.0, -78.0, ALL, lambda e, y, m, d: ordinal(y, m, d) % 12 == {"TMAX": 0, "TMIN": 1, "PRCP": 2}[e]),  # fails
    "CFM00064001": (-5.0, 25.0, ALL, lambda e, y, m, d: ordinal(y, m, d) % 12 == {"TMAX": 0, "TMIN": 1, "PRCP": 2}[e]),   # fails (Africa)
    "MYM00048647": (5.0, 105.0, range(1991, 2021), None),                      # 30 valid years: selected
    "USW00023174": (33.94, -118.39, range(1991, 2022), None),                  # required (Los Angeles)
    "USW00055555": (35.0, -117.0, ALL, None),                                  # better, but 170 km from the required station
}
expected_selected = ["MYM00048647", "USW00011111", "USW00023174", "USW00033333"]

with tempfile.TemporaryDirectory() as tmp:
    project = Path(tmp)
    (project / "src").mkdir()
    shutil.copy(ROOT / "src" / "screen_candidate_stations.py", project / "src")
    shutil.copytree(ROOT / "reference", project / "reference")
    raw = project / "data" / "raw" / "ghcn_d"
    source = project / "data" / "source_dly"
    raw.mkdir(parents=True)
    source.mkdir(parents=True)

    inventory = []
    for sid, (lat, lon, years, rule) in stations.items():
        for element in ELEMENTS:
            inventory.append(inventory_line(sid, lat, lon, element, 1980, 2025))
        (source / f"{sid}.dly").write_text("\n".join(dly_lines(sid, years, missing=rule)) + "\n")
    inventory.append(inventory_line("USW00066666", 40.0, -100.0, "TMAX", 1980, 2025))   # no TMIN or PRCP: not a candidate
    (raw / "ghcnd-inventory.txt").write_text("\n".join(inventory) + "\n")
    (raw / "OLD00000001.dly").write_text("old file\n")                                  # from the previous list
    (raw / "global_pilot_station_ids.txt").write_text("OLD00000001\n")

    def run(*extra):
        return subprocess.run(
            [sys.executable, str(project / "src" / "screen_candidate_stations.py"),
             "--source-dir", str(source), "--workers", "2", *extra],
            capture_output=True, text=True, cwd=project)

    result = run()
    check("script runs", result.returncode == 0,
          "" if result.returncode == 0 else "\n" + result.stdout[-1500:] + result.stderr[-1500:])
    screened = pd.read_csv(project / "data" / "screening" / "screening_results.csv")
    selected = pd.read_csv(project / "data" / "screening" / "selected_stations.csv")

    check("only the 8 stations with all three variables are screened (not USW00066666)",
          set(screened["station_id"]) == set(stations))
    check("selected: best of each cell, 300 km apart, required station kept",
          sorted(selected["station_id"]) == expected_selected, f"(got {sorted(selected['station_id'])})")
    check("USW00022222 is left out: 60 km from a better station",
          "USW00022222" not in set(selected["station_id"]))
    check("USW00055555 is left out: it is 170 km from the required Los Angeles station",
          "USW00055555" not in set(selected["station_id"]))
    check("failing stations are in the results but not selected",
          not screened.set_index("station_id").loc[["USW00044444", "CFM00064001"], "baseline_eligible"].any())
    check("continents: 3 North America, 1 Asia",
          selected["continent"].value_counts().to_dict() == {"North America": 3, "Asia": 1})
    check("the new list is written, one ID per line",
          (raw / "global_pilot_station_ids.txt").read_text().split() == expected_selected)
    check("the previous list is kept",
          (raw / "global_pilot_station_ids_previous.txt").read_text().split() == ["OLD00000001"])
    check("old .dly files are moved to the archive, not deleted",
          not (raw / "OLD00000001.dly").exists() and (project / "data" / "raw" / "ghcn_d_archive" / "OLD00000001.dly").exists())
    check("exactly the selected .dly files are in data/raw/ghcn_d",
          sorted(p.stem for p in raw.glob("*.dly")) == expected_selected)

    again = run()
    check("a rerun skips stations already screened and changes nothing",
          again.returncode == 0 and "to screen: 0" in again.stdout
          and (raw / "global_pilot_station_ids.txt").read_text().split() == expected_selected)

    capped = run("--max-stations", "3")
    capped_ids = (raw / "global_pilot_station_ids.txt").read_text().split()
    check("--max-stations 3 keeps 3 and keeps the required station",
          capped.returncode == 0 and len(capped_ids) == 3 and "USW00023174" in capped_ids, f"(got {capped_ids})")

print("Compare mode (screening against the pipeline's own table)")
with tempfile.TemporaryDirectory() as tmp:
    project = Path(tmp)
    (project / "src").mkdir()
    shutil.copy(ROOT / "src" / "screen_candidate_stations.py", project / "src")
    shutil.copytree(ROOT / "reference", project / "reference")
    source = project / "dly"
    source.mkdir()
    for name in ("A_FULL", "E_BOUND_FAIL", "D_LEAP329"):
        (source / f"{name}.dly").write_text("\n".join(cases[name][0]) + "\n")
    table = pd.DataFrame([
        ("A_FULL", 30, 35, True, True, True),
        ("E_BOUND_FAIL", 24, 29, False, False, False),
        ("D_LEAP329", 29, 34, True, True, True),
    ], columns=["station_id", "baseline_valid_years", "valid_years_1991_2025",
                "meets_80_all_variables", "baseline_eligible", "enough_years_for_trend"])
    table.to_csv(project / "good.csv", index=False)
    table.loc[2, "valid_years_1991_2025"] = 35
    table.to_csv(project / "bad.csv", index=False)

    def compare(csv):
        return subprocess.run(
            [sys.executable, str(project / "src" / "screen_candidate_stations.py"),
             "--compare", str(project / csv), "--source-dir", str(source)],
            capture_output=True, text=True, cwd=project)

    good, bad = compare("good.csv"), compare("bad.csv")
    check("matching table: no differences, exit code 0",
          good.returncode == 0 and "Differences: 0" in good.stdout)
    check("one wrong number is reported and gives exit code 1",
          bad.returncode == 1 and "Differences: 1" in bad.stdout and "D_LEAP329" in bad.stdout)

print("Failure handling (the crash seen on real NOAA downloads)")
import http.client
import urllib.request


class FakeResponse:
    def __init__(self, text):
        self.text = text

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self.text.encode("ascii")


good_text = "\n".join(cases["A_FULL"][0]) + "\n"
real_urlopen, real_sleep = urllib.request.urlopen, module.time.sleep
module.time.sleep = lambda seconds: None          # no waiting in the test
with tempfile.TemporaryDirectory() as tmp:
    module.ELIGIBLE_DIR = Path(tmp) / "kept"      # nothing kept yet
    calls = {"n": 0}

    def drops_twice(request, timeout=0):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise http.client.IncompleteRead(b"x" * 10, 500)
        return FakeResponse(good_text)

    urllib.request.urlopen = drops_twice
    lines, status = module.read_station("USW00000001", None)
    check("a download that stops halfway is retried and then succeeds",
          status == "ok" and calls["n"] == 3 and lines is not None, f"(status {status}, attempts {calls['n']})")

    def always_drops(request, timeout=0):
        raise http.client.IncompleteRead(b"x", 500)

    urllib.request.urlopen = always_drops
    lines, status = module.read_station("USW00000001", None)
    check("a download that always fails gives 'download_failed' and does not crash",
          status == "download_failed" and lines is None)

    def not_found(request, timeout=0):
        raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)

    urllib.request.urlopen = not_found
    check("a missing file (404) is reported as not_found",
          module.read_station("USW00000001", None)[1] == "not_found")

    module.ELIGIBLE_DIR.mkdir()
    (module.ELIGIBLE_DIR / "USW00000002.dly").write_text(good_text)

    def must_not_be_called(request, timeout=0):
        raise AssertionError("downloaded a station that was already kept")

    urllib.request.urlopen = must_not_be_called
    lines, status = module.read_station("USW00000002", None)
    check("a station kept from an earlier run is read from disk, not downloaded again",
          status == "ok" and lines is not None)
urllib.request.urlopen, module.time.sleep = real_urlopen, real_sleep

with tempfile.TemporaryDirectory() as tmp:
    module.SCREEN_DIR = Path(tmp)
    module.RESULTS_PATH = Path(tmp) / "screening_results.csv"
    module.ELIGIBLE_DIR = Path(tmp) / "kept"
    countries = module.load_countries()
    candidates = pd.DataFrame(
        [(f"USW0000000{i}", 40.0 + i, -100.0, 1980, 2025) for i in range(1, 6)],
        columns=["station_id", "lat", "lon", "first_year", "last_year"])
    real_screen_station = module.screen_station

    def breaks_on_three(row, source_dir, countries):
        if row["station_id"] == "USW00000003":
            raise ValueError("unexpected problem in one station")
        return {"station_id": row["station_id"], "lat": row["lat"], "lon": row["lon"], "country": "United States",
                "continent": "North America", "status": "ok", "baseline_eligible": True,
                "enough_years_for_trend": True}

    # an unexpected error inside one station is recorded and the run continues
    module.read_station = lambda station_id, source_dir: (_ for _ in ()).throw(ValueError("boom")) if station_id == "USW00000003" else (good_text.splitlines(), "ok")
    results = module.screen_all(candidates, None, 2, countries)
    status_by_id = results.set_index("station_id")["status"].to_dict()
    check("one unexpected error does not stop the run: that station is 'error', the rest are 'ok'",
          status_by_id["USW00000003"] == "error" and sum(v == "ok" for v in status_by_id.values()) == 4, f"({status_by_id})")
    saved = pd.read_csv(module.RESULTS_PATH)
    check("results are saved to disk", len(saved) == 5)

    # a rerun retries the failed station only
    module.read_station = lambda station_id, source_dir: (good_text.splitlines(), "ok")
    rerun = module.screen_all(candidates, None, 2, countries)
    check("a rerun retries only the failed station and then all 5 are 'ok'",
          (rerun["status"] == "ok").all() and len(rerun) == 5)

    # Ctrl+C: what was finished is saved
    module.RESULTS_PATH.unlink()
    module.screen_station = lambda row, source_dir, countries: (_ for _ in ()).throw(KeyboardInterrupt()) if row["station_id"] == "USW00000003" else breaks_on_three(row, source_dir, countries)
    try:
        module.screen_all(candidates, None, 1, countries)
        interrupted = False
    except KeyboardInterrupt:
        interrupted = True
    saved = pd.read_csv(module.RESULTS_PATH) if module.RESULTS_PATH.exists() else pd.DataFrame()
    check("stopping the run (Ctrl+C) keeps the stations already screened",
          interrupted and len(saved) == 2, f"(interrupted {interrupted}, saved {len(saved)})")
    module.screen_station = real_screen_station

print()
if failures:
    print(f"FAILED: {len(failures)} check(s): {failures}")
    sys.exit(1)
print("All checks passed.")
