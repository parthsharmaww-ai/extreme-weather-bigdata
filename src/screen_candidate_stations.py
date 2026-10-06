"""
Screen candidate stations with the pipeline's own eligibility rules, then pick
a usable, well-spread set of stations.

Why: most stations that cover 1991-2025 on paper do not pass the rules in
docs/event_definitions.md, because those rules need TMAX, TMIN and PRCP all
present on the same day. Screening finds the stations that pass before the
whole pipeline is run on them.

Rules (same as the pipeline; see prepare_raw.py, prepare_baseline.py and
check_station_eligibility.py):
- A daily value counts only if it is not -9999 and its quality flag is blank.
- Valid year: days_present_pct >= 90, where days_present_pct is the share of
  days in the year with TMAX, TMIN and PRCP all present (rounded half up to
  2 decimals).
- Baseline eligible: each variable has >= 80% of the 10,958 days in 1991-2020,
  and at least 25 of those 30 years are valid years.
- Trend eligible: at least 30 valid years in 1991-2025.

Steps:
1. Read NOAA's inventory (downloaded if missing). Candidates are stations with
   TMAX, TMIN and PRCP covering 1991 to 2024. Up to a few candidates per
   10-degree cell are screened, so every part of the world is tried.
2. Each candidate's .dly file is read and scored. Only candidates that pass are
   kept on disk. Results are saved, so a rerun skips what is done.
3. From the passing stations, at most --per-cell per cell are selected, at
   least --min-separation-km apart, up to --max-stations in total.
4. The selection is installed: the chosen .dly files are copied into
   data/raw/ghcn_d, the list is written to global_pilot_station_ids.txt (the
   old list is saved next to it), and the old .dly files are moved to
   data/raw/ghcn_d_archive so they are not processed again.

Check mode: --compare data/export/station_eligibility.csv --source-dir DIR
screens the .dly files in DIR and compares them with the pipeline's own
eligibility table. Every number must match.
"""

import argparse
import calendar
import http.client
import math
import shutil
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "ghcn_d"
INVENTORY_PATH = RAW_DIR / "ghcnd-inventory.txt"
PILOT_IDS_PATH = RAW_DIR / "global_pilot_station_ids.txt"
PREVIOUS_IDS_PATH = RAW_DIR / "global_pilot_station_ids_previous.txt"
ARCHIVE_DIR = PROJECT_ROOT / "data" / "raw" / "ghcn_d_archive"
SCREEN_DIR = PROJECT_ROOT / "data" / "screening"
RESULTS_PATH = SCREEN_DIR / "screening_results.csv"
SELECTED_PATH = SCREEN_DIR / "selected_stations.csv"
ELIGIBLE_DIR = SCREEN_DIR / "eligible_dly"
COUNTRY_PATH = PROJECT_ROOT / "reference" / "country_continent.csv"

GHCN_BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"
INVENTORY_URL = f"{GHCN_BASE}/ghcnd-inventory.txt"
DLY_URL = f"{GHCN_BASE}/all/{{}}.dly"
USER_AGENT = "extreme-weather-bigdata-course-project"

ELEMENTS = ("TMAX", "TMIN", "PRCP")
VALID_YEAR_PCT = Decimal("90")
BASELINE_FIRST, BASELINE_LAST = 1991, 2020
TREND_FIRST, TREND_LAST = 1991, 2025
EXPECTED_BASELINE_DAYS = 10958
COMPLETENESS_THRESHOLD = 0.80
MIN_BASELINE_VALID_YEARS = 25
MIN_TREND_VALID_YEARS = 30

CANDIDATE_FIRST_YEAR = 1991     # inventory must start by this year ...
CANDIDATE_LAST_YEAR = 2024      # ... and run to at least this year
# Third letter of a station ID is its network. WMO, WBAN, national and ECA&D
# stations tend to report all three variables, so they are tried first.
NETWORK_PRIOR = {"W": 2, "M": 2, "N": 2, "E": 2}

DEFAULT_REQUIRED = ["USW00094728", "USW00023174"]
RUSSIA_SPLIT_LONGITUDE = 60.0
EARTH_RADIUS_KM = 6371.0088

RESULT_COLUMNS = [
    "station_id", "lat", "lon", "country", "continent", "status",
    "baseline_valid_years", "valid_years_1991_2025", "meets_80_all_variables",
    "baseline_eligible", "enough_years_for_trend",
    "baseline_days_tmax", "baseline_days_tmin", "baseline_days_prcp",
]


# ---------------------------------------------------------------- scoring ---

def screen_lines(lines):
    """Score one station from the lines of its .dly file."""
    present = {element: {} for element in ELEMENTS}

    for line in lines:
        line = line.rstrip("\r\n")
        if len(line) < 21:
            continue
        element = line[17:21]
        if element not in present:
            continue
        try:
            year = int(line[11:15])
            month = int(line[15:17])
        except ValueError:
            continue
        if not TREND_FIRST <= year <= TREND_LAST or not 1 <= month <= 12:
            continue

        line = line.ljust(269)
        days = present[element].setdefault(year, set())
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            start = 21 + 8 * (day - 1)
            try:
                value = int(line[start:start + 5])
            except ValueError:
                continue
            if value == -9999 or line[start + 6].strip() != "":
                continue
            days.add((month, day))

    baseline_days = {
        element: sum(
            len(days) for year, days in present[element].items()
            if BASELINE_FIRST <= year <= BASELINE_LAST
        )
        for element in ELEMENTS
    }
    meets_80 = all(
        baseline_days[element] / EXPECTED_BASELINE_DAYS >= COMPLETENESS_THRESHOLD
        for element in ELEMENTS
    )

    baseline_valid = window_valid = 0
    for year in range(TREND_FIRST, TREND_LAST + 1):
        complete = len(
            present["TMAX"].get(year, set())
            & present["TMIN"].get(year, set())
            & present["PRCP"].get(year, set())
        )
        days_in_year = 366 if calendar.isleap(year) else 365
        pct = (Decimal(complete) * 100 / Decimal(days_in_year)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        if pct >= VALID_YEAR_PCT:
            window_valid += 1
            if year <= BASELINE_LAST:
                baseline_valid += 1

    return {
        "baseline_valid_years": baseline_valid,
        "valid_years_1991_2025": window_valid,
        "meets_80_all_variables": meets_80,
        "baseline_eligible": meets_80 and baseline_valid >= MIN_BASELINE_VALID_YEARS,
        "enough_years_for_trend": window_valid >= MIN_TREND_VALID_YEARS,
        "baseline_days_tmax": baseline_days["TMAX"],
        "baseline_days_tmin": baseline_days["TMIN"],
        "baseline_days_prcp": baseline_days["PRCP"],
    }


# --------------------------------------------------------------- geography ---

def load_countries():
    return pd.read_csv(COUNTRY_PATH).set_index("country_code")


def country_and_continent(station_id, lon, countries):
    code = station_id[:2]
    if code not in countries.index:
        return None, None
    country = countries.loc[code, "country"]
    continent = countries.loc[code, "continent"]
    if code == "RS":
        continent = "Europe" if lon < RUSSIA_SPLIT_LONGITUDE else "Asia"
    return country, continent


def distance_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


def cell_of(lat, lon, cell_degrees):
    return (math.floor(lat / cell_degrees), math.floor(lon / cell_degrees))


# --------------------------------------------------------------- candidates ---

def parse_inventory(path):
    """One row per station that has TMAX, TMIN and PRCP, with their year ranges."""
    first, last, position = {}, {}, {}
    with open(path, encoding="ascii", errors="replace") as handle:
        for line in handle:
            element = line[31:35]
            if element not in ELEMENTS:
                continue
            station_id = line[0:11]
            try:
                first.setdefault(station_id, {})[element] = int(line[36:40])
                last.setdefault(station_id, {})[element] = int(line[41:45])
                position[station_id] = (float(line[12:20]), float(line[21:30]))
            except ValueError:
                continue

    rows = []
    for station_id, firsts in first.items():
        if len(firsts) < 3:
            continue
        rows.append({
            "station_id": station_id,
            "lat": position[station_id][0],
            "lon": position[station_id][1],
            "first_year": max(firsts.values()),
            "last_year": min(last[station_id].values()),
        })
    return pd.DataFrame(rows, columns=["station_id", "lat", "lon", "first_year", "last_year"])


def choose_candidates(inventory, cell_degrees, per_cell, max_candidates, required):
    """Up to per_cell candidates for each cell, best prior first."""
    pool = inventory[
        (inventory["first_year"] <= CANDIDATE_FIRST_YEAR)
        & (inventory["last_year"] >= CANDIDATE_LAST_YEAR)
        & inventory["lat"].between(-90, 90)
        & inventory["lon"].between(-180, 180)
    ].copy()
    if pool.empty:
        return pool.assign(cell=[], rank=[])

    pool["cell"] = [cell_of(a, b, cell_degrees) for a, b in zip(pool["lat"], pool["lon"])]
    pool["prior"] = pool["station_id"].str[2].map(NETWORK_PRIOR).fillna(0)
    pool = pool.sort_values(
        ["prior", "first_year", "last_year", "station_id"],
        ascending=[False, True, False, True],
    )
    pool["rank"] = pool.groupby("cell").cumcount()
    chosen = pool[pool["rank"] < per_cell]
    chosen = chosen.sort_values(["rank", "cell", "station_id"]).head(max_candidates)

    missing = [s for s in required if s in set(pool["station_id"]) and s not in set(chosen["station_id"])]
    if missing:
        chosen = pd.concat([chosen, pool[pool["station_id"].isin(missing)]])
    return chosen.reset_index(drop=True)


# ----------------------------------------------------------------- fetching ---

def read_station(station_id, source_dir):
    """Lines of the station's .dly file, or None if it cannot be read."""
    if source_dir is not None:
        path = Path(source_dir) / f"{station_id}.dly"
        if not path.exists():
            return None, "not_found"
        return path.read_text(encoding="ascii", errors="replace").splitlines(), "ok"

    kept = ELIGIBLE_DIR / f"{station_id}.dly"
    if kept.exists():                       # passed in an earlier run: no need to download again
        return kept.read_text(encoding="ascii", errors="replace").splitlines(), "ok"

    request = urllib.request.Request(
        DLY_URL.format(station_id), headers={"User-Agent": USER_AGENT}
    )
    for attempt in range(4):
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                text = response.read().decode("ascii", errors="replace")
            return text.splitlines(), "ok"
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None, "not_found"
        except (urllib.error.URLError, http.client.HTTPException, TimeoutError, OSError):
            pass                            # includes a download that stops halfway
        time.sleep(2 * (attempt + 1))
    return None, "download_failed"


def screen_station(row, source_dir, countries):
    country, continent = country_and_continent(row["station_id"], row["lon"], countries)
    result = {
        "station_id": row["station_id"], "lat": row["lat"], "lon": row["lon"],
        "country": country, "continent": continent, "status": "error",
    }
    try:
        lines, status = read_station(row["station_id"], source_dir)
        result["status"] = status
        if lines is not None:
            result.update(screen_lines(lines))
            if result["baseline_eligible"] and result["enough_years_for_trend"] and source_dir is None:
                ELIGIBLE_DIR.mkdir(parents=True, exist_ok=True)
                (ELIGIBLE_DIR / f"{row['station_id']}.dly").write_text(
                    "\n".join(lines) + "\n", encoding="ascii"
                )
    except Exception:                       # the station is tried again on the next run
        result["status"] = "error"
    return result


def screen_all(candidates, source_dir, workers, countries):
    """Screen every candidate that is not already in the results file.

    Results are saved every 25 stations and when the run stops for any reason
    (including Ctrl+C), so a rerun only does what is missing.
    """
    SCREEN_DIR.mkdir(parents=True, exist_ok=True)
    done = pd.DataFrame(columns=RESULT_COLUMNS)
    if RESULTS_PATH.exists():
        done = pd.read_csv(RESULTS_PATH)
        done = done[done["status"].isin(["ok", "not_found"])]

    todo = candidates[~candidates["station_id"].isin(done["station_id"])]
    print(f"Candidates: {len(candidates):,} | already screened: "
          f"{len(candidates) - len(todo):,} | to screen: {len(todo):,}")

    results = []

    def save():
        frames = [f for f in (done, pd.DataFrame(results, columns=RESULT_COLUMNS)) if not f.empty]
        combined = (
            pd.concat(frames, ignore_index=True)
            if frames else pd.DataFrame(columns=RESULT_COLUMNS)
        )
        combined = combined.drop_duplicates("station_id", keep="last")
        combined.to_csv(RESULTS_PATH, index=False)
        return combined

    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        futures = [
            pool.submit(screen_station, row, source_dir, countries)
            for row in todo.to_dict("records")
        ]
        for count, future in enumerate(as_completed(futures), start=1):
            results.append(future.result())
            if count % 25 == 0 or count == len(futures):
                passed = sum(
                    1 for r in results
                    if r.get("baseline_eligible") and r.get("enough_years_for_trend")
                )
                print(f"  screened {count:,} of {len(futures):,} | passing so far: {passed:,}")
                save()
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        combined = save()

    failed = int(combined["status"].isin(["download_failed", "error"]).sum())
    if failed:
        print(f"  {failed} station(s) could not be read; they are tried again on the next run.")
    return combined[combined["station_id"].isin(candidates["station_id"])]


# ---------------------------------------------------------------- selection ---

def select_stations(results, cell_degrees, per_cell, min_separation_km, max_stations, required):
    """Pick well-spread stations from those that pass every rule."""
    ok = results[
        (results["status"] == "ok")
        & results["baseline_eligible"].astype(bool)
        & results["enough_years_for_trend"].astype(bool)
    ].copy()
    if ok.empty:
        return ok

    ok["cell"] = [cell_of(a, b, cell_degrees) for a, b in zip(ok["lat"], ok["lon"])]
    ok = ok.sort_values(
        ["valid_years_1991_2025", "baseline_valid_years", "station_id"],
        ascending=[False, False, True],
    )
    required_ok = [s for s in required if s in set(ok["station_id"])]
    required_rows = ok[ok["station_id"].isin(required_ok)]

    picked = []
    for _, group in ok.groupby("cell", sort=True):
        chosen_here = [r for r in required_rows.itertuples() if r.cell == group["cell"].iloc[0]]
        for row in group.itertuples():
            if row.station_id in required_ok:
                continue
            if len(chosen_here) >= per_cell:
                break
            if all(distance_km(row.lat, row.lon, c.lat, c.lon) >= min_separation_km
                   for c in chosen_here):
                chosen_here.append(row)
        picked.extend(chosen_here)

    selected = pd.DataFrame([r._asdict() for r in picked]).drop(columns="Index", errors="ignore")
    if len(selected) > max_stations:
        selected = thin(selected, max_stations, required_ok)
    return selected.sort_values("station_id").reset_index(drop=True)


def thin(selected, limit, required):
    """Farthest-point thinning, keeping the required stations."""
    rows = selected.to_dict("records")
    keep = [r for r in rows if r["station_id"] in required] or [rows[0]]
    rest = [r for r in rows if r not in keep]
    while len(keep) < limit and rest:
        best = max(
            rest,
            key=lambda r: min(distance_km(r["lat"], r["lon"], k["lat"], k["lon"]) for k in keep),
        )
        keep.append(best)
        rest.remove(best)
    return pd.DataFrame(keep)


def install(selected, source_dir, keep_old):
    """Copy chosen files into data/raw/ghcn_d and write the new station list."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    chosen = set(selected["station_id"])

    if PILOT_IDS_PATH.exists() and not PREVIOUS_IDS_PATH.exists():
        shutil.copy(PILOT_IDS_PATH, PREVIOUS_IDS_PATH)

    moved = 0
    if not keep_old:
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        for path in sorted(RAW_DIR.glob("*.dly")):
            if path.stem not in chosen:
                shutil.move(str(path), str(ARCHIVE_DIR / path.name))
                moved += 1

    missing = []
    for station_id in sorted(chosen):
        target = RAW_DIR / f"{station_id}.dly"
        if target.exists():
            continue
        origin = (Path(source_dir) if source_dir else ELIGIBLE_DIR) / f"{station_id}.dly"
        if origin.exists():
            shutil.copy(origin, target)
        else:
            missing.append(station_id)

    PILOT_IDS_PATH.write_text("\n".join(sorted(chosen)) + "\n", encoding="ascii")
    return moved, missing


# ------------------------------------------------------------------- report ---

def report(results, selected):
    ok = results[results["status"] == "ok"]
    passing = ok[ok["baseline_eligible"].astype(bool) & ok["enough_years_for_trend"].astype(bool)]
    table = pd.DataFrame({
        "screened": ok.groupby("continent").size(),
        "passing": passing.groupby("continent").size(),
        "selected": selected.groupby("continent").size() if len(selected) else None,
    }).fillna(0).astype(int)
    table.loc["TOTAL"] = table.sum()
    print("\nBy continent:")
    print(table.to_string())

    failed = int(results["status"].isin(["download_failed", "error"]).sum())
    if failed:
        print(f"\n{failed} candidate(s) could not be read. Run the script again "
              "to retry only those.")


def compare(source_dir, pipeline_csv, countries):
    """Screen the .dly files in source_dir and compare with the pipeline's table."""
    pipeline = pd.read_csv(pipeline_csv).set_index("station_id")
    rows = []
    for path in sorted(Path(source_dir).glob("*.dly")):
        lines = path.read_text(encoding="ascii", errors="replace").splitlines()
        rows.append({"station_id": path.stem, **screen_lines(lines)})
    mine = pd.DataFrame(rows).set_index("station_id")

    number_columns = ["baseline_valid_years", "valid_years_1991_2025"]
    flag_columns = ["meets_80_all_variables", "baseline_eligible", "enough_years_for_trend"]
    common = mine.index.intersection(pipeline.index)
    differences = []
    for station_id in common:
        for column in number_columns:
            a, b = int(mine.loc[station_id, column]), int(pipeline.loc[station_id, column])
            if a != b:
                differences.append((station_id, column, a, b))
        for column in flag_columns:
            a, b = bool(mine.loc[station_id, column]), bool(pipeline.loc[station_id, column])
            if a != b:
                differences.append((station_id, column, a, b))

    print("=" * 65)
    print("SCREENING VS PIPELINE ELIGIBILITY")
    print("=" * 65)
    print(f"Stations compared: {len(common):,}")
    print(f"Only in the screening folder: {len(mine.index.difference(pipeline.index)):,}")
    print(f"Only in the pipeline table: {len(pipeline.index.difference(mine.index)):,}")
    print(f"Differences: {len(differences)}")
    for station_id, column, a, b in differences[:20]:
        print(f"  {station_id} {column}: screening {a}, pipeline {b}")
    return len(differences) == 0 and len(common) > 0


# --------------------------------------------------------------------- main ---

def main():
    parser = argparse.ArgumentParser(
        description="Screen candidate stations with the pipeline's eligibility rules and select a usable, well-spread set."
    )
    parser.add_argument("--cell-degrees", type=float, default=10.0)
    parser.add_argument("--candidates-per-cell", type=int, default=5)
    parser.add_argument("--max-candidates", type=int, default=1000)
    parser.add_argument("--per-cell", type=int, default=2)
    parser.add_argument("--min-separation-km", type=float, default=300.0)
    parser.add_argument("--max-stations", type=int, default=120)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--required", nargs="*", default=DEFAULT_REQUIRED)
    parser.add_argument("--source-dir", help="read .dly files from this folder instead of downloading")
    parser.add_argument("--compare", help="pipeline station_eligibility.csv to compare against")
    parser.add_argument("--dry-run", action="store_true", help="list candidates and stop")
    parser.add_argument("--keep-old", action="store_true", help="do not move old .dly files away")
    args = parser.parse_args()

    if not COUNTRY_PATH.exists():
        print(f"ERROR: {COUNTRY_PATH} not found.")
        return False
    countries = load_countries()

    if args.compare:
        if not args.source_dir:
            print("ERROR: --compare needs --source-dir.")
            return False
        return compare(args.source_dir, args.compare, countries)

    if not INVENTORY_PATH.exists():
        print("Downloading NOAA's station inventory (about 45 MB) ...")
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        request = urllib.request.Request(INVENTORY_URL, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=300) as response, open(INVENTORY_PATH, "wb") as out:
            shutil.copyfileobj(response, out)

    inventory = parse_inventory(INVENTORY_PATH)
    candidates = choose_candidates(
        inventory, args.cell_degrees, args.candidates_per_cell,
        args.max_candidates, args.required,
    )
    qualifying = int(
        ((inventory["first_year"] <= CANDIDATE_FIRST_YEAR) & (inventory["last_year"] >= CANDIDATE_LAST_YEAR)).sum()
    )
    print("=" * 65)
    print("CANDIDATE SCREENING")
    print("=" * 65)
    print(f"Stations with TMAX, TMIN and PRCP covering 1991-2024: {qualifying:,}")
    if candidates.empty:
        print("ERROR: no candidates found.")
        return False

    if args.dry_run:
        names = [country_and_continent(s, lon, countries)[1] for s, lon in zip(candidates["station_id"], candidates["lon"])]
        print(pd.Series(names).fillna("(unknown)").value_counts().to_string())
        return True

    results = screen_all(candidates, args.source_dir, args.workers, countries)
    selected = select_stations(
        results, args.cell_degrees, args.per_cell,
        args.min_separation_km, args.max_stations, args.required,
    )
    if selected.empty:
        print("ERROR: no candidate passed every rule.")
        report(results, selected)
        return False

    selected.drop(columns=["cell"], errors="ignore").to_csv(SELECTED_PATH, index=False)
    report(results, selected)
    moved, missing = install(selected, args.source_dir, args.keep_old)

    print(f"\nSelected stations: {len(selected):,}")
    print(f"Station list written: {PILOT_IDS_PATH}")
    if PREVIOUS_IDS_PATH.exists():
        print(f"Previous list kept as: {PREVIOUS_IDS_PATH}")
    if moved:
        print(f"Moved {moved} old .dly file(s) to {ARCHIVE_DIR}")
    if missing:
        print(f"WARNING: {len(missing)} selected file(s) were not found and must be "
              f"downloaded: {', '.join(missing[:10])}")
    print("\nNext: run prepare_raw.py and the rest of the pipeline (docs/RUNBOOK.md).")
    return True


if __name__ == "__main__":
    if not main():
        sys.exit(1)
