"""
Check src/check_station_eligibility.py and its use in calculate_thresholds.py
on synthetic data with known answers.

Builds a throw-away project folder, copies the two scripts into it, runs them
with Spark, and checks the outputs. Nothing in the real data/ folder is read
or written.

Run from the repository root:  python tests/check_station_eligibility.py
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
failures = []


def check(name, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {name} {detail}")
    if not condition:
        failures.append(name)


def run(script, cwd):
    result = subprocess.run(
        [sys.executable, str(cwd / "src" / script)],
        capture_output=True, text=True, cwd=cwd,
    )
    return result


def station_frame(station_id, first_year=1991, last_year=2025, mask=None):
    """Daily TMAX, TMIN, PRCP for one station. mask(df) sets values to NaN."""
    dates = pd.date_range(f"{first_year}-01-01", f"{last_year}-12-31")
    df = pd.DataFrame({"date": dates})
    df["doy"] = df["date"].dt.dayofyear
    df["year"] = df["date"].dt.year
    df["TMAX"] = 20.0 + 5 * np.sin(df["doy"] / 58.0)
    df["TMIN"] = df["TMAX"] - 8.0
    df["PRCP"] = np.where(df["doy"] % 3 == 0, 5.0, 0.0)
    if mask is not None:
        mask(df)
    df["station_id"] = station_id
    return df


def drop_days(df, years, every):
    """Set all three variables to NaN on days where doy % every == 0."""
    rows = df["year"].isin(years) & (df["doy"] % every == 0)
    df.loc[rows, ["TMAX", "TMIN", "PRCP"]] = np.nan


def drop_n_days_in_year(df, year, n):
    rows = df.index[df["year"] == year][:n]
    df.loc[rows, ["TMAX", "TMIN", "PRCP"]] = np.nan


def staggered(df):
    """Each variable missing 1 day in 12, on different days."""
    df.loc[df["doy"] % 12 == 0, "TMAX"] = np.nan
    df.loc[df["doy"] % 12 == 1, "TMIN"] = np.nan
    df.loc[df["doy"] % 12 == 2, "PRCP"] = np.nan


stations = {
    "ST_OK": station_frame("ST_OK"),
    # exactly 25 valid baseline years: 5 baseline years with 20% of days missing
    "ST_25": station_frame("ST_25", mask=lambda d: drop_days(d, range(1991, 1996), 5)),
    # 24 valid baseline years
    "ST_24": station_frame("ST_24", mask=lambda d: drop_days(d, range(1991, 1997), 5)),
    # each variable is present on ~92% of days, but never all three on the same day
    "ST_SAMEDAY": station_frame("ST_SAMEDAY", mask=staggered),
    # leap year 2020 has 366 days: 330 complete days = 90.16% (valid)
    "ST_LEAP_330": station_frame("ST_LEAP_330", mask=lambda d: drop_n_days_in_year(d, 2020, 36)),
    # 329 complete days = 89.89% (not valid)
    "ST_LEAP_329": station_frame("ST_LEAP_329", mask=lambda d: drop_n_days_in_year(d, 2020, 37)),
    # complete data, but PRCP fails the 80% rule in prepare_baseline
    "ST_COMPL": station_frame("ST_COMPL"),
    # only a few days in 2026
    "ST_NONE": station_frame("ST_NONE", 2026, 2026).head(10),
}

# What the 80% rule (prepare_baseline.py) would have said
failing_80 = {"ST_COMPL": ["PRCP"], "ST_NONE": ["TMAX", "TMIN", "PRCP"]}

all_days = pd.concat(stations.values(), ignore_index=True)
long = all_days.melt(
    id_vars=["station_id", "date"], value_vars=["TMAX", "TMIN", "PRCP"],
    var_name="element", value_name="value",
).dropna(subset=["value"])
long["date"] = long["date"].dt.date

completeness = pd.DataFrame(
    [
        (sid, el, "NO" if el in failing_80.get(sid, []) else "YES")
        for sid in stations for el in ("TMAX", "TMIN", "PRCP")
    ],
    columns=["station_id", "element", "meets_80_percent"],
)

baseline_stations = ["ST_OK", "ST_25", "ST_24", "ST_SAMEDAY", "ST_COMPL"]
baseline = long[
    long["station_id"].isin(baseline_stations)
    & (pd.to_datetime(long["date"]).dt.year <= 2020)
]

with tempfile.TemporaryDirectory() as tmp:
    project = Path(tmp)
    (project / "src").mkdir()
    for script in ("check_station_eligibility.py", "calculate_thresholds.py"):
        shutil.copy(ROOT / "src" / script, project / "src" / script)
    parquet = project / "data" / "parquet"
    parquet.mkdir(parents=True)
    long.to_parquet(parquet / "ghcn_daily", index=False)
    baseline.to_parquet(parquet / "ghcn_baseline_1991_2020", index=False)
    completeness.to_parquet(parquet / "station_completeness_1991_2020", index=False)

    print("Eligibility script")
    result = run("check_station_eligibility.py", project)
    check("script runs", result.returncode == 0,
          "" if result.returncode == 0 else "\n" + result.stdout[-1500:] + result.stderr[-1500:])
    out = pd.read_csv(project / "data" / "export" / "station_eligibility.csv").set_index("station_id")

    expected = {
        #               baseline_valid, window_valid, meets_80, eligible, enough_for_trend
        "ST_OK":        (30, 35, True,  True,  True),
        "ST_25":        (25, 30, True,  True,  True),
        "ST_24":        (24, 29, True,  False, False),
        "ST_SAMEDAY":   (0,  0,  True,  False, False),
        "ST_LEAP_330":  (30, 35, True,  True,  True),
        "ST_LEAP_329":  (29, 34, True,  True,  True),
        "ST_COMPL":     (30, 35, False, False, True),
        "ST_NONE":      (0,  0,  False, False, False),
    }
    notes = {
        "ST_25": "exactly 25 valid baseline years is eligible",
        "ST_24": "24 valid baseline years is not eligible",
        "ST_SAMEDAY": "each variable >= 90% but never together = no valid year",
        "ST_LEAP_330": "330 of 366 days (90.16%) is a valid year",
        "ST_LEAP_329": "329 of 366 days (89.89%) is not a valid year",
        "ST_COMPL": "passes the years rule but fails the 80% rule",
    }
    for sid, values in expected.items():
        got = tuple(out.loc[sid, ["baseline_valid_years", "valid_years_1991_2025",
                                  "meets_80_all_variables", "baseline_eligible",
                                  "enough_years_for_trend"]].tolist())
        got = (int(got[0]), int(got[1]), bool(got[2]), bool(got[3]), bool(got[4]))
        check(f"{sid}: {notes.get(sid, 'as expected')}", got == values,
              f"(got {got}, expected {values})" if got != values else "")

    print("Threshold gate (calculate_thresholds.py)")
    result = run("calculate_thresholds.py", project)
    check("script runs with station_eligibility present", result.returncode == 0,
          "" if result.returncode == 0 else "\n" + result.stdout[-1500:] + result.stderr[-1500:])
    thresholds = pd.read_parquet(parquet / "extreme_thresholds_1991_2020")
    got = set(thresholds["station_id"])
    check("only baseline-eligible stations get thresholds", got == {"ST_OK", "ST_25"},
          f"(got {sorted(got)})")
    check("no warning when the eligibility table exists", "WARNING: station_eligibility" not in result.stdout)

    shutil.rmtree(parquet / "station_eligibility")
    shutil.rmtree(parquet / "extreme_thresholds_1991_2020")
    result = run("calculate_thresholds.py", project)
    got = set(pd.read_parquet(parquet / "extreme_thresholds_1991_2020")["station_id"])
    check("without the eligibility table: warns that the rule is not applied",
          "WARNING: station_eligibility not found" in result.stdout)
    check("without the eligibility table: only the 80% rule applies",
          got == {"ST_OK", "ST_25", "ST_24", "ST_SAMEDAY"}, f"(got {sorted(got)})")

print()
if failures:
    print(f"FAILED: {len(failures)} check(s): {failures}")
    sys.exit(1)
print("All checks passed.")
