"""
Check src/validate_against_storm_events.py on a small scenario whose answers
were worked out by hand.

Run from the repository root:  python tests/check_storm_events_validation.py
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


events = pd.DataFrame([
    # station, type, start, end
    ("S_NY", "HEATWAVE", "2023-07-10", "2023-07-14"),     # 5 days
    ("S_TX", "HEATWAVE", "2023-08-20", "2023-08-22"),     # 3 days, no storm record
    ("S_NY", "HEAVY_RAIN", "2023-05-05", "2023-05-05"),   # storm record the next day
    ("S_FR", "HEATWAVE", "2023-07-11", "2023-07-12"),     # not a US station: ignored
], columns=["station_id", "event_type", "start_date", "end_date"])
events["start_date"] = pd.to_datetime(events["start_date"]).dt.date
events["end_date"] = pd.to_datetime(events["end_date"]).dt.date
events["duration_days"] = 1
events["intensity_value"] = 1.0
events["intensity_measure"] = "x"

storm = pd.DataFrame([
    # state, type, begin, end
    ("NEW YORK", "Excessive Heat", "11-JUL-23 12:00:00", "13-JUL-23 18:00:00"),   # 3 state-days
    ("TEXAS", "Heat", "01-AUG-23 10:00:00", "01-AUG-23 20:00:00"),                # 1 state-day, no detection
    ("NEW YORK", "Flash Flood", "06-MAY-23 01:00:00", "06-MAY-23 05:00:00"),      # 1 state-day
    ("CALIFORNIA", "Heat", "05-JUL-23 10:00:00", "05-JUL-23 20:00:00"),           # state not covered
    ("NEW YORK", "Hail", "15-JUN-23 10:00:00", "15-JUN-23 11:00:00"),             # other type: ignored
], columns=["STATE", "EVENT_TYPE", "BEGIN_DATE_TIME", "END_DATE_TIME"])
storm["YEAR"] = "2023"

geography = pd.DataFrame([
    ("S_NY", "New York", "United States", "North America"),
    ("S_TX", "Texas", "United States", "North America"),
    ("S_FR", "France", "France", "Europe"),
], columns=["station_id", "region_name", "country", "continent"])

with tempfile.TemporaryDirectory() as tmp:
    project = Path(tmp)
    (project / "src").mkdir()
    shutil.copy(ROOT / "src" / "validate_against_storm_events.py", project / "src")
    parquet = project / "data" / "parquet"
    export = project / "data" / "export"
    parquet.mkdir(parents=True)
    export.mkdir(parents=True)
    events.to_parquet(parquet / "extreme_events", partition_cols=["event_type"], index=False)
    storm.to_parquet(parquet / "storm_events", index=False)
    geography.to_csv(export / "station_geography.csv", index=False)

    result = subprocess.run(
        [sys.executable, str(project / "src" / "validate_against_storm_events.py")],
        capture_output=True, text=True, cwd=project)
    check("script runs", result.returncode == 0,
          "" if result.returncode == 0 else "\n" + result.stdout[-1200:] + result.stderr[-1200:])
    out = pd.read_csv(export / "storm_events_validation.csv").set_index("category")

print("Heatwave (hand-worked: 8 detected state-days, 4 storm state-days)")
h = out.loc["heatwave"]
check("France is ignored: 2 stations in 2 states", h.stations == 2 and h.states == 2)
check("detected state-days = 5 (NY) + 3 (TX) = 8", h.detected_state_days == 8)
check("precision = 5/8 (NY days 10-14 are within 1 day of the storm days 11-13)", abs(h.precision - 0.625) < 1e-9,
      f"(got {h.precision})")
check("storm state-days = 3 (NY) + 1 (TX) = 4; California is not covered", h.storm_state_days == 4)
check("recall = 3/4 (the Texas day has no detection)", abs(h.recall - 0.75) < 1e-9, f"(got {h.recall})")
check("base rate = 4 / (2 states x 365 days) = 0.0055", abs(h.base_rate - round(4 / 730, 4)) < 1e-9,
      f"(got {h.base_rate})")
check("lift = precision / base rate = 114.1", abs(h.lift - 114.1) < 0.05, f"(got {h.lift})")

print("Heavy rain (tolerance of one day)")
r = out.loc["heavy_rain"]
check("detection on 5 May matches the storm record on 6 May: precision 1 and recall 1",
      r.detected_state_days == 1 and r.precision == 1.0 and r.recall == 1.0 and r.storm_state_days == 1)

print("Categories with no detections")
d = out.loc["dry_spell"]
check("dry_spell has no detections: precision empty, no crash",
      d.detected_state_days == 0 and pd.isna(d.precision))
check("all four categories are reported", list(out.index) == ["heatwave", "cold_snap", "heavy_rain", "dry_spell"])

print()
if failures:
    print(f"FAILED: {len(failures)} check(s): {failures}")
    sys.exit(1)
print("All checks passed.")
