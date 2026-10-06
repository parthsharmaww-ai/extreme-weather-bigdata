"""
Build Table B (station_summary): one trend row per station and event type.

Reads data/export/station_year_metrics.csv (Table A) and writes
data/export/station_summary.csv.

Rules from docs/event_definitions.md (v2):
- Trends use only the common window 1991-2025 for every station, even where
  Table A holds a longer history.
- Only valid years count: days_present_pct >= 90 (all of TMAX, TMIN and PRCP
  present on the same day). Incomplete years, including 2026, are excluded.
- trend_eligible = at least 30 valid years in that window.
  years_used is the number of valid years actually used for that station
  and event type. Ineligible rows have an empty slope and p-value.
- trend_slope is the Theil-Sen slope in event days per decade, calculated on
  the real years (gaps are allowed). trend_p_value is the Mann-Kendall
  p-value.
- The measure behind each event type is the one in the storyboard (Section 2).
  For dry_spell it is longest_dry_spell.

risk_score, risk_rank, gi_z_score, gi_p_value and hotspot_class belong to later
phases (the risk formula is not decided yet). They are written empty, never
with made-up values.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pymannkendall as mk
from scipy import stats


PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FILE = PROJECT_ROOT / "data" / "export" / "station_year_metrics.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "export" / "station_summary.csv"

TREND_FIRST_YEAR = 1991
TREND_LAST_YEAR = 2025
VALID_YEAR_PCT = 90.0
MIN_TREND_YEARS = 30

# event_type in Table B -> Table A column the trend is calculated on
EVENT_METRICS = {
    "hot_days": "hot_days",
    "heatwave": "heatwave_days",
    "cold": "coldsnap_days",
    "heavy_rain": "heavy_rain_days",
    "dry_spell": "longest_dry_spell",
    "compound": "compound_days",
}

STATION_COLUMNS = ["lat", "lon", "region_name", "country", "continent"]

OUTPUT_COLUMNS = [
    "station_id", "lat", "lon", "region_name", "country", "continent",
    "event_type", "trend_slope", "trend_p_value", "years_used",
    "trend_eligible", "risk_score", "risk_rank", "gi_z_score",
    "gi_p_value", "hotspot_class",
]

# Filled in later phases.
LATER_PHASE_COLUMNS = [
    "risk_score", "risk_rank", "gi_z_score", "gi_p_value", "hotspot_class",
]


def trend(years, values):
    """Return (Theil-Sen slope per decade, Mann-Kendall p-value)."""
    x = np.asarray(years, dtype=float)
    y = np.asarray(values, dtype=float)
    slope_per_year = stats.theilslopes(y, x)[0]
    p_value = mk.original_test(y).p
    return float(slope_per_year) * 10.0, float(p_value)


def build_summary(table_a):
    """Build Table B from Table A."""
    required = (
        ["station_id", "year", "days_present_pct"]
        + STATION_COLUMNS
        + list(EVENT_METRICS.values())
    )
    missing = [c for c in required if c not in table_a.columns]
    if missing:
        raise ValueError(f"Table A is missing columns: {missing}")

    in_window = table_a[
        table_a["year"].between(TREND_FIRST_YEAR, TREND_LAST_YEAR)
        & (table_a["days_present_pct"] >= VALID_YEAR_PCT)
    ]

    stations = (
        table_a.sort_values("year")
        .drop_duplicates("station_id", keep="last")
        .set_index("station_id")[STATION_COLUMNS]
        .sort_index()
    )

    rows = []
    for station_id, meta in stations.iterrows():
        station_years = in_window[in_window["station_id"] == station_id]
        station_years = station_years.sort_values("year")

        for event_type, metric in EVENT_METRICS.items():
            used = station_years.dropna(subset=[metric])
            years_used = len(used)
            eligible = years_used >= MIN_TREND_YEARS

            slope = p_value = None
            if eligible:
                slope, p_value = trend(used["year"], used[metric])
                slope, p_value = round(slope, 3), round(p_value, 4)

            row = {
                "station_id": station_id,
                **meta.to_dict(),
                "event_type": event_type,
                "trend_slope": slope,
                "trend_p_value": p_value,
                "years_used": years_used,
                "trend_eligible": eligible,
            }
            for column in LATER_PHASE_COLUMNS:
                row[column] = None
            rows.append(row)

    summary = pd.DataFrame(rows)[OUTPUT_COLUMNS]
    return summary.sort_values(["station_id", "event_type"]).reset_index(
        drop=True
    )


def validate(summary, table_a):
    """Basic output checks. Returns a list of problems."""
    problems = []

    if summary.duplicated(["station_id", "event_type"]).any():
        problems.append("duplicate station-event_type rows")

    expected_rows = table_a["station_id"].nunique() * len(EVENT_METRICS)
    if len(summary) != expected_rows:
        problems.append(f"{len(summary)} rows, expected {expected_rows}")

    eligible = summary["trend_eligible"]
    if summary.loc[eligible, ["trend_slope", "trend_p_value"]].isna().any().any():
        problems.append("eligible rows without a slope or p-value")
    if summary.loc[~eligible, ["trend_slope", "trend_p_value"]].notna().any().any():
        problems.append("ineligible rows with a slope or p-value")
    if (eligible != (summary["years_used"] >= MIN_TREND_YEARS)).any():
        problems.append("trend_eligible disagrees with years_used >= 30")
    window_years = TREND_LAST_YEAR - TREND_FIRST_YEAR + 1
    if (summary["years_used"] > window_years).any():
        problems.append(f"years_used above {window_years}")

    return problems


def main():
    if not INPUT_FILE.exists():
        print(f"ERROR: Table A not found: {INPUT_FILE}")
        print("Run src/export_station_year_metrics.py first.")
        return False

    table_a = pd.read_csv(INPUT_FILE)
    if table_a.empty:
        print("ERROR: Table A is empty.")
        return False

    summary = build_summary(table_a)

    problems = validate(summary, table_a)
    if problems:
        print("ERROR: validation failed:")
        for problem in problems:
            print(f"  - {problem}")
        return False

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_FILE, index=False)

    print("=" * 65)
    print("STATION SUMMARY (Table B)")
    print("=" * 65)
    print(f"Trend window: {TREND_FIRST_YEAR}-{TREND_LAST_YEAR}, valid years "
          f"only (days_present_pct >= {VALID_YEAR_PCT:.0f}), "
          f"{MIN_TREND_YEARS} needed")
    print(f"Stations: {summary['station_id'].nunique():,}")
    print(f"Rows: {len(summary):,}")
    print("\nTrend-eligible rows by event type:")
    print(summary.groupby("event_type")["trend_eligible"].agg(
        eligible="sum", total="count").to_string())
    print(f"\nOutput: {OUTPUT_FILE}")
    return True


if __name__ == "__main__":
    if not main():
        sys.exit(1)
