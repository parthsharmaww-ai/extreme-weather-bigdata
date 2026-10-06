"""
Build the two Tableau-ready tables from the pipeline exports.

Reads (data/export):
  station_year_metrics.csv (Table A), station_summary.csv (Table B),
  global_stats.csv (Table C), station_events.csv (Table E)

Writes (data/export/tableau):
  dashboard_data.csv    one row per station, complete year and event type.
                        `value` is the yearly measure of that event type, so the
                        dashboard's event-type selector is a plain filter.
                        The trend, risk, hotspot and Moran's I columns of Tables
                        B and C are attached to every row.
  dashboard_season.csv  events grouped by station, start year, meteorological
                        season (DJF, MAM, JJA, SON) and event type, for the
                        season view. Events belong to the season of their start
                        date, and to the year of their start date.

Both tables keep complete years only (days_present_pct >= 90), as the
storyboard requires. Nothing is recalculated: values come from the exports.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from compute_station_summary import EVENT_METRICS, VALID_YEAR_PCT


PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = PROJECT_ROOT / "data" / "export"
OUTPUT_DIR = EXPORT_DIR / "tableau"

EVENT_LABELS = {
    "hot_days": "Warm days",
    "heatwave": "Warm spells",
    "cold": "Cold snaps",
    "heavy_rain": "Heavy rain",
    "dry_spell": "Dry spells",
    "compound": "Compound",
}
STATION_COLUMNS = [
    "station_id", "station_name", "lat", "lon", "elevation_m",
    "region_name", "country", "continent",
]
TOOLTIP_COLUMNS = [
    "heatwave_max_intensity", "max_daily_prcp_mm", "prcp_total_mm",
    "tmax_anomaly_mean",
]
TREND_COLUMNS = [
    "trend_slope", "trend_p_value", "years_used", "trend_eligible",
    "risk_score", "risk_rank", "gi_z_score", "gi_p_value", "hotspot_class",
]
GLOBAL_COLUMNS = ["morans_i", "morans_p_value", "n_stations"]
SEASONS = {12: "DJF", 1: "DJF", 2: "DJF", 3: "MAM", 4: "MAM", 5: "MAM",
           6: "JJA", 7: "JJA", 8: "JJA", 9: "SON", 10: "SON", 11: "SON"}


def trend_status(eligible, slope, p_value):
    if not eligible:
        return "Not enough years"
    if p_value < 0.05 and slope > 0:
        return "Increasing (p<0.05)"
    if p_value < 0.05 and slope < 0:
        return "Decreasing (p<0.05)"
    return "Not significant"


def build_long_table(table_a, table_b, table_c):
    complete = table_a[table_a["days_present_pct"] >= VALID_YEAR_PCT]

    parts = []
    for event_type, metric in EVENT_METRICS.items():
        part = complete[
            STATION_COLUMNS + ["year", "days_present_pct"] + TOOLTIP_COLUMNS
        ].copy()
        part["event_type"] = event_type
        part["event_label"] = EVENT_LABELS[event_type]
        part["value"] = complete[metric].to_numpy()
        parts.append(part)
    long = pd.concat(parts, ignore_index=True)

    long = long.merge(
        table_b[["station_id", "event_type"] + TREND_COLUMNS],
        on=["station_id", "event_type"], how="left",
    )
    long = long.merge(
        table_c[["event_type"] + GLOBAL_COLUMNS], on="event_type", how="left"
    )

    eligible = long["trend_eligible"].fillna(False).astype(bool)
    long["trend_eligible"] = eligible
    long["trend_status"] = [
        trend_status(e, s, p)
        for e, s, p in zip(eligible, long["trend_slope"], long["trend_p_value"])
    ]

    columns = (
        STATION_COLUMNS + ["year", "days_present_pct", "event_type", "event_label",
                           "value"] + TOOLTIP_COLUMNS
        + TREND_COLUMNS[:4] + ["trend_status"] + TREND_COLUMNS[4:] + GLOBAL_COLUMNS
    )
    return long[columns].sort_values(
        ["event_type", "station_id", "year"]
    ).reset_index(drop=True)


def build_season_table(table_a, table_e):
    complete_years = table_a.loc[
        table_a["days_present_pct"] >= VALID_YEAR_PCT, ["station_id", "year"]
    ]
    events = table_e.copy()
    events["season"] = pd.to_datetime(events["start_date"]).dt.month.map(SEASONS)
    events = events.merge(complete_years, on=["station_id", "year"], how="inner")

    grouped = (
        events.groupby(["station_id", "year", "season", "event_type"])
        .agg(events=("duration_days", "size"),
             event_days=("duration_days", "sum"),
             max_intensity=("intensity_value", "max"))
        .reset_index()
    )
    stations = table_a.drop_duplicates("station_id")[STATION_COLUMNS]
    grouped = grouped.merge(stations, on="station_id", how="left")
    grouped["event_label"] = grouped["event_type"].map(EVENT_LABELS)
    columns = STATION_COLUMNS + ["year", "season", "event_type", "event_label",
                                 "events", "event_days", "max_intensity"]
    return grouped[columns].sort_values(
        ["event_type", "station_id", "year", "season"]
    ).reset_index(drop=True)


def reconcile(table_a, season):
    """Event days in the season table must add up to the yearly counts in Table A."""
    pairs = {
        "heatwave": "heatwave_days", "cold": "coldsnap_days",
        "compound": "compound_days", "heavy_rain": "heavy_rain_days",
        "dry_spell": "dry_spell_days",
    }
    complete = table_a[table_a["days_present_pct"] >= VALID_YEAR_PCT]
    problems = []
    for event_type, column in pairs.items():
        mine = (season[season["event_type"] == event_type]
                .groupby(["station_id", "year"])["event_days"].sum())
        theirs = complete.set_index(["station_id", "year"])[column]
        theirs = theirs[theirs > 0]
        joined = pd.concat([mine.rename("season"), theirs.rename("yearly")], axis=1).fillna(0)
        bad = int((joined["season"] != joined["yearly"]).sum())
        print(f"  {event_type:10s} {len(joined):6,d} station-years compared, differences: {bad}")
        if bad:
            problems.append(event_type)
    return problems


def main():
    needed = ["station_year_metrics.csv", "station_summary.csv",
              "global_stats.csv", "station_events.csv"]
    for name in needed:
        if not (EXPORT_DIR / name).exists():
            print(f"ERROR: {EXPORT_DIR / name} not found. Run the export steps first.")
            return False

    table_a = pd.read_csv(EXPORT_DIR / "station_year_metrics.csv")
    table_b = pd.read_csv(EXPORT_DIR / "station_summary.csv")
    table_c = pd.read_csv(EXPORT_DIR / "global_stats.csv")
    table_e = pd.read_csv(EXPORT_DIR / "station_events.csv")

    long = build_long_table(table_a, table_b, table_c)
    season = build_season_table(table_a, table_e)

    print("=" * 65)
    print("DASHBOARD DATA")
    print("=" * 65)
    print("Season table against yearly counts in Table A:")
    problems = reconcile(table_a, season)
    if problems:
        print(f"ERROR: event days do not add up for {problems}.")
        return False

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    long.to_csv(OUTPUT_DIR / "dashboard_data.csv", index=False)
    season.to_csv(OUTPUT_DIR / "dashboard_season.csv", index=False)

    print(f"\ndashboard_data.csv:   {len(long):,} rows, "
          f"{long['station_id'].nunique()} stations, "
          f"years {long['year'].min()}-{long['year'].max()}")
    print(f"dashboard_season.csv: {len(season):,} rows")
    print(f"Written to {OUTPUT_DIR}")
    return True


if __name__ == "__main__":
    if not main():
        sys.exit(1)
