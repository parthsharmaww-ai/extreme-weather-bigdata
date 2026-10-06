"""
Check src/build_dashboard_data.py on a small scenario that can be worked out by hand.

Run from the repository root:  python tests/check_dashboard_data.py
"""

import importlib.util
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
spec = importlib.util.spec_from_file_location(
    "build_dashboard_data", ROOT / "src" / "build_dashboard_data.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

failures = []


def check(name, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {name} {detail}")
    if not condition:
        failures.append(name)


# ---- events (Table E): two stations, years 1995-1997; 1997 of S2 is an incomplete year
events = pd.DataFrame([
    # station, type, start, duration, intensity
    ("S1", "heatwave", "1995-07-10", 4, 3.5), ("S1", "heatwave", "1996-12-30", 5, 2.0),
    ("S1", "dry_spell", "1995-01-05", 12, 12.0), ("S1", "dry_spell", "1996-08-01", 20, 20.0),
    ("S1", "heavy_rain", "1996-04-02", 1, 18.0), ("S1", "heavy_rain", "1996-04-03", 1, 22.0),
    ("S1", "cold", "1995-01-20", 3, 4.0), ("S1", "compound", "1996-12-30", 3, 3.0),
    ("S2", "heatwave", "1996-01-10", 3, 1.5), ("S2", "dry_spell", "1996-10-01", 15, 15.0),
    ("S2", "heatwave", "1997-06-01", 6, 5.0),        # falls in an incomplete year: must be dropped
], columns=["station_id", "event_type", "start_date", "duration_days", "intensity_value"])
events["end_date"] = events["start_date"]
events["intensity_measure"] = "x"
events["year"] = pd.to_datetime(events["start_date"]).dt.year

# ---- yearly table (Table A): counts derived from the events so the two agree
rows = []
for sid, name in (("S1", "Alpha"), ("S2", "Beta")):
    for year in (1995, 1996, 1997):
        pct = 80.0 if (sid == "S2" and year == 1997) else 99.0
        e = events[(events.station_id == sid) & (events.year == year)]
        days = lambda t: int(e.loc[e.event_type == t, "duration_days"].sum())
        rows.append(dict(
            station_id=sid, station_name=name, lat=10.0, lon=20.0, elevation_m=5.0,
            region_name="R", country="C", continent="K", year=year, days_present_pct=pct,
            hot_days=year - 1990, heatwave_count=0, heatwave_days=days("heatwave"),
            heatwave_max_intensity=1.0, cold_days=0, coldsnap_count=0, coldsnap_days=days("cold"),
            heavy_rain_days=days("heavy_rain"), max_daily_prcp_mm=30.0, prcp_total_mm=500.0,
            dry_spell_count=0, dry_spell_days=days("dry_spell"),
            longest_dry_spell=int(e.loc[e.event_type == "dry_spell", "duration_days"].max()) if len(e[e.event_type == "dry_spell"]) else 0,
            compound_event_count=0, compound_days=days("compound"), tmax_anomaly_mean=0.1 * (year - 1995)))
table_a = pd.DataFrame(rows)

types = ["hot_days", "heatwave", "cold", "heavy_rain", "dry_spell", "compound"]
table_b = pd.DataFrame(
    [(s, t, 1.0, 0.01, 35, True, 50.0, 1, 0.5, 0.01, "Hot spot 95%") for s in ("S1", "S2") for t in types],
    columns=["station_id", "event_type", "trend_slope", "trend_p_value", "years_used", "trend_eligible",
             "risk_score", "risk_rank", "gi_z_score", "gi_p_value", "hotspot_class"])
def set_b(station, event, **values):
    for key, value in values.items():
        table_b.loc[(table_b.station_id == station) & (table_b.event_type == event), key] = value
set_b("S1", "cold", trend_slope=-2.0, trend_p_value=0.001)                    # decreasing
set_b("S1", "compound", trend_p_value=0.40)                                   # not significant
set_b("S2", "dry_spell", trend_eligible=False, trend_slope=None, trend_p_value=None, years_used=12)
table_c = pd.DataFrame({"event_type": types, "morans_i": [0.7, 0.45, 0.48, 0.58, 0.54, 0.47],
                        "morans_p_value": [0.0] * 6, "n_stations": [2] * 6})

print("Long table")
long = module.build_long_table(table_a, table_b, table_c)
check("6 event types x 5 complete station-years = 30 rows (the incomplete year is dropped)", len(long) == 30)
check("no incomplete year is in the table", (long["days_present_pct"] >= 90).all())
row = lambda s, t, y: long[(long.station_id == s) & (long.event_type == t) & (long.year == y)].iloc[0]
check("Warm days uses hot_days", row("S1", "hot_days", 1996).value == 6)
check("Warm spells uses heatwave_days (5 days starting 30 Dec 1996)", row("S1", "heatwave", 1996).value == 5)
check("Cold snaps uses coldsnap_days", row("S1", "cold", 1995).value == 3)
check("Heavy rain uses heavy_rain_days (2 wet days)", row("S1", "heavy_rain", 1996).value == 2)
check("Dry spells uses longest_dry_spell (20, not the 12 of the other year)", row("S1", "dry_spell", 1996).value == 20)
check("Compound uses compound_days", row("S1", "compound", 1996).value == 3)
check("event_label is the dashboard name", row("S1", "dry_spell", 1996).event_label == "Dry spells")
status = lambda s, t: long[(long.station_id == s) & (long.event_type == t)].trend_status.iloc[0]
check("trend_status: increasing", status("S1", "hot_days") == "Increasing (p<0.05)")
check("trend_status: decreasing", status("S1", "cold") == "Decreasing (p<0.05)")
check("trend_status: not significant", status("S1", "compound") == "Not significant")
check("trend_status: not enough years", status("S2", "dry_spell") == "Not enough years")
check("Moran's I of each event type is attached", row("S1", "cold", 1995).morans_i == 0.48 and row("S2", "hot_days", 1996).morans_i == 0.7)
check("hotspot class and risk score come from Table B", row("S1", "heatwave", 1995).hotspot_class == "Hot spot 95%" and row("S1", "heatwave", 1995).risk_score == 50.0)

print("Season table")
season = module.build_season_table(table_a, events)
get = lambda s, y, season_, t: season[(season.station_id == s) & (season.year == y) & (season.season == season_) & (season.event_type == t)]
check("a 30 Dec 1996 warm spell is DJF and belongs to year 1996", len(get("S1", 1996, "DJF", "heatwave")) == 1 and get("S1", 1996, "DJF", "heatwave").event_days.iloc[0] == 5)
check("10 Jan 1996 is DJF, 2 Apr is MAM, 1 Aug is JJA, 1 Oct is SON",
      len(get("S2", 1996, "DJF", "heatwave")) == 1 and len(get("S1", 1996, "MAM", "heavy_rain")) == 1
      and len(get("S1", 1996, "JJA", "dry_spell")) == 1 and len(get("S2", 1996, "SON", "dry_spell")) == 1)
check("two heavy-rain days in one season are counted as 2 events and 2 days",
      get("S1", 1996, "MAM", "heavy_rain").events.iloc[0] == 2 and get("S1", 1996, "MAM", "heavy_rain").event_days.iloc[0] == 2)
check("events in an incomplete year are dropped", (season[season.station_id == "S2"].year != 1997).all())
check("the season table adds up to the yearly counts", module.reconcile(table_a, season) == [])

tampered = events.copy()
tampered.loc[0, "duration_days"] = 9
check("an event table that disagrees with the yearly table is detected",
      module.reconcile(table_a, module.build_season_table(table_a, tampered)) == ["heatwave"])

print("Script run")
with tempfile.TemporaryDirectory() as tmp:
    project = Path(tmp)
    (project / "src").mkdir()
    for name in ("build_dashboard_data.py", "compute_station_summary.py"):
        shutil.copy(ROOT / "src" / name, project / "src" / name)
    export = project / "data" / "export"
    export.mkdir(parents=True)
    for name, frame in (("station_year_metrics", table_a), ("station_summary", table_b),
                        ("global_stats", table_c), ("station_events", events)):
        frame.to_csv(export / f"{name}.csv", index=False)

    def run():
        return subprocess.run([sys.executable, str(project / "src" / "build_dashboard_data.py")],
                              capture_output=True, text=True, cwd=project)

    result = run()
    out = export / "tableau"
    check("script runs and writes both files", result.returncode == 0 and (out / "dashboard_data.csv").exists()
          and (out / "dashboard_season.csv").exists(), "" if result.returncode == 0 else result.stdout[-800:] + result.stderr[-800:])
    check("the file has 30 rows", result.returncode == 0 and len(pd.read_csv(out / "dashboard_data.csv")) == 30)

    shutil.rmtree(out)
    tampered.to_csv(export / "station_events.csv", index=False)
    result = run()
    check("it refuses to write files when the tables disagree",
          result.returncode == 1 and not (out / "dashboard_data.csv").exists())

print()
if failures:
    print(f"FAILED: {len(failures)} check(s): {failures}")
    sys.exit(1)
print("All checks passed.")
