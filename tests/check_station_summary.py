"""
Check src/compute_station_summary.py on synthetic Table A data with known
answers. Plain pandas, no Spark.

Run from the repository root:  python tests/check_station_summary.py
"""

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location(
    "compute_station_summary", ROOT / "src" / "compute_station_summary.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

METRICS = [
    "hot_days", "heatwave_days", "coldsnap_days", "cold_days",
    "heavy_rain_days", "longest_dry_spell", "compound_days",
]

failures = []


def check(name, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {name} {detail}")
    if not condition:
        failures.append(name)


def station(station_id, years, invalid_years=(), **series):
    """Rows for one station. series: metric -> function(year) or constant."""
    rows = []
    for year in years:
        row = {
            "station_id": station_id, "lat": 10.0, "lon": 20.0,
            "region_name": None, "country": None, "continent": None,
            "year": year,
            "days_present_pct": 85.0 if year in invalid_years
            else (74.6 if year == 2026 else 100.0),
        }
        for metric in METRICS:
            value = series.get(metric, 3)
            row[metric] = value(year) if callable(value) else value
        rows.append(row)
    return rows


rng = np.random.default_rng(7)
noise = rng.normal(0, 1.5, 35) + 0.05 * np.arange(35) + 10   # weak trend, moderate p-value

rows = []
# 1. Planted linear trend: hot_days +0.5 per year = +5.0 per decade
rows += station("T1_LINEAR", range(1991, 2026),
                hot_days=lambda y: 10 + 0.5 * (y - 1991))
# 2. Long history plus a partial 2026. Only 1991-2025 may be used.
rows += station("T2_WINDOW", range(1950, 2027),
                hot_days=lambda y: 2 * (y - 1991) if 1991 <= y <= 2025 else 1000)
# 3. Only 29 valid years in the window
rows += station("T3_SHORT", range(1991, 2026),
                invalid_years={1992, 1996, 2000, 2004, 2008, 2012},
                hot_days=lambda y: y * 0.1)
# 4. Exactly 30 valid years with gaps. Slope must use the real years.
rows += station("T4_GAPS", range(1991, 2026),
                invalid_years={1995, 2000, 2005, 2010, 2015},
                hot_days=lambda y: 0.4 * y)
# 5. Nulls in valid years reduce years_used for that event type only
rows += station("T5_NULLS", range(1991, 2026),
                longest_dry_spell=lambda y: np.nan if y in (2000, 2001) else 50)
# 6. Constant series
rows += station("T6_CONSTANT", range(1991, 2026), compound_days=0)
# 7. Noisy series, checked against an independent calculation
rows += station("T7_NOISE", range(1991, 2026),
                hot_days=lambda y: float(noise[y - 1991]))
# 8. cold uses coldsnap_days, not cold_days (cold_days falls, coldsnap rises)
rows += station("T8_COLD", range(1991, 2026),
                coldsnap_days=lambda y: 0.1 * (y - 1991),
                cold_days=lambda y: 100 - (y - 1991))

table_a = pd.DataFrame(rows)
summary = module.build_summary(table_a)


def get(station_id, event_type):
    return summary[
        (summary.station_id == station_id) & (summary.event_type == event_type)
    ].iloc[0]


print("Structure")
check("48 rows (8 stations x 6 event types)", len(summary) == 48)
check("columns match the Table B spec",
      list(summary.columns) == module.OUTPUT_COLUMNS)
check("no problems from validate()", module.validate(summary, table_a) == [])
check("later-phase columns are empty",
      summary[module.LATER_PHASE_COLUMNS].isna().all().all())

print("Trend values")
r = get("T1_LINEAR", "hot_days")
check("T1 slope is +5.0 per decade", abs(r.trend_slope - 5.0) < 1e-9,
      f"(got {r.trend_slope})")
check("T1 is eligible with 35 years and a tiny p-value",
      r.trend_eligible and r.years_used == 35 and r.trend_p_value < 1e-6)
r = get("T1_LINEAR", "heatwave")
check("T1 constant series gives slope 0 and p = 1",
      r.trend_slope == 0 and r.trend_p_value == 1.0)

r = get("T2_WINDOW", "hot_days")
check("T2 uses only 1991-2025 (35 years, slope 20.0)",
      r.years_used == 35 and abs(r.trend_slope - 20.0) < 1e-9,
      f"(years_used {r.years_used}, slope {r.trend_slope})")

r = get("T4_GAPS", "hot_days")
check("T4 has exactly 30 valid years and is eligible",
      r.years_used == 30 and r.trend_eligible)
check("T4 slope uses the real years (4.0 per decade, not index-based)",
      abs(r.trend_slope - 4.0) < 1e-9, f"(got {r.trend_slope})")

r = get("T8_COLD", "cold")
check("cold uses coldsnap_days (+1.0 per decade)",
      abs(r.trend_slope - 1.0) < 1e-9, f"(got {r.trend_slope})")

r = get("T6_CONSTANT", "compound")
check("constant compound_days gives slope 0 and p = 1.0 (not NaN)",
      r.trend_slope == 0 and r.trend_p_value == 1.0)

print("Eligibility")
for event_type in module.EVENT_METRICS:
    r = get("T3_SHORT", event_type)
    check(f"T3 {event_type}: 29 valid years, not eligible, no slope or p",
          r.years_used == 29 and not r.trend_eligible
          and pd.isna(r.trend_slope) and pd.isna(r.trend_p_value))
check("T5 nulls: dry_spell years_used = 33, hot_days = 35",
      get("T5_NULLS", "dry_spell").years_used == 33
      and get("T5_NULLS", "hot_days").years_used == 35)

print("Independent check on a noisy series")
r = get("T7_NOISE", "hot_days")
years = np.arange(1991, 2026)
pairwise = [(noise[j] - noise[i]) / (years[j] - years[i])
            for i in range(35) for j in range(i + 1, 35)]
expected_slope = float(np.median(pairwise)) * 10
scipy_p = stats.kendalltau(years, noise).pvalue
check("slope equals the median of all pairwise slopes x 10",
      abs(r.trend_slope - round(expected_slope, 3)) < 1e-9,
      f"(got {r.trend_slope}, expected {expected_slope:.3f})")
check("p-value agrees with scipy's Kendall tau test within 0.03",
      abs(r.trend_p_value - scipy_p) < 0.03,
      f"(Mann-Kendall {r.trend_p_value}, scipy {scipy_p:.4f})")

print()
if failures:
    print(f"FAILED: {len(failures)} check(s): {failures}")
    sys.exit(1)
print("All checks passed.")
