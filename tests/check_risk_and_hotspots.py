"""
Check src/compute_risk_and_hotspots.py on synthetic stations with planted
patterns, against independent calculations.

Run from the repository root:  python tests/check_risk_and_hotspots.py
"""

import importlib.util
import shutil
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import rankdata

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
spec = importlib.util.spec_from_file_location(
    "compute_risk_and_hotspots", ROOT / "src" / "compute_risk_and_hotspots.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

failures = []


def check(name, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {name} {detail}")
    if not condition:
        failures.append(name)


LATS = [-35, -25, -15, -5, 5, 15, 25, 35]
LONS = [-70, -50, -30, -10, 10, 30, 50, 70]
CONSTANT_METRICS = dict(coldsnap_days=3.0, cold_days=3.0, heavy_rain_days=2.0,
                        longest_dry_spell=12.0, compound_days=0.0)


def make_table_a(grid=True, extra_short_stations=0, n_stations=None):
    """64 stations on a grid: a hot block, a cold block, random elsewhere."""
    rng = np.random.default_rng(11)
    rows = []
    cells = [(iy, ix) for iy in range(8) for ix in range(8)]
    if n_stations:
        cells = cells[:n_stations]
    for iy, ix in cells:
        sid = f"ST{iy}{ix}"
        hot = 4 <= iy <= 6 and 4 <= ix <= 6
        cold = 0 <= iy <= 2 and 0 <= ix <= 2
        base = 30 if hot else (5 if cold else 15 + rng.normal(0, 3))
        random_level = rng.uniform(5, 25)
        trend = 0.5 if sid == "ST55" else 0.0
        for year in range(1991, 2026):
            rows.append(dict(
                station_id=sid, lat=LATS[iy], lon=LONS[ix], region_name=None,
                country=None, continent=None, year=year, days_present_pct=99.0,
                hot_days=base + trend * (year - 1991) + rng.normal(0, 1.0),
                heatwave_days=random_level + rng.normal(0, 1.0),
                **CONSTANT_METRICS))
    for k in range(extra_short_stations):       # only 20 valid years: not trend eligible
        for year in range(2006, 2026):
            rows.append(dict(
                station_id=f"SHORT{k}", lat=0.0, lon=0.0, region_name=None,
                country=None, continent=None, year=year, days_present_pct=99.0,
                hot_days=50.0, heatwave_days=50.0, **CONSTANT_METRICS))
    return pd.DataFrame(rows)


def run_pipeline(table_a):
    """Run Table B then the risk script in a throw-away project folder."""
    tmp = tempfile.TemporaryDirectory()
    project = Path(tmp.name)
    (project / "src").mkdir()
    for script in ("compute_station_summary.py", "compute_risk_and_hotspots.py"):
        shutil.copy(ROOT / "src" / script, project / "src" / script)
    (project / "data" / "export").mkdir(parents=True)
    table_a.to_csv(project / "data" / "export" / "station_year_metrics.csv", index=False)
    results = []
    for script in ("compute_station_summary.py", "compute_risk_and_hotspots.py"):
        results.append(subprocess.run(
            [sys.executable, str(project / "src" / script)],
            capture_output=True, text=True, cwd=project))
    return tmp, project, results


print("Full network (64 eligible stations + 2 short ones)")
tmp, project, results = run_pipeline(make_table_a(extra_short_stations=2))
check("both scripts run", all(r.returncode == 0 for r in results),
      "" if all(r.returncode == 0 for r in results) else "\n" + results[-1].stdout[-1500:] + results[-1].stderr[-1500:])
b = pd.read_csv(project / "data" / "export" / "station_summary.csv")
c = pd.read_csv(project / "data" / "export" / "global_stats.csv")
tmp.cleanup()

check("Table B has 66 stations x 6 rows", len(b) == 66 * 6)
check("Table B columns keep the spec order",
      list(b.columns) == ["station_id", "lat", "lon", "region_name", "country", "continent",
                          "event_type", "trend_slope", "trend_p_value", "years_used",
                          "trend_eligible", "risk_score", "risk_rank", "gi_z_score",
                          "gi_p_value", "hotspot_class"])
check("Table C columns and six event types",
      list(c.columns) == ["event_type", "morans_i", "morans_p_value", "n_stations"] and len(c) == 6)
check("short stations are not counted (n_stations = 64)", (c["n_stations"] == 64).all())

short = b[b["station_id"].str.startswith("SHORT")]
check("short stations stay empty",
      short[module.RISK_COLUMNS].isna().all().all())

h = b[b["event_type"] == "hot_days"].set_index("station_id")
h = h[~h.index.str.startswith("SHORT")]

# independent risk calculation
table_a = make_table_a()
level = table_a.groupby("station_id")["hot_days"].mean().loc[h.index].to_numpy()
slope = h["trend_slope"].to_numpy(dtype=float)
pr = lambda v: (rankdata(v, method="average") - 1) / (len(v) - 1)
expected = np.round(100 * (0.6 * pr(level) + 0.4 * pr(slope)), 1)
check("risk_score equals 100 x (0.6 level rank + 0.4 trend rank)",
      np.allclose(h["risk_score"].to_numpy(dtype=float), expected, atol=1e-9))
check("risk_rank 1 is the planted high-level, high-trend station",
      h["risk_rank"].idxmin() == "ST55" and h.loc["ST55", "risk_rank"] == 1)
check("scores run from 0 to 100", h["risk_score"].min() >= 0 and h["risk_score"].max() == 100)

hot_block = [f"ST{iy}{ix}" for iy in (4, 5, 6) for ix in (4, 5, 6)]
cold_block = [f"ST{iy}{ix}" for iy in (0, 1, 2) for ix in (0, 1, 2)]
check("every hot-block station has a positive Gi* z-score", (h.loc[hot_block, "gi_z_score"] > 0).all())
check("every cold-block station has a negative Gi* z-score", (h.loc[cold_block, "gi_z_score"] < 0).all())
check("at least 7 of 9 hot-block stations are hot spots",
      h.loc[hot_block, "hotspot_class"].str.startswith("Hot spot").sum() >= 7)
check("at least 7 of 9 cold-block stations are cold spots",
      h.loc[cold_block, "hotspot_class"].str.startswith("Cold spot").sum() >= 7)
check("centres are the strongest: ST55 hot spot 99%, ST11 cold spot 99%",
      h.loc["ST55", "hotspot_class"] == "Hot spot 99%" and h.loc["ST11", "hotspot_class"] == "Cold spot 99%")
check("no cold spot in the hot block and no hot spot in the cold block",
      not h.loc[hot_block, "hotspot_class"].str.startswith("Cold").any()
      and not h.loc[cold_block, "hotspot_class"].str.startswith("Hot").any())

moran = c.set_index("event_type")
check("clustered measure: Moran's I > 0.4 and p < 0.001",
      moran.loc["hot_days", "morans_i"] > 0.4 and moran.loc["hot_days", "morans_p_value"] < 0.001,
      f"(I {moran.loc['hot_days', 'morans_i']}, p {moran.loc['hot_days', 'morans_p_value']})")
check("random measure: Moran's I is small (|I| < 0.25)", abs(moran.loc["heatwave", "morans_i"]) < 0.25,
      f"(I {moran.loc['heatwave', 'morans_i']})")

constant = b[b["event_type"].isin(["cold", "heavy_rain", "dry_spell", "compound"])]
check("constant measures: risk and hotspot columns stay empty",
      constant[module.RISK_COLUMNS].isna().all().all())
check("constant measures: Moran's I is empty",
      moran.loc[["cold", "heavy_rain", "dry_spell", "compound"], "morans_i"].isna().all())

print("Small network (6 eligible stations)")
tmp, project, results = run_pipeline(make_table_a(n_stations=6))
check("both scripts run", all(r.returncode == 0 for r in results))
b6 = pd.read_csv(project / "data" / "export" / "station_summary.csv")
c6 = pd.read_csv(project / "data" / "export" / "global_stats.csv").set_index("event_type")
tmp.cleanup()
h6 = b6[b6["event_type"] == "hot_days"]
check("risk score is still calculated", h6["risk_score"].notna().all())
check("hotspot columns and Moran's I are empty (fewer than 10 stations)",
      h6[["gi_z_score", "gi_p_value", "hotspot_class"]].isna().all().all()
      and c6["morans_i"].isna().all() and (c6["n_stations"] == 6).all())

print("Building blocks")
check("great-circle distance, equator quarter circle = 10007.5 km",
      abs(module.haversine_matrix([0, 0], [0, 90])[0, 1] - 10007.54) < 0.1)
check("classification at the p = 0.10, 0.05 and 0.01 boundaries",
      module.classify(1.7, 0.089) == "Hot spot 90%" and module.classify(2.0, 0.0455) == "Hot spot 95%"
      and module.classify(-2.7, 0.007) == "Cold spot 99%" and module.classify(1.0, 0.3173) == "Not significant"
      and module.classify(float("nan"), float("nan")) is None)
check("one station gets a percentile rank of 0.5", module.percentile_rank([7.0])[0] == 0.5)

two_a = [(10.0, float(x)) for x in range(0, 6)]
two_b = [(10.0, float(x)) for x in range(150, 156)]
lat12 = np.array([a for a, b in two_a + two_b]); lon12 = np.array([b for a, b in two_a + two_b])
y12 = np.random.default_rng(3).poisson(8, 12).astype(float)
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    module.hotspot_statistics(y12, lat12, lon12)
check("two far-apart groups of stations raise no repeated warning",
      not any("connected" in str(w.message) for w in caught))
check("the number of separate groups is reported (2)", module.connected_groups(lat12, lon12) == 2)

print("Calibration on random data (200 runs, Poisson counts, 64 stations)")
la, lo = np.meshgrid(np.array(LATS), np.array(LONS), indexing="ij")
moran_hits, gi_share = [], []
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    for seed in range(200):
        y = np.random.default_rng(1000 + seed).poisson(8, 64).astype(float)
        _, p, _, _, p_moran = module.hotspot_statistics(y, la.ravel(), lo.ravel())
        moran_hits.append(p_moran < 0.05)
        gi_share.append(np.mean(p < 0.05))
check("Moran's I flags about 5% of random data sets at the 5% level (2%-9%)",
      0.02 <= np.mean(moran_hits) <= 0.09, f"(got {np.mean(moran_hits):.3f})")
check("Gi* flags about 5% of stations at 95% on random data (2%-8%)",
      0.02 <= np.mean(gi_share) <= 0.08, f"(got {np.mean(gi_share):.3f})")

print()
if failures:
    print(f"FAILED: {len(failures)} check(s): {failures}")
    sys.exit(1)
print("All checks passed.")
