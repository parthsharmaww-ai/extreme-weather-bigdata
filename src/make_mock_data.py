"""Generate MOCK dashboard data for the Tableau prototype (event definitions v2).

THIS IS NOT REAL CLIMATE DATA. Values are random placeholders shaped like the
pipeline exports so the dashboard can be wired up before the real tables exist.
Every station_id starts with "MOCK".

Outputs (tableau/):
  mock_station_year_metrics.csv  Table A, same columns as
                                 src/export_station_year_metrics.py after the
                                 v2 rename (extreme_rain_days -> heavy_rain_days)
  mock_station_summary.csv       Table B, one row per station per event type
  mock_global_stats.csv          Table C, one row per event type

Rules mirrored from docs/event_definitions.md (v2):
  - valid year: days_present_pct >= 90 (2026 is partial, ~74.6%)
  - trends: Theil-Sen slope per decade + Mann-Kendall p-value on valid years
    1991-2025, only when >= 30 valid years (trend_eligible)
  - dry_spell trend uses longest_dry_spell
  - compound_days <= heatwave_days and <= dry_spell_days (overlap, not stacked)

Run:  python src/make_mock_data.py
"""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

SEED = 20261005
FIRST_YEAR, LAST_YEAR = 1991, 2026
PARTIAL_YEAR_PCT = 74.6  # 2026 data runs to 29 Sep
VALID_YEAR_PCT = 90.0
MIN_TREND_YEARS = 30

OUT = Path(__file__).resolve().parents[1] / "tableau"

# id, name, lat, lon, elev, region, country, continent,
# base hot days, heavy-rain rate, annual prcp, typical longest dry spell,
# first year of record, share of patchy years
STATIONS = [
    ("MOCK0001", "Delhi Safdarjung", 28.58, 77.21, 216, "North India", "India", "Asia", 20, 4.0, 780, 75, 1991, 0.0),
    ("MOCK0002", "Jaipur Sanganer", 26.82, 75.80, 390, "Rajasthan", "India", "Asia", 20, 3.0, 600, 95, 1991, 0.0),
    ("MOCK0003", "Kochi", 9.97, 76.28, 3, "Kerala", "India", "Asia", 18, 8.0, 3000, 30, 1991, 0.0),
    ("MOCK0004", "Niamey", 13.48, 2.17, 223, "Sahel", "Niger", "Africa", 19, 2.5, 550, 190, 1991, 0.05),
    ("MOCK0005", "Cape Town", -33.97, 18.60, 42, "Western Cape", "South Africa", "Africa", 18, 3.5, 520, 45, 1991, 0.0),
    ("MOCK0006", "Austin", 30.19, -97.67, 150, "Texas", "USA", "North America", 18, 4.5, 850, 40, 1991, 0.0),
    ("MOCK0007", "Los Angeles", 33.94, -118.41, 30, "California", "USA", "North America", 18, 1.35, 320, 145, 1991, 0.0),
    ("MOCK0008", "Manaus", -3.12, -60.02, 67, "Amazonas", "Brazil", "South America", 18, 7.0, 2300, 22, 1991, 0.0),
    ("MOCK0009", "Seville", 37.42, -5.90, 34, "Andalusia", "Spain", "Europe", 19, 3.0, 540, 85, 1991, 0.0),
    ("MOCK0010", "Sydney", -33.95, 151.18, 6, "New South Wales", "Australia", "Oceania", 18, 5.5, 1200, 25, 1991, 0.0),
    ("MOCK0011", "New York Central Park", 40.78, -73.97, 40, "New York", "USA", "North America", 18, 5.0, 1250, 16, 1991, 0.0),
    # Weak records, to show trend_eligible = False on the dashboard
    ("MOCK0012", "Nairobi", -1.32, 36.93, 1624, "Nairobi", "Kenya", "Africa", 18, 4.0, 870, 55, 2001, 0.0),
    ("MOCK0013", "Ulaanbaatar", 47.92, 106.92, 1300, "Ulaanbaatar", "Mongolia", "Asia", 18, 2.0, 260, 60, 1991, 0.25),
]

TABLE_A_COLUMNS = [
    "station_id", "station_name", "lat", "lon", "elevation_m",
    "region_name", "country", "continent", "year", "days_present_pct",
    "hot_days", "heatwave_count", "heatwave_days", "heatwave_max_intensity",
    "cold_days", "coldsnap_count", "coldsnap_days", "heavy_rain_days",
    "max_daily_prcp_mm", "prcp_total_mm", "dry_spell_count",
    "dry_spell_days", "longest_dry_spell", "compound_event_count",
    "compound_days", "tmax_anomaly_mean",
]

# event_type in Table B -> Table A column the trend is calculated on
TREND_METRIC = {
    "hot_days": "hot_days",
    "heatwave": "heatwave_days",
    "cold": "cold_days",
    "heavy_rain": "heavy_rain_days",
    "dry_spell": "longest_dry_spell",
    "compound": "compound_days",
}


def split_into_runs(rng, total_days, n_runs, min_len):
    """Return run lengths (each >= min_len) summing to total_days."""
    if n_runs == 0:
        return []
    extra = total_days - n_runs * min_len
    cuts = rng.multinomial(extra, np.ones(n_runs) / n_runs)
    return [min_len + int(c) for c in cuts]


def make_station_years(rng):
    rows = []
    for (sid, name, lat, lon, elev, region, country, continent,
         hot_base, rain_rate, prcp_mean, dry_base, first_year, patchy) in STATIONS:
        for year in range(first_year, LAST_YEAR + 1):
            t = (year - 2005.5) / 10.0  # decades from mid-period
            if year == LAST_YEAR:
                pct = PARTIAL_YEAR_PCT
            elif rng.random() < patchy:
                pct = round(rng.uniform(60, 89.5), 1)
            else:
                pct = round(min(100.0, rng.uniform(95, 100.4)), 1)
            frac = pct / 100.0

            # Warm days: ~5% of days in the baseline, rising with warming
            hot_days = int(rng.poisson(max(1.0, (hot_base + 6 * t) * frac)))
            hw_days = int(min(hot_days, rng.binomial(hot_days, 0.45)))
            hw_count = hw_days // 3 if hw_days >= 3 else 0
            hw_count = int(rng.integers(1, hw_count + 1)) if hw_count else 0
            hw_days = hw_days if hw_count else 0
            hw_runs = split_into_runs(rng, hw_days, hw_count, 3)
            hw_int = round(float(rng.uniform(0.8, 3.0) + 0.3 * max(hw_runs)), 1) if hw_runs else None

            cold_days = int(rng.poisson(max(1.0, (18 - 5 * t) * frac)))
            cs_days = int(min(cold_days, rng.binomial(cold_days, 0.4)))
            cs_count = cs_days // 3 if cs_days >= 3 else 0
            cs_count = int(rng.integers(1, cs_count + 1)) if cs_count else 0
            cs_days = cs_days if cs_count else 0

            heavy = int(rng.poisson(rain_rate * (1 + 0.04 * t) * frac))
            prcp_total = round(float(prcp_mean * frac * rng.lognormal(0, 0.18)), 1)
            max_daily = round(float(prcp_total * rng.uniform(0.04, 0.12)), 1)

            longest = int(max(3, rng.normal(dry_base * (1 + 0.04 * t), dry_base * 0.18)))
            longest = int(min(longest, round(365 * frac)))
            if longest >= 10:
                n_dry = int(max(1, rng.poisson(1 + 6 * min(1.0, dry_base / 140))))
                others = [int(rng.integers(10, max(11, min(longest, 40)) + 1)) for _ in range(n_dry - 1)]
                dry_runs = [longest] + others
            else:
                dry_runs = []
            dry_count, dry_days = len(dry_runs), int(sum(dry_runs))

            # Compound: part of the heatwave days that fall inside dry spells
            if hw_count and dry_count:
                comp_days = int(rng.binomial(hw_days, min(0.9, dry_days / (365 * frac))))
                comp_days = min(comp_days, hw_days, dry_days)
                comp_count = int(min(hw_count, max(1, comp_days // 3))) if comp_days else 0
            else:
                comp_days = comp_count = 0

            anomaly = round(float(rng.normal(0.25 * t, 0.35)), 2)

            rows.append({
                "station_id": sid, "station_name": name, "lat": lat, "lon": lon,
                "elevation_m": elev, "region_name": region, "country": country,
                "continent": continent, "year": year, "days_present_pct": pct,
                "hot_days": hot_days, "heatwave_count": hw_count,
                "heatwave_days": hw_days, "heatwave_max_intensity": hw_int,
                "cold_days": cold_days, "coldsnap_count": cs_count,
                "coldsnap_days": cs_days, "heavy_rain_days": heavy,
                "max_daily_prcp_mm": max_daily, "prcp_total_mm": prcp_total,
                "dry_spell_count": dry_count, "dry_spell_days": dry_days,
                "longest_dry_spell": longest, "compound_event_count": comp_count,
                "compound_days": comp_days, "tmax_anomaly_mean": anomaly,
            })
    return pd.DataFrame(rows)[TABLE_A_COLUMNS]


def mann_kendall_p(values):
    years = np.arange(len(values))
    return float(stats.kendalltau(years, values).pvalue)


def make_summary(rng, table_a):
    valid = table_a[table_a["days_present_pct"] >= VALID_YEAR_PCT]
    meta = table_a.drop_duplicates("station_id").set_index("station_id")
    rows = []
    for event_type, metric in TREND_METRIC.items():
        for sid, g in valid.groupby("station_id"):
            g = g.sort_values("year")
            years_used = len(g)
            eligible = years_used >= MIN_TREND_YEARS
            slope = p = None
            if eligible:
                res = stats.theilslopes(g[metric], g["year"])
                slope = round(float(res.slope) * 10, 2)  # per decade
                p = round(mann_kendall_p(g[metric].to_numpy()), 3)
            recent = g[g["year"] >= LAST_YEAR - 10][metric].mean()
            rows.append({
                "station_id": sid, "lat": meta.at[sid, "lat"], "lon": meta.at[sid, "lon"],
                "region_name": meta.at[sid, "region_name"], "country": meta.at[sid, "country"],
                "continent": meta.at[sid, "continent"], "event_type": event_type,
                "trend_slope": slope, "trend_p_value": p, "years_used": years_used,
                "trend_eligible": eligible, "_recent": recent,
            })
    df = pd.DataFrame(rows)

    # PLACEHOLDER risk score (formula not agreed yet): 60% recent level rank,
    # 40% trend rank, within each event type, scaled 0-100.
    parts = []
    for _, g in df.groupby("event_type"):
        g = g.copy()
        level = g["_recent"].rank(pct=True)
        trend = g["trend_slope"].fillna(g["trend_slope"].median()).rank(pct=True)
        g["risk_score"] = (100 * (0.6 * level + 0.4 * trend)).round(1)
        g["risk_rank"] = g["risk_score"].rank(ascending=False, method="min").astype(int)
        z = (g["risk_score"] - g["risk_score"].mean()) / g["risk_score"].std() * 1.6
        g["gi_z_score"] = (z + rng.normal(0, 0.4, len(g))).round(2)
        g["gi_p_value"] = (2 * stats.norm.sf(g["gi_z_score"].abs())).round(3)
        parts.append(g)
    df = pd.concat(parts)

    def classify(row):
        z, p = row["gi_z_score"], row["gi_p_value"]
        for level, cut in ((99, 0.01), (95, 0.05), (90, 0.10)):
            if p < cut:
                return f"{'Hot' if z > 0 else 'Cold'} spot {level}%"
        return "Not significant"

    df["hotspot_class"] = df.apply(classify, axis=1)
    cols = ["station_id", "lat", "lon", "region_name", "country", "continent",
            "event_type", "trend_slope", "trend_p_value", "years_used",
            "trend_eligible", "risk_score", "risk_rank", "gi_z_score",
            "gi_p_value", "hotspot_class"]
    return df.sort_values(["station_id", "event_type"])[cols]


def make_global_stats(rng, summary):
    rows = []
    for event_type, g in summary.groupby("event_type", sort=False):
        i = round(float(rng.uniform(0.05, 0.45)), 3)
        rows.append({"event_type": event_type, "morans_i": i,
                     "morans_p_value": round(float(rng.uniform(0.001, 0.2)), 3),
                     "n_stations": int(g["station_id"].nunique())})
    return pd.DataFrame(rows)


def main():
    rng = np.random.default_rng(SEED)
    table_a = make_station_years(rng)
    summary = make_summary(rng, table_a)
    global_stats = make_global_stats(rng, summary)

    OUT.mkdir(parents=True, exist_ok=True)
    table_a.to_csv(OUT / "mock_station_year_metrics.csv", index=False)
    summary.to_csv(OUT / "mock_station_summary.csv", index=False)
    global_stats.to_csv(OUT / "mock_global_stats.csv", index=False)
    print(f"Table A: {len(table_a)} rows, Table B: {len(summary)} rows, "
          f"Table C: {len(global_stats)} rows -> {OUT}")


if __name__ == "__main__":
    main()
