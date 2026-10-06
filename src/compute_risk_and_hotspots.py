"""
Fill the risk and hotspot columns of Table B and write Table C (global_stats).

Reads data/export/station_year_metrics.csv (Table A) and
data/export/station_summary.csv (Table B, from compute_station_summary.py).
Rewrites Table B with risk_score, risk_rank, gi_z_score, gi_p_value and
hotspot_class filled in, and writes data/export/global_stats.csv.

Only trend-eligible stations are used (at least 30 valid years in 1991-2025),
so every station is judged on the same window. All other rows stay empty.

For each event type, with "level" meaning the station's mean annual value of
the event measure over its valid years in 1991-2025:

Risk score (0-100). Both parts are percentile ranks among the eligible
stations, so the score is relative to the stations in the data set:
    risk_score = 100 * (0.6 * rank(level) + 0.4 * rank(trend_slope))
  The level says how much of the hazard a station gets now; the trend says
  how fast that is changing. risk_rank 1 is the highest score. If every
  eligible station has the same level and the same trend, there is nothing to
  rank and the columns stay empty.

Hotspots (Getis-Ord Gi*) on the level, and global Moran's I on the level:
  - Neighbours: the 5 nearest stations by great-circle distance.
  - Gi* z-score and a two-sided p-value from the normal distribution.
  - hotspot_class: "Hot spot 99% / 95% / 90%" (z > 0) or "Cold spot ..."
    (z < 0) when p < 0.01 / 0.05 / 0.10, otherwise "Not significant".
  - Moran's I uses the randomisation assumption and a two-sided p-value.
  - Needs at least 10 eligible stations and a level that is not constant.
  Limitations: with many stations and event types some "significant" cells
  are expected by chance (no multiple-testing correction), and on a sparse
  global network the 5 nearest neighbours can be far apart.
"""

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from esda.getisord import G_Local
from esda.moran import Moran
from libpysal.weights import W
from scipy.stats import norm, rankdata

from compute_station_summary import (
    EVENT_METRICS,
    TREND_FIRST_YEAR,
    TREND_LAST_YEAR,
    VALID_YEAR_PCT,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent

TABLE_A_FILE = PROJECT_ROOT / "data" / "export" / "station_year_metrics.csv"
TABLE_B_FILE = PROJECT_ROOT / "data" / "export" / "station_summary.csv"
TABLE_C_FILE = PROJECT_ROOT / "data" / "export" / "global_stats.csv"

NEIGHBOURS = 5
MIN_STATIONS = 10
LEVEL_WEIGHT = 0.6
TREND_WEIGHT = 0.4
EARTH_RADIUS_KM = 6371.0088

RISK_COLUMNS = [
    "risk_score", "risk_rank", "gi_z_score", "gi_p_value", "hotspot_class",
]
TABLE_C_COLUMNS = ["event_type", "morans_i", "morans_p_value", "n_stations"]


def station_levels(table_a, metric):
    """Mean annual value of the metric over valid years in the trend window."""
    valid = table_a[
        table_a["year"].between(TREND_FIRST_YEAR, TREND_LAST_YEAR)
        & (table_a["days_present_pct"] >= VALID_YEAR_PCT)
    ].dropna(subset=[metric])
    return valid.groupby("station_id")[metric].mean()


def percentile_rank(values):
    """0 for the lowest value, 1 for the highest, ties share the average."""
    values = np.asarray(values, dtype=float)
    if len(values) == 1:
        return np.array([0.5])
    return (rankdata(values, method="average") - 1) / (len(values) - 1)


def risk_scores(level, slope):
    """Return (risk_score, risk_rank) arrays, or (None, None) if no variation."""
    if np.ptp(level) == 0 and np.ptp(slope) == 0:
        return None, None
    score = 100 * (
        LEVEL_WEIGHT * percentile_rank(level)
        + TREND_WEIGHT * percentile_rank(slope)
    )
    rank = rankdata(-score, method="min").astype(int)
    return score, rank


def haversine_matrix(lat, lon):
    """Great-circle distances in km between all pairs of stations."""
    phi = np.radians(np.asarray(lat, dtype=float))
    lam = np.radians(np.asarray(lon, dtype=float))
    dphi = phi[:, None] - phi[None, :]
    dlam = lam[:, None] - lam[None, :]
    a = (
        np.sin(dphi / 2) ** 2
        + np.cos(phi)[:, None] * np.cos(phi)[None, :] * np.sin(dlam / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def nearest_neighbour_weights(lat, lon, k=NEIGHBOURS):
    """Spatial weights: each station is linked to its k nearest stations."""
    distances = haversine_matrix(lat, lon)
    n = len(distances)
    k = min(k, n - 1)
    neighbours = {}
    for i in range(n):
        order = [j for j in np.argsort(distances[i], kind="stable") if j != i]
        neighbours[i] = order[:k]
    with warnings.catch_warnings():
        # A graph with separate groups is reported once in main(), not on every call.
        warnings.simplefilter("ignore", UserWarning)
        return W(neighbours, {i: [1.0] * len(neighbours[i]) for i in range(n)})


def connected_groups(lat, lon):
    """Number of separate groups in the nearest-neighbour graph."""
    return nearest_neighbour_weights(lat, lon).n_components


def classify(z, p):
    """Hot or cold spot class from a Gi* z-score and its two-sided p-value."""
    if np.isnan(z) or np.isnan(p):
        return None
    for level, label in ((0.01, "99%"), (0.05, "95%"), (0.10, "90%")):
        if p < level:
            return f"Hot spot {label}" if z > 0 else f"Cold spot {label}"
    return "Not significant"


def hotspot_statistics(level, lat, lon):
    """Gi* z, p, class per station plus global Moran's I; None if not possible."""
    level = np.asarray(level, dtype=float)
    if len(level) < MIN_STATIONS or np.ptp(level) == 0:
        return None
    weights = nearest_neighbour_weights(lat, lon)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        gi = G_Local(level, weights, transform="B", star=True, permutations=0)
        moran = Moran(level, weights, transformation="r", permutations=0,
                      two_tailed=True)
    z = np.asarray(gi.Zs, dtype=float)
    p = 2 * (1 - norm.cdf(np.abs(z)))
    classes = [classify(zi, pi) for zi, pi in zip(z, p)]
    return z, p, classes, float(moran.I), float(moran.p_rand)


def build_tables(table_a, table_b):
    """Return (updated Table B, Table C)."""
    summary = table_b.copy()
    for column in RISK_COLUMNS:
        summary[column] = None
    summary["risk_rank"] = summary["risk_rank"].astype("object")

    global_rows = []
    for event_type, metric in EVENT_METRICS.items():
        rows = summary[
            (summary["event_type"] == event_type) & summary["trend_eligible"]
        ]
        levels = station_levels(table_a, metric)
        rows = rows[rows["station_id"].isin(levels.index)]
        n_stations = len(rows)
        row_ids = rows.index

        moran_i = moran_p = None
        if n_stations > 0:
            level = levels.loc[rows["station_id"]].to_numpy()
            slope = rows["trend_slope"].to_numpy(dtype=float)

            score, rank = risk_scores(level, slope)
            if score is not None:
                summary.loc[row_ids, "risk_score"] = np.round(score, 1)
                summary.loc[row_ids, "risk_rank"] = rank

            has_position = rows["lat"].notna() & rows["lon"].notna()
            if has_position.sum() == n_stations:
                result = hotspot_statistics(
                    level, rows["lat"].to_numpy(), rows["lon"].to_numpy()
                )
                if result is not None:
                    z, p, classes, moran_i, moran_p = result
                    summary.loc[row_ids, "gi_z_score"] = np.round(z, 3)
                    summary.loc[row_ids, "gi_p_value"] = np.round(p, 4)
                    summary.loc[row_ids, "hotspot_class"] = classes
                    moran_i, moran_p = round(moran_i, 4), round(moran_p, 4)

        global_rows.append({
            "event_type": event_type,
            "morans_i": moran_i,
            "morans_p_value": moran_p,
            "n_stations": n_stations,
        })

    summary["risk_rank"] = pd.to_numeric(summary["risk_rank"]).astype("Int64")
    for column in ("risk_score", "gi_z_score", "gi_p_value"):
        summary[column] = pd.to_numeric(summary[column])
    return summary, pd.DataFrame(global_rows)[TABLE_C_COLUMNS]


def main():
    for path in (TABLE_A_FILE, TABLE_B_FILE):
        if not path.exists():
            print(f"ERROR: Required input not found: {path}")
            print("Run src/export_station_year_metrics.py and "
                  "src/compute_station_summary.py first.")
            return False

    table_a = pd.read_csv(TABLE_A_FILE)
    table_b = pd.read_csv(TABLE_B_FILE)
    if table_b.empty:
        print("ERROR: Table B is empty.")
        return False

    summary, global_stats = build_tables(table_a, table_b)

    stations = summary[summary["trend_eligible"]].drop_duplicates("station_id")
    stations = stations.dropna(subset=["lat", "lon"])
    groups = connected_groups(stations["lat"].to_numpy(), stations["lon"].to_numpy()) if len(stations) > NEIGHBOURS else 1

    summary.to_csv(TABLE_B_FILE, index=False)
    global_stats.to_csv(TABLE_C_FILE, index=False)

    print("=" * 65)
    print("RISK SCORE AND HOTSPOTS")
    print("=" * 65)
    print(f"Neighbours per station: {NEIGHBOURS}; minimum stations: "
          f"{MIN_STATIONS}; weights: level {LEVEL_WEIGHT}, trend {TREND_WEIGHT}")
    if groups > 1:
        print(f"\nNote: the {NEIGHBOURS}-nearest-neighbour graph has {groups} separate groups of "
              "stations (for example a continent far from the others). Hotspot classes are "
              "relative within each group.")
    print("\nGlobal statistics (Table C):")
    print(global_stats.to_string(index=False))

    spots = summary.dropna(subset=["hotspot_class"])
    if not spots.empty:
        print("\nHotspot classes by event type:")
        print(pd.crosstab(spots["event_type"], spots["hotspot_class"]).to_string())

    print(f"\nUpdated: {TABLE_B_FILE}")
    print(f"Written: {TABLE_C_FILE}")
    return True


if __name__ == "__main__":
    if not main():
        sys.exit(1)
