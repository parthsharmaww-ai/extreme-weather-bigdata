"""Build the ENSO lookup table (Table D, enso_monthly) for the dashboard.

Source: NOAA CPC Oceanic Nino Index (ONI), the 3-month running mean of
Nino 3.4 sea-surface temperature anomalies.
    https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt

Input:  data/raw/oni.ascii.txt (NOAA original: SEAS YR TOTAL ANOM) or
        data/raw/oni_anomalies.txt (SEAS YR ANOM). The last column is read.
Output: tableau/enso_monthly.csv
        year, month, season, nino34_anomaly, enso_phase, provisional

Each 3-month season is assigned to its centre month (DJF -> 1, JFM -> 2, ...).
enso_phase follows NOAA's operational rule: El Nino (La Nina) when the ONI is
>= +0.5 (<= -0.5) for at least 5 consecutive overlapping seasons, else Neutral.

provisional = True for the latest seasons whose phase can still change: a run
at the end of the record that is past the +/-0.5 threshold but has not yet
reached 5 seasons. They stay "Neutral" until the run is long enough (or ends).
"""

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_CANDIDATES = [
    PROJECT_ROOT / "data" / "raw" / "oni.ascii.txt",
    PROJECT_ROOT / "data" / "raw" / "oni_anomalies.txt",
]
OUTPUT_FILE = PROJECT_ROOT / "tableau" / "enso_monthly.csv"

SEASONS = ["DJF", "JFM", "FMA", "MAM", "AMJ", "MJJ",
           "JJA", "JAS", "ASO", "SON", "OND", "NDJ"]
THRESHOLD = 0.5
MIN_RUN = 5


def read_oni(path: Path) -> pd.DataFrame:
    rows = []
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) < 3 or parts[0] not in SEASONS:
            continue
        rows.append({
            "year": int(parts[1]),
            "month": SEASONS.index(parts[0]) + 1,
            "season": parts[0],
            "nino34_anomaly": float(parts[-1]),
        })
    if not rows:
        raise ValueError(f"No ONI rows parsed from {path}")
    return pd.DataFrame(rows).sort_values(["year", "month"]).reset_index(drop=True)


def label_phases(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    sign = pd.Series(0, index=df.index)
    sign[df["nino34_anomaly"] >= THRESHOLD] = 1
    sign[df["nino34_anomaly"] <= -THRESHOLD] = -1

    phase = pd.Series("Neutral", index=df.index)
    provisional = pd.Series(False, index=df.index)
    run_id = (sign != sign.shift()).cumsum()
    runs = list(sign.groupby(run_id))
    for i, (_, run) in enumerate(runs):
        if run.iloc[0] == 0:
            continue
        if len(run) >= MIN_RUN:
            phase[run.index] = "El Nino" if run.iloc[0] == 1 else "La Nina"
        elif i == len(runs) - 1:
            # Open run at the end of the record: not yet 5 seasons long.
            provisional[run.index] = True
    return phase, provisional


def main() -> None:
    source = next((p for p in RAW_CANDIDATES if p.exists()), None)
    if source is None:
        raise FileNotFoundError(
            "Download oni.ascii.txt from NOAA CPC into data/raw/ first."
        )

    df = read_oni(source)
    df["enso_phase"], df["provisional"] = label_phases(df)

    if df.duplicated(["year", "month"]).any():
        raise ValueError("Duplicate year-month rows in ONI input.")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Source: {source.name}")
    print(f"Rows: {len(df):,}  ({df['year'].min()}-{df['year'].max()})")
    print(df["enso_phase"].value_counts().to_string())
    prov = df[df["provisional"]]
    if not prov.empty:
        print("Provisional (open run < 5 seasons): "
              + ", ".join(f"{r.season} {r.year} ({r.nino34_anomaly:+.2f})"
                          for r in prov.itertuples()))
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
