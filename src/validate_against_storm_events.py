"""
Validate detected events against NOAA Storm Events (read-only).

Compares our station-based events with the Storm Events database at state
level, for United States stations only and for the years in the Storm Events
file (the pipeline downloads one year, 2023 by default).

Storm Events records have a state, an event type and a begin and end time, but
mostly no usable coordinates, so a station cannot be matched to a record
directly. Instead, days are compared per state:

  detected state-day   a day on which at least one of our stations in that
                       state is inside a detected event
  storm state-day      a day on which Storm Events has a record of a matching
                       type in that state

  precision  share of detected state-days within +/-1 day of a storm state-day
  recall     share of storm state-days within +/-1 day of a detected state-day
  base rate  share of all state-days (in the covered states) that have a
             storm record. Precision far above the base rate means the
             detections are not just landing on days when something is
             always reported.
  lift       precision / base rate

Only states with at least one of our stations are counted. This is a coarse
check, not a ground truth: a storm record is a local report, a station is a
single point, and drought records follow a weekly monitor rather than a
daily series. Low precision for rain and drought is expected; heat is the
cleanest comparison.

Reads:  data/parquet/extreme_events, data/parquet/storm_events,
        data/export/station_geography.csv
Writes: data/export/storm_events_validation.csv
"""

import datetime
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent

EVENTS_PATH = PROJECT_ROOT / "data" / "parquet" / "extreme_events"
STORM_PATH = PROJECT_ROOT / "data" / "parquet" / "storm_events"
GEOGRAPHY_PATH = PROJECT_ROOT / "data" / "export" / "station_geography.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "export" / "storm_events_validation.csv"

TOLERANCE_DAYS = 1
MAX_RECORD_DAYS = 120        # cap very long records (for example drought)

# category -> (our event types, matching Storm Events types)
CATEGORIES = {
    "heatwave": (["HEATWAVE"], ["Excessive Heat", "Heat"]),
    "cold_snap": (
        ["COLD_SNAP"],
        ["Extreme Cold/Wind Chill", "Cold/Wind Chill", "Frost/Freeze"],
    ),
    "heavy_rain": (
        ["HEAVY_RAIN", "EXTREME_RAIN"],
        ["Heavy Rain", "Flash Flood", "Flood"],
    ),
    "dry_spell": (["DRY_SPELL"], ["Drought"]),
}

OUTPUT_COLUMNS = [
    "category", "stations", "states", "detected_state_days", "precision",
    "storm_state_days", "recall", "base_rate", "lift",
]


def expand_days(states, begins, ends):
    """Set of (state, day) for records spanning begin..end (inclusive)."""
    days = set()
    for state, begin, end in zip(states, begins, ends):
        for day in pd.date_range(begin, end):
            days.add((state, day))
    return days


def storm_state_days(storm, storm_types):
    """Storm Events state-days for the given event types."""
    records = storm[storm["EVENT_TYPE"].isin(storm_types)].copy()
    begin = pd.to_datetime(
        records["BEGIN_DATE_TIME"], format="%d-%b-%y %H:%M:%S", errors="coerce"
    ).dt.normalize()
    end = pd.to_datetime(
        records["END_DATE_TIME"], format="%d-%b-%y %H:%M:%S", errors="coerce"
    ).dt.normalize()
    valid = begin.notna() & end.notna()
    begin, end = begin[valid], end[valid]
    end = pd.concat([begin, end], axis=1).max(axis=1)
    limit = begin + datetime.timedelta(days=MAX_RECORD_DAYS)
    end = end.where(end <= limit, limit)
    states = records.loc[valid, "STATE"].str.upper()
    return expand_days(states, begin, end)


def detected_state_days(events, event_types, state_of_station):
    """State-days with at least one detected event of the given types."""
    chosen = events[events["event_type"].isin(event_types)].copy()
    chosen["state"] = chosen["station_id"].map(state_of_station)
    chosen = chosen.dropna(subset=["state"])
    return expand_days(
        chosen["state"],
        pd.to_datetime(chosen["start_date"]),
        pd.to_datetime(chosen["end_date"]),
    )


def near(day_set, state, day):
    """True if the state has a day in the set within the tolerance."""
    for shift in range(-TOLERANCE_DAYS, TOLERANCE_DAYS + 1):
        if (state, day + datetime.timedelta(days=shift)) in day_set:
            return True
    return False


def compare(detected, storm_days, covered_states, n_days_per_state):
    """Precision, recall, base rate and lift for one category."""
    storm_days = {d for d in storm_days if d[0] in covered_states}

    matched_detected = sum(near(storm_days, s, d) for s, d in detected)
    matched_storm = sum(near(detected, s, d) for s, d in storm_days)

    precision = matched_detected / len(detected) if detected else None
    recall = matched_storm / len(storm_days) if storm_days else None
    base_rate = (
        len(storm_days) / (len(covered_states) * n_days_per_state)
        if covered_states and n_days_per_state else None
    )
    lift = (
        precision / base_rate
        if precision is not None and base_rate else None
    )
    return {
        "detected_state_days": len(detected),
        "precision": precision,
        "storm_state_days": len(storm_days),
        "recall": recall,
        "base_rate": base_rate,
        "lift": lift,
    }


def validate(events, storm, geography):
    """Return the validation table."""
    us = geography[geography["country"] == "United States"].dropna(
        subset=["region_name"]
    )
    state_of_station = dict(zip(us["station_id"], us["region_name"].str.upper()))
    covered_states = set(state_of_station.values())

    years = pd.to_numeric(storm["YEAR"], errors="coerce").dropna().astype(int)
    first, last = f"{years.min()}-01-01", f"{years.max()}-12-31"
    n_days = len(pd.date_range(first, last))

    events = events.copy()
    start = pd.to_datetime(events["start_date"])
    events = events[(start >= first) & (start <= last)]

    rows = []
    for category, (our_types, storm_types) in CATEGORIES.items():
        detected = detected_state_days(events, our_types, state_of_station)
        detected = {d for d in detected if first <= str(d[1].date()) <= last}
        station_ids = set(
            events.loc[events["event_type"].isin(our_types), "station_id"]
        ) & set(state_of_station)
        result = compare(
            detected, storm_state_days(storm, storm_types),
            covered_states, n_days,
        )
        rows.append({
            "category": category,
            "stations": len(station_ids),
            "states": len({state_of_station[s] for s in station_ids}),
            **result,
        })

    table = pd.DataFrame(rows)[OUTPUT_COLUMNS]
    for column in ("precision", "recall", "base_rate"):
        table[column] = table[column].astype(float).round(4)
    table["lift"] = table["lift"].astype(float).round(1)
    return table


def main():
    for path in (EVENTS_PATH, STORM_PATH, GEOGRAPHY_PATH):
        if not path.exists():
            print(f"ERROR: Required input not found: {path}")
            return False

    events = pd.read_parquet(EVENTS_PATH)
    events["event_type"] = events["event_type"].astype(str)
    storm = pd.read_parquet(STORM_PATH)
    geography = pd.read_csv(GEOGRAPHY_PATH)

    table = validate(events, storm, geography)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(OUTPUT_PATH, index=False)

    print("=" * 65)
    print("VALIDATION AGAINST NOAA STORM EVENTS (state level)")
    print("=" * 65)
    print(f"Match tolerance: +/-{TOLERANCE_DAYS} day. United States stations only.")
    print(table.to_string(index=False))
    print("\nPrecision well above the base rate (lift above 1) means detected "
          "events fall on days when Storm Events also has a matching record.")
    print(f"\nOutput: {OUTPUT_PATH}")
    return True


if __name__ == "__main__":
    if not main():
        sys.exit(1)
