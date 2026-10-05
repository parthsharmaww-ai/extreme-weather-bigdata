"""
Validate compound events (read-only).

A compound event must be the overlap of one qualifying heatwave (3+ days) and
one qualifying dry spell (10+ days) at the same station. It must not be simply
a run of hot-and-dry days. This script rebuilds the expected compound events
from the detected heatwaves and dry spells and compares them with the
COMPOUND_HEATWAVE_DRY_SPELL events in the events table.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, greatest, least, lit


PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVENT_PATH = PROJECT_ROOT / "data" / "parquet" / "extreme_events"

HEATWAVE_MIN_DAYS = 3
DRY_SPELL_MIN_DAYS = 10
KEYS = ["station_id", "start_date", "end_date", "duration_days"]


def main():
    spark = (
        SparkSession.builder
        .appName("ValidateCompoundEvents")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not EVENT_PATH.exists():
            print(f"ERROR: Event dataset not found: {EVENT_PATH}")
            return False

        events = spark.read.parquet(str(EVENT_PATH))

        heatwaves = events.filter(col("event_type") == "HEATWAVE").select(
            "station_id",
            col("start_date").alias("h_start"),
            col("end_date").alias("h_end"),
            col("duration_days").alias("h_days"),
        )
        dry_spells = events.filter(col("event_type") == "DRY_SPELL").select(
            "station_id",
            col("start_date").alias("d_start"),
            col("end_date").alias("d_end"),
            col("duration_days").alias("d_days"),
        )
        compound = events.filter(
            col("event_type") == "COMPOUND_HEATWAVE_DRY_SPELL"
        ).select(*KEYS)

        expected = (
            heatwaves.join(dry_spells, "station_id")
            .filter(
                (col("h_start") <= col("d_end"))
                & (col("d_start") <= col("h_end"))
            )
            .select(
                "station_id",
                greatest(col("h_start"), col("d_start")).alias("start_date"),
                least(col("h_end"), col("d_end")).alias("end_date"),
            )
            .withColumn(
                "duration_days",
                datediff(col("end_date"), col("start_date")) + lit(1),
            )
        )

        compound_count = compound.count()
        expected_count = expected.count()
        not_an_overlap = compound.join(expected, KEYS, "left_anti").count()
        missing_overlap = expected.join(compound, KEYS, "left_anti").count()
        short_heatwaves = heatwaves.filter(
            col("h_days") < HEATWAVE_MIN_DAYS
        ).count()
        short_dry_spells = dry_spells.filter(
            col("d_days") < DRY_SPELL_MIN_DAYS
        ).count()

        print("\n=== COMPOUND EVENT VALIDATION ===")
        print(f"Heatwaves: {heatwaves.count():,} (minimum {HEATWAVE_MIN_DAYS} days)")
        print(f"Dry spells: {dry_spells.count():,} (minimum {DRY_SPELL_MIN_DAYS} days)")
        print(f"Compound events: {compound_count:,}")
        print(f"Overlaps of a heatwave and a dry spell: {expected_count:,}")
        print(f"Compound events that are not such an overlap: {not_an_overlap}")
        print(f"Overlaps missing from the compound events: {missing_overlap}")
        print(f"Heatwaves shorter than {HEATWAVE_MIN_DAYS} days: {short_heatwaves}")
        print(f"Dry spells shorter than {DRY_SPELL_MIN_DAYS} days: {short_dry_spells}")

        passed = (
            compound_count == expected_count
            and not_an_overlap == 0
            and missing_overlap == 0
            and short_heatwaves == 0
            and short_dry_spells == 0
        )
        if passed:
            print("\nCompound event validation PASSED.")
        else:
            print("\nCompound event validation FAILED: review the counts above.")
        return passed

    finally:
        spark.stop()


if __name__ == "__main__":
    if not main():
        sys.exit(1)

