"""
Calculate baseline calendar-day mean TMAX per station for 1991-2020.

Uses the same calendar mapping (leap reference year 2000, 366 positions) and
the same +/-7-day window as calculate_thresholds.py. The output is used to
define tmax_anomaly_mean as TMAX minus the baseline mean for that calendar day.

Raw and baseline datasets are not modified.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    dayofyear,
    explode,
    lit,
    pmod,
    sequence,
    to_date,
)
from pyspark.sql.types import IntegerType


PROJECT_ROOT = Path(__file__).resolve().parent.parent

BASELINE_PATH = (
    PROJECT_ROOT / "data" / "parquet" / "ghcn_baseline_1991_2020"
)
OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "parquet" / "baseline_means_1991_2020"
)

WINDOW_DAYS = 7
CALENDAR_DAYS = 366


def calculate_baseline_means(spark):
    """Calculate the +/-7-day baseline mean of TMAX for each calendar day."""

    if not BASELINE_PATH.exists():
        print(f"ERROR: Baseline dataset not found: {BASELINE_PATH}")
        print("Run src/prepare_baseline.py first.")
        return False

    print("=" * 65)
    print("BASELINE CALENDAR-DAY MEAN TMAX")
    print("=" * 65)
    print(f"Calendar window: +/-{WINDOW_DAYS} days, {CALENDAR_DAYS} positions")

    baseline = spark.read.parquet(str(BASELINE_PATH))

    # Same month/day mapping as calculate_thresholds.py.
    tmax = (
        baseline
        .filter(
            (col("element") == "TMAX")
            & col("value").isNotNull()
            & col("date").isNotNull()
        )
        .withColumn(
            "calendar_date",
            to_date(
                date_format(col("date"), "'2000-'MM-dd"),
                "yyyy-MM-dd",
            ),
        )
        .withColumn(
            "calendar_day",
            dayofyear(col("calendar_date")).cast(IntegerType()),
        )
    )

    # Spread each observation over the target days within +/-7 days,
    # wrapping around the calendar year.
    expanded = (
        tmax
        .withColumn(
            "offset",
            explode(sequence(lit(-WINDOW_DAYS), lit(WINDOW_DAYS))),
        )
        .withColumn(
            "target_calendar_day",
            (
                pmod(
                    col("calendar_day") - 1 + col("offset"),
                    lit(CALENDAR_DAYS),
                ) + 1
            ).cast(IntegerType()),
        )
    )

    means = (
        expanded
        .groupBy("station_id", "target_calendar_day")
        .agg(
            avg("value").alias("baseline_mean_tmax"),
            count("*").alias("n_obs"),
        )
    )

    output_count = means.count()

    if output_count == 0:
        print("ERROR: No baseline means were calculated.")
        return False

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    means.write.mode("overwrite").parquet(str(OUTPUT_PATH))

    print(f"\nBaseline mean rows: {output_count:,}")
    print(f"Saved to: {OUTPUT_PATH}")

    print("\nRows per station (expect 366 each):")
    means.groupBy("station_id").count().orderBy("station_id").show(
        100, truncate=False
    )

    print("\nSample (calendar day 197 = July 15):")
    means.filter(col("target_calendar_day") == 197).orderBy(
        "station_id"
    ).show(100, truncate=False)

    return True


def main():
    spark = (
        SparkSession.builder
        .appName("ExtremeWeatherBaselineMeans")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        success = calculate_baseline_means(spark)

        if not success:
            sys.exit(1)

        print("\nBaseline mean calculation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()