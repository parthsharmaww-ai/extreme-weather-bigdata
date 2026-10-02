
"""
Prepare the 1991-2020 baseline dataset and assess station completeness.

This script preserves raw data and does not apply event thresholds.
The team's minimum-years requirement is not yet finalized, so stations
are not automatically excluded.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    lit,
    round as spark_round,
    when,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_PATH = PROJECT_ROOT / "data" / "parquet" / "ghcn_daily"
OUTPUT_DIR = PROJECT_ROOT / "data" / "parquet"

BASELINE_START = "1991-01-01"
BASELINE_END = "2020-12-31"

# 1991-2020 inclusive: 30 years, including 8 leap years.
EXPECTED_DAYS = 10958
COMPLETENESS_THRESHOLD = 0.80


def prepare_baseline(spark):
    """Filter baseline observations and calculate completeness."""

    if not INPUT_PATH.exists():
        print(f"ERROR: Input Parquet directory not found: {INPUT_PATH}")
        return False

    print("=" * 65)
    print("BASELINE DATA PREPARATION")
    print("=" * 65)
    print(f"Baseline start: {BASELINE_START}")
    print(f"Baseline end:   {BASELINE_END}")
    print(f"Expected days:  {EXPECTED_DAYS:,}")
    print(f"Completeness threshold: {COMPLETENESS_THRESHOLD:.0%}")

    # Read the previously prepared daily observations.
    weather = spark.read.parquet(str(INPUT_PATH))

    # Select the baseline period without changing the source dataset.
    baseline = weather.filter(
        (col("date") >= lit(BASELINE_START).cast("date"))
        & (col("date") <= lit(BASELINE_END).cast("date"))
        & col("element").isin("TMAX", "TMIN", "PRCP")
    )

    baseline_count = baseline.count()

    if baseline_count == 0:
        print("ERROR: No observations found in the baseline period.")
        return False

    print(f"\nBaseline observations: {baseline_count:,}")

    print("\nObservation counts by station and element:")
    baseline.groupBy("station_id", "element").count().orderBy(
        "station_id", "element"
    ).show(100, truncate=False)

    # Assess completeness separately for each station and variable.
    # The input dataset already excludes missing values and invalid dates.
    completeness = (
        baseline.groupBy("station_id", "element")
        .agg(countDistinct("date").alias("observed_days"))
        .withColumn("expected_days", lit(EXPECTED_DAYS))
        .withColumn(
            "completeness_percent",
            spark_round(
                col("observed_days") / col("expected_days") * 100, 2
            ),
        )
        .withColumn(
            "meets_80_percent",
            when(
                col("observed_days")
                / col("expected_days")
                >= lit(COMPLETENESS_THRESHOLD),
                lit("YES"),
            ).otherwise(lit("NO")),
        )
        .orderBy("station_id", "element")
    )

    print("\nStation-variable completeness report:")
    completeness.show(100, truncate=False)

    # Save baseline observations and completeness results.
    baseline_path = OUTPUT_DIR / "ghcn_baseline_1991_2020"
    completeness_path = OUTPUT_DIR / "station_completeness_1991_2020"

    baseline.write.mode("overwrite").parquet(str(baseline_path))
    completeness.write.mode("overwrite").parquet(str(completeness_path))

    print(f"\nBaseline Parquet saved to: {baseline_path}")
    print(f"Completeness report saved to: {completeness_path}")

    print(
        "\nIMPORTANT: The 80% rule is reported for each station-variable "
        "combination. No stations are automatically excluded. "
        "The team's minimum-years requirement remains undecided."
    )

    return True


def main():
    spark = (
        SparkSession.builder
        .appName("ExtremeWeatherBaselinePreparation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        success = prepare_baseline(spark)

        if not success:
            sys.exit(1)

        print("\nBaseline preparation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()