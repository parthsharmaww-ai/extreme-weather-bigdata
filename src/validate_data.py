
"""
Validate processed GHCN-Daily and NOAA Storm Events Parquet datasets.

This script reads existing Parquet files only; it does not modify them.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
GHCN_PATH = PROJECT_ROOT / "data" / "parquet" / "ghcn_daily"
STORM_PATH = PROJECT_ROOT / "data" / "parquet" / "storm_events"


def validate_ghcn(spark):
    print("\n" + "=" * 65)
    print("GHCN-DAILY VALIDATION")
    print("=" * 65)

    if not GHCN_PATH.exists():
        print(f"Missing Parquet directory: {GHCN_PATH}")
        return False

    df = spark.read.parquet(str(GHCN_PATH))

    print("\nSchema:")
    df.printSchema()

    print("\nTotal valid observations:")
    print(f"{df.count():,}")

    print("\nDate range:")
    df.select(
        spark_min("date").alias("first_date"),
        spark_max("date").alias("last_date"),
    ).show(truncate=False)

    print("\nDistinct stations:")
    print(df.select(countDistinct("station_id")).first()[0])

    print("\nStation IDs:")
    df.select("station_id").distinct().orderBy("station_id").show(
        100, truncate=False
    )

    print("\nObservation counts by element:")
    df.groupBy("element").count().orderBy("element").show(truncate=False)

    print("\nObservation counts by station and element:")
    df.groupBy("station_id", "element").count().orderBy(
        "station_id", "element"
    ).show(100, truncate=False)

    print("\nMissing values in important fields:")
    df.select(
        spark_sum(col("station_id").isNull().cast("long")).alias(
            "missing_station_id"
        ),
        spark_sum(col("date").isNull().cast("long")).alias("missing_date"),
        spark_sum(col("element").isNull().cast("long")).alias(
            "missing_element"
        ),
        spark_sum(col("value").isNull().cast("long")).alias("missing_value"),
    ).show(truncate=False)

    print("\nDuplicate station-date-element records:")
    duplicate_groups = (
        df.groupBy("station_id", "date", "element")
        .agg(count("*").alias("records"))
        .filter(col("records") > 1)
    )
    print(f"Duplicate groups: {duplicate_groups.count():,}")
    duplicate_groups.show(10, truncate=False)

    print("\nValue ranges by element:")
    df.groupBy("element").agg(
        spark_min("value").alias("minimum"),
        spark_max("value").alias("maximum"),
    ).orderBy("element").show(truncate=False)

    return True


def validate_storm_events(spark):
    print("\n" + "=" * 65)
    print("NOAA STORM EVENTS VALIDATION")
    print("=" * 65)

    if not STORM_PATH.exists():
        print(f"Missing Parquet directory: {STORM_PATH}")
        return False

    df = spark.read.parquet(str(STORM_PATH))

    print("\nSchema:")
    df.printSchema()

    print("\nTotal storm-event records:")
    print(f"{df.count():,}")

    print("\nAvailable columns:")
    print(", ".join(df.columns))

    if "YEAR" in df.columns:
        print("\nYears represented:")
        df.groupBy("YEAR").count().orderBy("YEAR").show(
            100, truncate=False
        )

    if "EVENT_TYPE" in df.columns:
        print("\nMost frequent event types:")
        df.groupBy("EVENT_TYPE").count().orderBy(
            col("count").desc()
        ).show(20, truncate=False)

    print("\nMissing key fields:")
    key_fields = [
        field
        for field in ["EVENT_ID", "YEAR", "STATE", "EVENT_TYPE"]
        if field in df.columns
    ]

    if key_fields:
        df.select(*[
            spark_sum(col(field).isNull().cast("long")).alias(
                f"missing_{field.lower()}"
            )
            for field in key_fields
        ]).show(truncate=False)

    if "EVENT_ID" in df.columns:
        print("\nDuplicate EVENT_ID values:")
        duplicate_events = (
            df.groupBy("EVENT_ID")
            .count()
            .filter(col("count") > 1)
        )
        print(f"Duplicate EVENT_ID groups: {duplicate_events.count():,}")
        duplicate_events.show(10, truncate=False)

    return True


def main():
    spark = (
        SparkSession.builder
        .appName("ExtremeWeatherDataValidation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        ghcn_ok = validate_ghcn(spark)
        storm_ok = validate_storm_events(spark)

        print("\n" + "=" * 65)
        print("VALIDATION SUMMARY")
        print("=" * 65)
        print(f"GHCN-Daily: {'FOUND' if ghcn_ok else 'NOT FOUND'}")
        print(f"Storm Events: {'FOUND' if storm_ok else 'NOT FOUND'}")

        if not (ghcn_ok and storm_ok):
            sys.exit(1)

        print(
            "\nValidation report completed. Review the counts, "
            "date ranges, missing values, and duplicates above."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()