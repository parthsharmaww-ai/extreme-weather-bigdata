
"""Validate detected extreme-weather events without modifying the data."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    datediff,
    max as spark_max,
    min as spark_min,
    when,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVENT_PATH = PROJECT_ROOT / "data" / "parquet" / "extreme_events"


def main():
    spark = (
        SparkSession.builder
        .appName("ValidateExtremeWeatherEvents")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not EVENT_PATH.exists():
            raise FileNotFoundError(
                f"Event dataset not found: {EVENT_PATH}"
            )

        events = spark.read.parquet(str(EVENT_PATH))
        total = events.count()

        print("\n=== EVENT VALIDATION ===")
        print(f"Total events: {total:,}")

        print("\nEvents by type:")
        events.groupBy("event_type").count().orderBy(
            "event_type"
        ).show(truncate=False)

        duplicate_count = (
            events
            .groupBy(
                "station_id",
                "event_type",
                "start_date",
                "end_date",
            )
            .count()
            .filter(col("count") > 1)
            .count()
        )
        print(f"Duplicate event records: {duplicate_count}")

        invalid_dates = events.filter(
            col("start_date").isNull()
            | col("end_date").isNull()
            | (col("end_date") < col("start_date"))
            | (col("duration_days") < 1)
        ).count()
        print(f"Invalid dates or durations: {invalid_dates}")

        duration_mismatches = events.filter(
            datediff(col("end_date"), col("start_date")) + 1
            != col("duration_days")
        ).count()
        print(f"Duration/date mismatches: {duration_mismatches}")

        print("\nDuration summary by event type:")
        events.groupBy("event_type").agg(
            count("*").alias("event_count"),
            spark_min("duration_days").alias("minimum_days"),
            spark_max("duration_days").alias("maximum_days"),
        ).orderBy("event_type").show(truncate=False)

        print("\nLongest dry spells:")
        events.filter(
            col("event_type") == "DRY_SPELL"
        ).orderBy(
            col("duration_days").desc()
        ).select(
            "station_id",
            "start_date",
            "end_date",
            "duration_days",
            "intensity_value",
        ).show(10, truncate=False)

        print("\nEvents by station and type:")
        events.groupBy("station_id", "event_type").count().orderBy(
            "station_id", "event_type"
        ).show(100, truncate=False)

        print("\nValidation summary:")
        print(f"Total events:             {total:,}")
        print(f"Duplicate event records:  {duplicate_count}")
        print(f"Invalid dates/durations:  {invalid_dates}")
        print(f"Duration mismatches:      {duration_mismatches}")

        if (
            total > 0
            and duplicate_count == 0
            and invalid_dates == 0
            and duration_mismatches == 0
        ):
            print("\nBasic event validation PASSED.")
            print("Review unusually long dry spells before interpretation.")
        else:
            print("\nReview the results before proceeding.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()