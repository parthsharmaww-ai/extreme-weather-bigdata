
"""
Validate the calculated extreme-weather thresholds.
This script checks the output without modifying any data.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when


PROJECT_ROOT = Path(__file__).resolve().parent.parent
THRESHOLD_PATH = (
    PROJECT_ROOT / "data" / "parquet" / "extreme_thresholds_1991_2020"
)


def main():
    spark = (
        SparkSession.builder
        .appName("ValidateExtremeWeatherThresholds")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not THRESHOLD_PATH.exists():
            raise FileNotFoundError(
                f"Threshold dataset not found: {THRESHOLD_PATH}"
            )

        thresholds = spark.read.parquet(str(THRESHOLD_PATH))

        total_rows = thresholds.count()
        station_count = thresholds.select("station_id").distinct().count()

        print("\n=== THRESHOLD VALIDATION ===")
        print(f"Total threshold rows: {total_rows:,}")
        print(f"Distinct stations: {station_count}")

        print("\nRows by element:")
        thresholds.groupBy("element").count().orderBy("element").show()

        print("\nRows by station and element:")
        thresholds.groupBy("station_id", "element").count().orderBy(
            "station_id", "element"
        ).show(100, truncate=False)

        duplicate_keys = (
            thresholds
            .groupBy("station_id", "element", "target_calendar_day")
            .count()
            .filter(col("count") > 1)
            .count()
        )
        print(f"\nDuplicate station-element-day keys: {duplicate_keys}")

        invalid_values = thresholds.filter(
            col("threshold").isNull()
            | isnan(col("threshold"))
        ).count()
        print(f"Missing or invalid threshold values: {invalid_values}")

        print("\nThreshold ranges by element:")
        thresholds.groupBy("element").agg(
            {"threshold": "min"}
        ).show()
        thresholds.groupBy("element").agg(
            {"threshold": "max"}
        ).show()

        print("\nMissing rainfall threshold days by station:")
        thresholds.filter(col("element") == "PRCP").groupBy(
            "station_id"
        ).count().filter(
            col("count") < 366
        ).orderBy("station_id").show(100, truncate=False)

        print("\nCentral Park TMAX threshold for July 15:")
        thresholds.filter(
            (col("station_id") == "USW00094728")
            & (col("element") == "TMAX")
            & (col("target_calendar_day") == 197)
        ).select(
            "station_id", "element", "target_calendar_day", "threshold"
        ).show(truncate=False)

        print("\nValidation checks:")
        print(f"Expected total rows: 25,226")
        print(f"Actual total rows:   {total_rows:,}")
        print(f"Duplicate keys:      {duplicate_keys}")
        print(f"Invalid values:      {invalid_values}")

        if total_rows == 25226 and duplicate_keys == 0 and invalid_values == 0:
            print("\nBasic threshold validation PASSED.")
        else:
            print("\nReview the results before proceeding.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()