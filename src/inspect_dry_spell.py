
"""Inspect rainfall observations for an unusually long dry spell."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min as spark_min, max as spark_max, when, lit, sequence, explode, to_date, datediff

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WEATHER_PATH = PROJECT_ROOT / "data" / "parquet" / "ghcn_daily"

STATION = "USW00023174"
START_DATE = "2024-05-06"
END_DATE = "2025-01-25"


def main():
    spark = (
        SparkSession.builder
        .appName("InspectLongDrySpell")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = spark.read.parquet(str(WEATHER_PATH))

        rainfall = (
            raw
            .filter(
                (col("station_id") == STATION)
                & (col("element") == "PRCP")
                & (col("date") >= START_DATE)
                & (col("date") <= END_DATE)
            )
            .select(
                to_date(col("date")).alias("date"),
                col("value").alias("rainfall_mm"),
            )
            .dropDuplicates(["date"])
        )

        expected_days = (
            spark.range(1)
            .select(
                explode(
                    sequence(
                        to_date(lit(START_DATE)),
                        to_date(lit(END_DATE)),
                    )
                ).alias("date")
            )
        )

        daily = expected_days.join(rainfall, "date", "left")

        print("\n=== DRY SPELL RAINFALL INSPECTION ===")
        print(f"Station: {STATION}")
        print(f"Period: {START_DATE} to {END_DATE}")
        print(f"Expected calendar days: {daily.count()}")

        daily.agg(
            count("rainfall_mm").alias("observed_rainfall_days"),
            spark_min("rainfall_mm").alias("minimum_rainfall_mm"),
            spark_max("rainfall_mm").alias("maximum_rainfall_mm"),
            count(when(col("rainfall_mm").isNull(), 1)).alias("missing_days"),
            count(when(col("rainfall_mm") < 1.0, 1)).alias("days_below_1mm"),
            count(when(col("rainfall_mm") >= 1.0, 1)).alias("days_at_least_1mm"),
        ).show(truncate=False)

        print("\nRainfall observations of at least 1 mm:")
        daily.filter(
            col("rainfall_mm") >= 1.0
        ).orderBy("date").show(100, truncate=False)

        print("\nFirst and last 15 daily records:")
        daily.orderBy("date").show(15, truncate=False)
        daily.orderBy(col("date").desc()).show(15, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()