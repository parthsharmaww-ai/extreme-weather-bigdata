
"""
Prepare a small GHCN-Daily and NOAA Storm Events dataset with PySpark.

Raw files are preserved. Processed data is written to Parquet.
"""

import csv
import gzip
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    col,
    concat_ws,
    explode,
    lpad,
    lit,
    substring,
    to_date,
    trim,
    when,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
GHCN_DIR = PROJECT_ROOT / "data" / "raw" / "ghcn_d"
STORM_DIR = PROJECT_ROOT / "data" / "raw" / "storm_events"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"

GHCN_SCHEMA = StructType([
    StructField("value", StringType(), True),
])


def prepare_ghcn(spark):
    """Parse fixed-width GHCN-Daily files and retain selected elements."""
    files = sorted(GHCN_DIR.glob("*.dly"))

    if not files:
        print(f"No GHCN-Daily files found in {GHCN_DIR}")
        return False

    print(f"Reading {len(files)} GHCN-Daily station file(s)...")

    # Each .dly line has station, year, month, element, and 31 daily blocks.
    source = spark.read.schema(GHCN_SCHEMA).text(
        [str(path) for path in files]
    )

    records = source.select(
        substring("value", 1, 11).alias("station_id"),
        substring("value", 12, 4).cast(IntegerType()).alias("year"),
        substring("value", 16, 2).cast(IntegerType()).alias("month"),
        substring("value", 18, 4).alias("element"),
        col("value"),
    ).filter(col("element").isin("TMAX", "TMIN", "PRCP"))

    # Filter to relevant elements before expanding daily values.
    daily_blocks = []
    for day in range(1, 32):
        start = 22 + (day - 1) * 8
        daily_blocks.append(
            {
                "day": lit(day),
                "raw_value": substring("value", start, 5).cast(IntegerType()),
                "measurement_flag": substring("value", start + 5, 1),
                "quality_flag": substring("value", start + 6, 1),
                "source_flag": substring("value", start + 7, 1),
            }
        )

    daily_array = array(*[
        # Use named fields for each daily observation.
        __import__("pyspark").sql.functions.struct(
            block["day"].alias("day"),
            block["raw_value"].alias("raw_value"),
            block["measurement_flag"].alias("measurement_flag"),
            block["quality_flag"].alias("quality_flag"),
            block["source_flag"].alias("source_flag"),
        )
        for block in daily_blocks
    ])

    daily = records.withColumn("daily", explode(daily_array))

    daily = (
        daily
        .withColumn(
            "date",
            to_date(
                concat_ws(
                    "-",
                    col("year").cast("string"),
                    lpad(col("month").cast("string"), 2, "0"),
                    lpad(col("daily.day").cast("string"), 2, "0"),
                ),
                "yyyy-MM-dd",
            ),
        )
        .withColumn("raw_value", col("daily.raw_value"))
        .withColumn("measurement_flag", col("daily.measurement_flag"))
        .withColumn("quality_flag", col("daily.quality_flag"))
        .withColumn("source_flag", col("daily.source_flag"))
        .filter(col("date").isNotNull())
        .filter(col("raw_value").isNotNull())
        .filter(col("raw_value") != -9999)
        .filter(trim(col("quality_flag")) == "")
        .withColumn(
            "value",
            when(col("element").isin("TMAX", "TMIN"), col("raw_value") / 10.0)
            .when(col("element") == "PRCP", col("raw_value") / 10.0),
        )
        .withColumn(
            "unit",
            when(col("element").isin("TMAX", "TMIN"), lit("degrees_C"))
            .when(col("element") == "PRCP", lit("mm")),
        )
        .select(
            "station_id",
            "date",
            "element",
            "value",
            "unit",
            "measurement_flag",
            "source_flag",
        )
    )

    output = PARQUET_DIR / "ghcn_daily"
    daily.write.mode("overwrite").parquet(str(output))

    print(f"GHCN-Daily Parquet saved to: {output}")
    print(f"Valid observation count: {daily.count():,}")
    daily.show(10, truncate=False)
    return True


def prepare_storm_events(spark):
    """Read NOAA Storm Events CSV using a declared Spark schema."""
    files = sorted(STORM_DIR.glob("*.csv.gz"))
    files += sorted(STORM_DIR.glob("*.csv"))

    if not files:
        print(f"No Storm Events CSV files found in {STORM_DIR}")
        return False

    # Read the CSV header, then declare every column as a string.
    # Numeric/date conversion can be handled in a later processing step.
    first_file = files[0]
    open_file = gzip.open if first_file.suffix == ".gz" else open

    with open_file(first_file, "rt", encoding="utf-8-sig", newline="") as stream:
        header = next(csv.reader(stream))

    storm_schema = StructType([
        StructField(name.strip(), StringType(), True)
        for name in header
    ])

    print(f"Reading NOAA Storm Events file: {first_file.name}")

    storm = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(storm_schema)
        .csv([str(path) for path in files])
    )

    output = PARQUET_DIR / "storm_events"
    storm.write.mode("overwrite").parquet(str(output))

    print(f"Storm Events Parquet saved to: {output}")
    print(f"Storm Events row count: {storm.count():,}")
    storm.show(5, truncate=False)
    return True


def main():
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("ExtremeWeatherRawPreparation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        ghcn_ok = prepare_ghcn(spark)
        storm_ok = prepare_storm_events(spark)

        print("\nPreparation summary")
        print(f"GHCN-Daily: {'OK' if ghcn_ok else 'FAILED'}")
        print(f"Storm Events: {'OK' if storm_ok else 'FAILED'}")

        if not (ghcn_ok and storm_ok):
            sys.exit(1)

        print("\nRaw-data preparation completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()