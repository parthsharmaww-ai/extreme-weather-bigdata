
"""
Calculate station-specific extreme-weather thresholds for 1991-2020.

Definitions from docs/event_definitions.md:
- Hot day: TMAX above the 95th percentile.
- Cold day: TMIN below the 5th percentile.
- Extreme rain day: PRCP above the 99th percentile of baseline wet days.
- Wet day: PRCP >= 1 mm.

Percentiles use a +/-7 calendar-day window across the baseline period.
Raw and baseline datasets are not modified.

Missing rainfall thresholds:
- Remain missing when no qualifying baseline wet days exist in a window.
- No fallback threshold is applied.
- Extreme-rain classification is unavailable for dates without a threshold.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofyear,
    explode,
    lit,
    pmod,
    sequence,
    to_date,
    when,
    percentile_approx,
    sum as spark_sum,
)
from pyspark.sql.types import IntegerType


PROJECT_ROOT = Path(__file__).resolve().parent.parent

BASELINE_PATH = (
    PROJECT_ROOT / "data" / "parquet" / "ghcn_baseline_1991_2020"
)

OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "parquet" / "extreme_thresholds_1991_2020"
)

BASELINE_START = "1991-01-01"
BASELINE_END = "2020-12-31"

WINDOW_DAYS = 7
CALENDAR_DAYS = 366
PERCENTILE_ACCURACY = 10000


def calculate_thresholds(spark):
    """Calculate station-specific percentile thresholds."""

    if not BASELINE_PATH.exists():
        print(f"ERROR: Baseline dataset not found: {BASELINE_PATH}")
        print("Run src/prepare_baseline.py first.")
        return False

    print("=" * 65)
    print("EXTREME-WEATHER THRESHOLD CALCULATION")
    print("=" * 65)
    print(f"Baseline: {BASELINE_START} to {BASELINE_END}")
    print(f"Calendar window: +/-{WINDOW_DAYS} days")
    print("Hot-day percentile: 95th")
    print("Cold-day percentile: 5th")
    print("Extreme-rain percentile: 99th of baseline wet days")
    print("Wet-day definition: PRCP >= 1 mm")
    print("Missing rainfall thresholds: no fallback applied")

    baseline = spark.read.parquet(str(BASELINE_PATH))

    completeness_path = (
        PROJECT_ROOT / "data" / "parquet" / "station_completeness_1991_2020"
    )
    if not completeness_path.exists():
        print(f"ERROR: Completeness report not found: {completeness_path}")
        print("Run src/prepare_baseline.py first.")
        return False

    completeness = spark.read.parquet(str(completeness_path))

    eligible_stations = (
        completeness
        .groupBy("station_id")
        .agg(
            spark_sum(
                when(col("meets_80_percent") == "YES", 1).otherwise(0)
            ).alias("passing_variables")
        )
        .filter(col("passing_variables") == 3)
        .select("station_id")
    )

    eligible_station_count = eligible_stations.count()

    print(
        f"Stations meeting 80% completeness for TMAX, TMIN, and PRCP: "
        f"{eligible_station_count}"
    )

    if eligible_station_count == 0:
        print("ERROR: No stations meet the completeness requirement.")
        return False

    baseline = baseline.join(
        eligible_stations,
        on="station_id",
        how="inner",
    )

    # Map each observation to its month/day position in reference
    # leap year 2000, allowing February 29 to have its own position.
    weather = (
        baseline
        .filter(
            col("date").isNotNull()
            & col("value").isNotNull()
            & col("element").isin("TMAX", "TMIN", "PRCP")
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

    # Temperature thresholds use valid temperature observations.
    # Rainfall thresholds use baseline wet days only (PRCP >= 1 mm).
    eligible = weather.filter(
        (col("element").isin("TMAX", "TMIN"))
        | (
            (col("element") == "PRCP")
            & (col("value") >= 1.0)
        )
    )

    # Expand each observation across the target calendar days within
    # +/-7 days, wrapping around the calendar year where necessary.
    expanded = (
        eligible
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
        .drop("offset")
    )

    # Calculate the 95th percentile for TMAX.
    temperature_max = (
        expanded
        .filter(col("element") == "TMAX")
        .groupBy("station_id", "element", "target_calendar_day")
        .agg(
            percentile_approx(
                "value", 0.95, PERCENTILE_ACCURACY
            ).alias("threshold")
        )
    )

    # Calculate the 5th percentile for TMIN.
    temperature_min = (
        expanded
        .filter(col("element") == "TMIN")
        .groupBy("station_id", "element", "target_calendar_day")
        .agg(
            percentile_approx(
                "value", 0.05, PERCENTILE_ACCURACY
            ).alias("threshold")
        )
    )

    # Calculate the 99th percentile for baseline wet-day PRCP.
    # If a window has no wet-day observations, no threshold row is
    # produced for that station and calendar day. No fallback is used.
    extreme_rain = (
        expanded
        .filter(col("element") == "PRCP")
        .groupBy("station_id", "element", "target_calendar_day")
        .agg(
            percentile_approx(
                "value", 0.99, PERCENTILE_ACCURACY
            ).alias("threshold")
        )
    )

    # Combine the three threshold tables and add metadata.
    thresholds = (
        temperature_max
        .unionByName(temperature_min)
        .unionByName(extreme_rain)
        .withColumn(
            "threshold_unit",
            when(
                col("element").isin("TMAX", "TMIN"),
                lit("degrees_C"),
            ).when(
                col("element") == "PRCP",
                lit("mm"),
            ),
        )
        .withColumn(
            "threshold_definition",
            when(
                col("element") == "TMAX",
                lit("95th percentile of baseline TMAX"),
            )
            .when(
                col("element") == "TMIN",
                lit("5th percentile of baseline TMIN"),
            )
            .otherwise(
                lit(
                    "99th percentile of baseline wet-day PRCP (>= 1 mm)"
                )
            ),
        )
        .withColumn("baseline_start", lit(BASELINE_START))
        .withColumn("baseline_end", lit(BASELINE_END))
        .withColumn("window_days_each_side", lit(WINDOW_DAYS))
        .withColumn("calendar_days", lit(CALENDAR_DAYS))
    )

    output_count = thresholds.count()

    if output_count == 0:
        print("ERROR: No thresholds were calculated.")
        return False

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    thresholds.write.mode("overwrite").parquet(str(OUTPUT_PATH))

    print(f"\nThreshold rows calculated: {output_count:,}")
    print(f"Threshold Parquet saved to: {OUTPUT_PATH}")

    print("\nThreshold rows by station and element:")
    thresholds.groupBy("station_id", "element").count().orderBy(
        "station_id", "element"
    ).show(100, truncate=False)

    print("\nSample thresholds:")
    thresholds.select(
        "station_id",
        "element",
        "target_calendar_day",
        "threshold",
        "threshold_unit",
        "threshold_definition",
    ).orderBy(
        "station_id", "element", "target_calendar_day"
    ).show(20, truncate=False)

    print(
        "\nIMPORTANT:"
        "\n- Calendar positions use leap reference year 2000 (366 days)."
        "\n- February 29 has its own calendar position."
        "\n- Rainfall percentiles use wet days only."
        "\n- These thresholds use the completeness-filtered global pilot stations."
        "\n- Validate the results before detecting events."
        "\n- Missing rainfall thresholds remain missing; no fallback is applied."
        "\n- Extreme-rain classification is unavailable for dates without a threshold."
    )

    return True


def main():
    spark = (
        SparkSession.builder
        .appName("ExtremeWeatherThresholdCalculation")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        success = calculate_thresholds(spark)

        if not success:
            sys.exit(1)

        print("\nThreshold calculation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()