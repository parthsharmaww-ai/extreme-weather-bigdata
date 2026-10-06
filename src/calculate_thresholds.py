"""
Calculate station-specific extreme-weather thresholds for 1991-2020.

Definitions from docs/event_definitions.md (v2):
- Warm day: TMAX above that calendar day's 95th percentile (season-relative).
- Cold day: TMIN below that calendar day's 5th percentile.
- Heavy rain day: PRCP above the station's annual 95th percentile of
  baseline wet days.
- Wet day: PRCP >= 1 mm.

Temperature percentiles use a +/-7 calendar-day window across the baseline
period. Rain uses ONE annual threshold per station, repeated on all 366
calendar days so the event-detection join does not change.
Raw and baseline datasets are not modified.
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
RAIN_PERCENTILE = 0.95
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
    print(f"Temperature calendar window: +/-{WINDOW_DAYS} days")
    print("Warm-day percentile: 95th (TMAX)")
    print("Cold-day percentile: 5th (TMIN)")
    print("Heavy-rain percentile: annual 95th of baseline wet days")
    print("Wet-day definition: PRCP >= 1 mm")

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

    # Station baseline eligibility also needs at least 25 valid baseline
    # years (src/check_station_eligibility.py).
    eligibility_path = (
        PROJECT_ROOT / "data" / "parquet" / "station_eligibility"
    )
    if eligibility_path.exists():
        baseline_eligible = (
            spark.read.parquet(str(eligibility_path))
            .filter(col("baseline_eligible"))
            .select("station_id")
        )
        eligible_stations = eligible_stations.join(
            baseline_eligible, on="station_id", how="inner"
        )
    else:
        print(
            "WARNING: station_eligibility not found, so the 25-of-30 "
            "valid-years rule is NOT applied. "
            "Run src/check_station_eligibility.py first."
        )

    eligible_station_count = eligible_stations.count()

    print(
        f"Stations eligible for thresholds (80% rule per variable, plus "
        f"25 of 30 valid baseline years): {eligible_station_count}"
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

    # Annual p95 of baseline wet days (PRCP >= 1 mm), one value per station,
    # repeated on all 366 calendar days so the event-detection join does not
    # change.
    annual_rain = (
        weather
        .filter((col("element") == "PRCP") & (col("value") >= 1.0))
        .groupBy("station_id", "element")
        .agg(
            percentile_approx(
                "value", RAIN_PERCENTILE, PERCENTILE_ACCURACY
            ).alias("threshold")
        )
    )

    calendar = spark.range(1, CALENDAR_DAYS + 1).select(
        col("id").cast(IntegerType()).alias("target_calendar_day")
    )

    extreme_rain = annual_rain.crossJoin(calendar).select(
        "station_id", "element", "target_calendar_day", "threshold"
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
                    "95th percentile of baseline wet-day PRCP (>= 1 mm), "
                    "annual per station"
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
        "\n- Rain thresholds are annual per station: the same value is"
        " repeated on every calendar day."
        "\n- Temperature thresholds use the +/-7-day window."
        "\n- These thresholds use the completeness-filtered pilot stations."
        "\n- Validate the results before detecting events."
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