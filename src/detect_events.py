
"""
Detect extreme-weather events using station-specific thresholds.

Provisional intensity measures are documented in docs/event_definitions.md.
This script does not modify raw data, baseline data, or thresholds.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    date_sub,
    datediff,
    dayofyear,
    explode,
    first,
    greatest,
    lag,
    least,
    lit,
    max as spark_max,
    min as spark_min,
    pmod,
    sequence,
    sum as spark_sum,
    to_date,
    when,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent

WEATHER_PATH = PROJECT_ROOT / "data" / "parquet" / "ghcn_daily"
THRESHOLD_PATH = (
    PROJECT_ROOT / "data" / "parquet" / "extreme_thresholds_1991_2020"
)
OUTPUT_PATH = PROJECT_ROOT / "data" / "parquet" / "extreme_events"

HOT_MIN_DAYS = 3
COLD_MIN_DAYS = 3
DRY_MIN_DAYS = 10
WET_DAY_MM = 1.0
CALENDAR_DAYS = 366


def detect_runs(
    daily,
    flag_column,
    intensity_column,
    event_type,
    intensity_measure,
    minimum_days,
    intensity_aggregation="avg",
):
    """Identify consecutive event runs, breaking sequences at missing dates."""

    window = Window.partitionBy("station_id").orderBy("date")

    marked = (
        daily
        .withColumn("_previous_flag", lag(col(flag_column)).over(window))
        .withColumn("_previous_date", lag(col("date")).over(window))
        .withColumn(
            "_new_run",
            when(
                (col(flag_column) == 1)
                & (
                    col("_previous_flag").isNull()
                    | (col("_previous_flag") != 1)
                    | (
                        date_sub(col("date"), 1)
                        != col("_previous_date")
                    )
                ),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "_run_id",
            spark_sum(col("_new_run")).over(
                window.rowsBetween(
                    Window.unboundedPreceding,
                    Window.currentRow,
                )
            ),
        )
        .filter(col(flag_column) == 1)
    )

    if intensity_aggregation == "sum":
        intensity_expression = spark_sum(col(intensity_column))
    elif intensity_aggregation == "max":
        intensity_expression = spark_max(col(intensity_column))
    elif intensity_aggregation == "duration":
        intensity_expression = count(lit(1))
    else:
        intensity_expression = avg(col(intensity_column))

    events = (
        marked
        .groupBy("station_id", "_run_id")
        .agg(
            spark_min("date").alias("start_date"),
            spark_max("date").alias("end_date"),
            count(lit(1)).alias("duration_days"),
            intensity_expression.alias("intensity_value"),
        )
        .filter(col("duration_days") >= minimum_days)
        .select(
            "station_id",
            lit(event_type).alias("event_type"),
            "start_date",
            "end_date",
            "duration_days",
            col("intensity_value").cast("double").alias("intensity_value"),
            lit(intensity_measure).alias("intensity_measure"),
        )
    )

    return events


def main():
    spark = (
        SparkSession.builder
        .appName("ExtremeWeatherEventDetection")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not WEATHER_PATH.exists():
            print(f"ERROR: Daily weather dataset not found: {WEATHER_PATH}")
            print("Run src/prepare_raw.py first.")
            return False

        if not THRESHOLD_PATH.exists():
            print(f"ERROR: Threshold dataset not found: {THRESHOLD_PATH}")
            print("Run src/calculate_thresholds.py first.")
            return False

        print("=" * 65)
        print("EXTREME-WEATHER EVENT DETECTION")
        print("=" * 65)
        print(f"Heatwave minimum duration: {HOT_MIN_DAYS} days")
        print(f"Cold snap minimum duration: {COLD_MIN_DAYS} days")
        print(f"Dry spell minimum duration: {DRY_MIN_DAYS} days")
        print(f"Wet-day threshold: {WET_DAY_MM} mm")
        print("Missing observations break consecutive-day sequences.")

        raw = spark.read.parquet(str(WEATHER_PATH))

        # Create one row per station and date, with separate weather columns.
        daily = (
            raw
            .filter(
                col("station_id").isNotNull()
                & col("date").isNotNull()
                & col("element").isin("TMAX", "TMIN", "PRCP")
                & col("value").isNotNull()
            )
            .select(
                "station_id",
                to_date(col("date")).alias("date"),
                "element",
                "value",
            )
            .groupBy("station_id", "date")
            .pivot("element", ["TMAX", "TMIN", "PRCP"])
            .agg(first("value"))
        )

        thresholds = spark.read.parquet(str(THRESHOLD_PATH))

        # Convert the 366-day threshold calendar into columns for joining.
        threshold_calendar = (
            thresholds
            .groupBy("station_id", "target_calendar_day")
            .pivot("element", ["TMAX", "TMIN", "PRCP"])
            .agg(first("threshold"))
            .withColumnRenamed("TMAX", "tmax_threshold")
            .withColumnRenamed("TMIN", "tmin_threshold")
            .withColumnRenamed("PRCP", "prcp_threshold")
        )

        daily = (
            daily
            .withColumn(
                "calendar_date",
                to_date(
                    date_format(col("date"), "'2000-'MM-dd"),
                    "yyyy-MM-dd",
                ),
            )
            .withColumn(
                "calendar_day",
                dayofyear(col("calendar_date")),
            )
            .join(
                threshold_calendar.withColumnRenamed(
                    "target_calendar_day", "calendar_day"
                ),
                on=["station_id", "calendar_day"],
                how="inner",
            )
            .drop("calendar_date", "calendar_day")
        )

        # Daily flags use strict comparisons, as defined in the documentation.
        daily = (
            daily
            .withColumn(
                "hot_flag",
                when(
                    col("TMAX").isNotNull()
                    & col("tmax_threshold").isNotNull()
                    & (col("TMAX") > col("tmax_threshold")),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "cold_flag",
                when(
                    col("TMIN").isNotNull()
                    & col("tmin_threshold").isNotNull()
                    & (col("TMIN") < col("tmin_threshold")),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "dry_flag",
                when(
                    col("PRCP").isNotNull()
                    & (col("PRCP") < WET_DAY_MM),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "rain_flag",
                when(
                    col("PRCP").isNotNull()
                    & col("prcp_threshold").isNotNull()
                    & (col("PRCP") > col("prcp_threshold")),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "heat_excess",
                when(
                    col("hot_flag") == 1,
                    col("TMAX") - col("tmax_threshold"),
                ),
            )
            .withColumn(
                "cold_deficit",
                when(
                    col("cold_flag") == 1,
                    col("tmin_threshold") - col("TMIN"),
                ),
            )
            .withColumn(
                "rain_excess",
                when(
                    col("rain_flag") == 1,
                    col("PRCP") - col("prcp_threshold"),
                ),
            )
            .withColumn(
                "rain_deficit",
                when(
                    col("dry_flag") == 1,
                    lit(WET_DAY_MM) - col("PRCP"),
                ),
            )
        )

        # Qualifying multi-day events.
        heatwaves = detect_runs(
            daily, "hot_flag", "heat_excess",
            "HEATWAVE", "max TMAX exceedance (degrees_C)",
            HOT_MIN_DAYS, intensity_aggregation="max",
        )

        cold_snaps = detect_runs(
            daily, "cold_flag", "cold_deficit",
            "COLD_SNAP", "max TMIN deficit (degrees_C)",
            COLD_MIN_DAYS, intensity_aggregation="max",
        )

        dry_spells = detect_runs(
            daily, "dry_flag", "dry_flag",
            "DRY_SPELL", "duration (days)",
            DRY_MIN_DAYS, intensity_aggregation="duration",
        )

        # Each heavy-rain day is recorded as a one-day event.
        heavy_rain = (
            daily
            .filter(col("rain_flag") == 1)
            .select(
                "station_id",
                lit("HEAVY_RAIN").alias("event_type"),
                col("date").alias("start_date"),
                col("date").alias("end_date"),
                lit(1).alias("duration_days"),
                col("rain_excess").cast("double").alias("intensity_value"),
                lit("rainfall excess (mm)").alias("intensity_measure"),
            )
        )

        # A compound event occurs where a qualifying heatwave and
        # qualifying dry spell overlap at the same station.
        h = heatwaves.alias("h")
        d = dry_spells.alias("d")

        overlap_days = (
            datediff(
                least(col("h.end_date"), col("d.end_date")),
                greatest(col("h.start_date"), col("d.start_date")),
            )
            + lit(1)
        )

        compound_events = (
            h.join(
                d,
                (col("h.station_id") == col("d.station_id"))
                & (col("h.start_date") <= col("d.end_date"))
                & (col("d.start_date") <= col("h.end_date")),
                "inner",
            )
            .select(
                col("h.station_id").alias("station_id"),
                lit("COMPOUND_HEATWAVE_DRY_SPELL").alias("event_type"),
                greatest(
                    col("h.start_date"), col("d.start_date")
                ).alias("start_date"),
                least(
                    col("h.end_date"), col("d.end_date")
                ).alias("end_date"),
                overlap_days.alias("duration_days"),
                overlap_days.cast("double").alias("intensity_value"),
                lit("overlap duration (days)").alias("intensity_measure"),
            )
        )

        all_events = (
            heatwaves
            .unionByName(cold_snaps)
            .unionByName(dry_spells)
            .unionByName(heavy_rain)
            .unionByName(compound_events)
        )

        total_events = all_events.count()

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        all_events.write.mode("overwrite").partitionBy(
            "event_type"
        ).parquet(str(OUTPUT_PATH))

        print(f"\nTotal detected events: {total_events:,}")
        print(f"Output saved to: {OUTPUT_PATH}")

        print("\nEvents by type:")
        all_events.groupBy("event_type").count().orderBy(
            "event_type"
        ).show(truncate=False)

        print("\nSample detected events:")
        all_events.orderBy(
            "station_id", "start_date", "event_type"
        ).show(20, truncate=False)

        print(
            "\nIMPORTANT:"
            "\n- Thresholds come from the validated 1991-2020 baseline."
            "\n- Event detection uses available daily observations."
            "\n- Missing dates break consecutive-day runs."
            "\n- Heavy-rain threshold is the annual station p95 of baseline wet days."
            "\n- Intensity measures remain provisional pending team review."
        )

        return True

    finally:
        spark.stop()


if __name__ == "__main__":
    if not main():
        sys.exit(1)
"""
Detect extreme-weather events using station-specific thresholds.

Provisional intensity measures are documented in docs/event_definitions.md.
This script does not modify raw data, baseline data, or thresholds.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    date_sub,
    datediff,
    dayofyear,
    explode,
    first,
    greatest,
    lag,
    least,
    lit,
    max as spark_max,
    min as spark_min,
    pmod,
    sequence,
    sum as spark_sum,
    to_date,
    when,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent

WEATHER_PATH = PROJECT_ROOT / "data" / "parquet" / "ghcn_daily"
THRESHOLD_PATH = (
    PROJECT_ROOT / "data" / "parquet" / "extreme_thresholds_1991_2020"
)
OUTPUT_PATH = PROJECT_ROOT / "data" / "parquet" / "extreme_events"

HOT_MIN_DAYS = 3
COLD_MIN_DAYS = 3
DRY_MIN_DAYS = 10
WET_DAY_MM = 1.0
CALENDAR_DAYS = 366


def detect_runs(
    daily,
    flag_column,
    intensity_column,
    event_type,
    intensity_measure,
    minimum_days,
    intensity_aggregation="avg",
):
    """Identify consecutive event runs, breaking sequences at missing dates."""

    window = Window.partitionBy("station_id").orderBy("date")

    marked = (
        daily
        .withColumn("_previous_flag", lag(col(flag_column)).over(window))
        .withColumn("_previous_date", lag(col("date")).over(window))
        .withColumn(
            "_new_run",
            when(
                (col(flag_column) == 1)
                & (
                    col("_previous_flag").isNull()
                    | (col("_previous_flag") != 1)
                    | (
                        date_sub(col("date"), 1)
                        != col("_previous_date")
                    )
                ),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "_run_id",
            spark_sum(col("_new_run")).over(
                window.rowsBetween(
                    Window.unboundedPreceding,
                    Window.currentRow,
                )
            ),
        )
        .filter(col(flag_column) == 1)
    )

    if intensity_aggregation == "sum":
        intensity_expression = spark_sum(col(intensity_column))
    elif intensity_aggregation == "max":
        intensity_expression = spark_max(col(intensity_column))
    else:
        intensity_expression = avg(col(intensity_column))

    events = (
        marked
        .groupBy("station_id", "_run_id")
        .agg(
            spark_min("date").alias("start_date"),
            spark_max("date").alias("end_date"),
            count(lit(1)).alias("duration_days"),
            intensity_expression.alias("intensity_value"),
        )
        .filter(col("duration_days") >= minimum_days)
        .select(
            "station_id",
            lit(event_type).alias("event_type"),
            "start_date",
            "end_date",
            "duration_days",
            col("intensity_value").cast("double").alias("intensity_value"),
            lit(intensity_measure).alias("intensity_measure"),
        )
    )

    return events


def main():
    spark = (
        SparkSession.builder
        .appName("ExtremeWeatherEventDetection")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not WEATHER_PATH.exists():
            print(f"ERROR: Daily weather dataset not found: {WEATHER_PATH}")
            print("Run src/prepare_raw.py first.")
            return False

        if not THRESHOLD_PATH.exists():
            print(f"ERROR: Threshold dataset not found: {THRESHOLD_PATH}")
            print("Run src/calculate_thresholds.py first.")
            return False

        print("=" * 65)
        print("EXTREME-WEATHER EVENT DETECTION")
        print("=" * 65)
        print(f"Heatwave minimum duration: {HOT_MIN_DAYS} days")
        print(f"Cold snap minimum duration: {COLD_MIN_DAYS} days")
        print(f"Dry spell minimum duration: {DRY_MIN_DAYS} days")
        print(f"Wet-day threshold: {WET_DAY_MM} mm")
        print("Missing observations break consecutive-day sequences.")

        raw = spark.read.parquet(str(WEATHER_PATH))

        # Create one row per station and date, with separate weather columns.
        daily = (
            raw
            .filter(
                col("station_id").isNotNull()
                & col("date").isNotNull()
                & col("element").isin("TMAX", "TMIN", "PRCP")
                & col("value").isNotNull()
            )
            .select(
                "station_id",
                to_date(col("date")).alias("date"),
                "element",
                "value",
            )
            .groupBy("station_id", "date")
            .pivot("element", ["TMAX", "TMIN", "PRCP"])
            .agg(first("value"))
        )

        thresholds = spark.read.parquet(str(THRESHOLD_PATH))

        # Convert the 366-day threshold calendar into columns for joining.
        threshold_calendar = (
            thresholds
            .groupBy("station_id", "target_calendar_day")
            .pivot("element", ["TMAX", "TMIN", "PRCP"])
            .agg(first("threshold"))
            .withColumnRenamed("TMAX", "tmax_threshold")
            .withColumnRenamed("TMIN", "tmin_threshold")
            .withColumnRenamed("PRCP", "prcp_threshold")
        )

        daily = (
            daily
            .withColumn(
                "calendar_date",
                to_date(
                    date_format(col("date"), "'2000-'MM-dd"),
                    "yyyy-MM-dd",
                ),
            )
            .withColumn(
                "calendar_day",
                dayofyear(col("calendar_date")),
            )
            .join(
                threshold_calendar,
                (daily["station_id"] == threshold_calendar["station_id"])
                & (
                    col("calendar_day")
                    == threshold_calendar["target_calendar_day"]
                ),
                "inner",
            )
            .drop(threshold_calendar["station_id"])
            .drop("target_calendar_day", "calendar_date", "calendar_day")
        )

        # Daily flags use strict comparisons, as defined in the documentation.
        daily = (
            daily
            .withColumn(
                "hot_flag",
                when(
                    col("TMAX").isNotNull()
                    & col("tmax_threshold").isNotNull()
                    & (col("TMAX") > col("tmax_threshold")),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "cold_flag",
                when(
                    col("TMIN").isNotNull()
                    & col("tmin_threshold").isNotNull()
                    & (col("TMIN") < col("tmin_threshold")),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "dry_flag",
                when(
                    col("PRCP").isNotNull()
                    & (col("PRCP") < WET_DAY_MM),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "rain_flag",
                when(
                    col("PRCP").isNotNull()
                    & col("prcp_threshold").isNotNull()
                    & (col("PRCP") > col("prcp_threshold")),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "heat_excess",
                when(
                    col("hot_flag") == 1,
                    col("TMAX") - col("tmax_threshold"),
                ),
            )
            .withColumn(
                "cold_deficit",
                when(
                    col("cold_flag") == 1,
                    col("tmin_threshold") - col("TMIN"),
                ),
            )
            .withColumn(
                "rain_excess",
                when(
                    col("rain_flag") == 1,
                    col("PRCP") - col("prcp_threshold"),
                ),
            )
            .withColumn(
                "rain_deficit",
                when(
                    col("dry_flag") == 1,
                    lit(WET_DAY_MM) - col("PRCP"),
                ),
            )
        )

        # Qualifying multi-day events.
        heatwaves = detect_runs(
            daily, "hot_flag", "heat_excess",
            "HEATWAVE", "max TMAX exceedance (degrees_C)",
            HOT_MIN_DAYS, intensity_aggregation="max",
        )

        cold_snaps = detect_runs(
            daily, "cold_flag", "cold_deficit",
            "COLD_SNAP", "max TMIN deficit (degrees_C)",
            COLD_MIN_DAYS, intensity_aggregation="max",
        )

        dry_spells = detect_runs(
            daily, "dry_flag", "rain_deficit",
            "DRY_SPELL", "cumulative rainfall deficit (mm)",
            DRY_MIN_DAYS, intensity_aggregation="sum",
        )

        # Each heavy-rain day is recorded as a one-day event.
        heavy_rain = (
            daily
            .filter(col("rain_flag") == 1)
            .select(
                "station_id",
                lit("HEAVY_RAIN").alias("event_type"),
                col("date").alias("start_date"),
                col("date").alias("end_date"),
                lit(1).alias("duration_days"),
                col("rain_excess").cast("double").alias("intensity_value"),
                lit("rainfall excess (mm)").alias("intensity_measure"),
            )
        )

        # A compound event occurs where a qualifying heatwave and
        # qualifying dry spell overlap at the same station.
        h = heatwaves.alias("h")
        d = dry_spells.alias("d")

        overlap_days = (
            datediff(
                least(col("h.end_date"), col("d.end_date")),
                greatest(col("h.start_date"), col("d.start_date")),
            )
            + lit(1)
        )

        compound_events = (
            h.join(
                d,
                (col("h.station_id") == col("d.station_id"))
                & (col("h.start_date") <= col("d.end_date"))
                & (col("d.start_date") <= col("h.end_date")),
                "inner",
            )
            .select(
                col("h.station_id").alias("station_id"),
                lit("COMPOUND_HEATWAVE_DRY_SPELL").alias("event_type"),
                greatest(
                    col("h.start_date"), col("d.start_date")
                ).alias("start_date"),
                least(
                    col("h.end_date"), col("d.end_date")
                ).alias("end_date"),
                overlap_days.alias("duration_days"),
                overlap_days.cast("double").alias("intensity_value"),
                lit("overlap duration (days)").alias("intensity_measure"),
            )
        )

        all_events = (
            heatwaves
            .unionByName(cold_snaps)
            .unionByName(dry_spells)
            .unionByName(heavy_rain)
            .unionByName(compound_events)
        )

        total_events = all_events.count()

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        all_events.write.mode("overwrite").partitionBy(
            "event_type"
        ).parquet(str(OUTPUT_PATH))

        print(f"\nTotal detected events: {total_events:,}")
        print(f"Output saved to: {OUTPUT_PATH}")

        print("\nEvents by type:")
        all_events.groupBy("event_type").count().orderBy(
            "event_type"
        ).show(truncate=False)

        print("\nSample detected events:")
        all_events.orderBy(
            "station_id", "start_date", "event_type"
        ).show(20, truncate=False)

        print(
            "\nIMPORTANT:"
            "\n- Thresholds come from the validated 1991-2020 baseline."
            "\n- Event detection uses available daily observations."
            "\n- Missing dates break consecutive-day runs."
            "\n- Heavy-rain threshold is the annual station p95 of baseline wet days."
            "\n- Intensity measures remain provisional pending team review."
        )

        return True

    finally:
        spark.stop()


if __name__ == "__main__":
    if not main():
        sys.exit(1)
