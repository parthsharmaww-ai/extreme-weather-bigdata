
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, functions as F


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DAILY_PATH = PROJECT_ROOT / "data" / "parquet" / "ghcn_daily"
BASELINE_PATH = PROJECT_ROOT / "data" / "parquet" / "ghcn_baseline_1991_2020"
THRESHOLD_PATH = PROJECT_ROOT / "data" / "parquet" / "extreme_thresholds_1991_2020"
EVENT_PATH = PROJECT_ROOT / "data" / "parquet" / "extreme_events"
METADATA_PATH = PROJECT_ROOT / "data" / "export" / "station_metadata.csv"

OUTPUT_DIR = PROJECT_ROOT / "data" / "export"
OUTPUT_FILE = OUTPUT_DIR / "station_year_metrics.csv"

# Thresholds are aligned to leap year 2000, which has 366 days.
EXPECTED_THRESHOLD_DAYS = 366


# ============================================================
# HELPER: AGGREGATE EVENTS BY STATION AND START YEAR
# ============================================================

def event_metrics(events, event_type, prefix):
    """
    Calculate event count, total duration, maximum intensity,
    and maximum duration by station and event start year.

    Event metrics are assigned to the year in which the event
    starts, rather than distributed across calendar years.
    """

    selected = events.filter(F.col("event_type") == event_type)

    return (
        selected
        .withColumn("year", F.year("start_date"))
        .groupBy("station_id", "year")
        .agg(
            F.count("*").alias(f"{prefix}_count"),
            F.sum("duration_days").alias(f"{prefix}_days"),
            F.max("intensity_value").alias(f"{prefix}_max_intensity"),
            F.max("duration_days").alias(f"{prefix}_longest_duration"),
        )
    )


# ============================================================
# MAIN
# ============================================================

def main():
    required_paths = [
        DAILY_PATH,
        BASELINE_PATH,
        THRESHOLD_PATH,
        EVENT_PATH,
        METADATA_PATH,
    ]

    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(
                f"Required input not found: {path}"
            )

    spark = (
        SparkSession.builder
        .appName("ExportStationYearMetrics")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # ----------------------------------------------------
        # 1. LOAD INPUT DATA
        # ----------------------------------------------------

        print("Loading daily observations...")
        daily = spark.read.parquet(str(DAILY_PATH))

        print("Loading baseline observations...")
        baseline = spark.read.parquet(str(BASELINE_PATH))

        print("Loading thresholds...")
        thresholds = spark.read.parquet(str(THRESHOLD_PATH))

        print("Loading detected events...")
        events = spark.read.parquet(str(EVENT_PATH))

        print("Loading station metadata...")
        metadata = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(METADATA_PATH))
        )

        # ----------------------------------------------------
        # 2. PIVOT DAILY WEATHER OBSERVATIONS
        # ----------------------------------------------------

        print("Preparing daily weather observations...")

        daily_wide = (
            daily
            .filter(
                F.col("element").isin("TMAX", "TMIN", "PRCP")
            )
            .groupBy("station_id", "date")
            .pivot("element", ["TMAX", "TMIN", "PRCP"])
            .agg(F.first("value"))
        )

        # Map month-day to leap year 2000 for threshold matching.
        # This correctly aligns dates after February in non-leap years.
        daily_wide = (
            daily_wide
            .withColumn(
                "target_calendar_day",
                F.dayofyear(
                    F.to_date(
                        F.concat(
                            F.lit("2000-"),
                            F.date_format("date", "MM-dd"),
                        )
                    )
                ),
            )
            .withColumn("year", F.year("date"))
            .withColumn(
                "days_in_year",
                F.dayofyear(
                    F.to_date(
                        F.concat(
                            F.col("year").cast("string"),
                            F.lit("-12-31"),
                        )
                    )
                ),
            )
        )

        # ----------------------------------------------------
        # 3. PIVOT DAILY THRESHOLDS
        # ----------------------------------------------------

        print("Preparing daily thresholds...")

        threshold_wide = (
            thresholds
            .filter(
                F.col("element").isin("TMAX", "TMIN", "PRCP")
            )
            .groupBy("station_id", "target_calendar_day")
            .pivot("element", ["TMAX", "TMIN", "PRCP"])
            .agg(F.first("threshold"))
        )

        threshold_wide = threshold_wide.select(
            "station_id",
            "target_calendar_day",
            F.col("TMAX").alias("TMAX_threshold"),
            F.col("TMIN").alias("TMIN_threshold"),
            F.col("PRCP").alias("PRCP_threshold"),
        )

        # Count available threshold days separately for each variable.
        # An annual count will be reported only if all 366 thresholds
        # for that variable are available.
        threshold_coverage = (
            threshold_wide
            .groupBy("station_id")
            .agg(
                F.count("TMAX_threshold").alias(
                    "_tmax_threshold_days"
                ),
                F.count("TMIN_threshold").alias(
                    "_tmin_threshold_days"
                ),
                F.count("PRCP_threshold").alias(
                    "_prcp_threshold_days"
                ),
            )
            .withColumn(
                "_tmax_threshold_complete",
                F.col("_tmax_threshold_days")
                == EXPECTED_THRESHOLD_DAYS,
            )
            .withColumn(
                "_tmin_threshold_complete",
                F.col("_tmin_threshold_days")
                == EXPECTED_THRESHOLD_DAYS,
            )
            .withColumn(
                "_prcp_threshold_complete",
                F.col("_prcp_threshold_days")
                == EXPECTED_THRESHOLD_DAYS,
            )
        )

        # A station appearing in the threshold table has passed
        # the threshold-generation eligibility filter.
        threshold_station_ids = (
            threshold_wide
            .select("station_id")
            .distinct()
            .withColumn("_has_threshold_station", F.lit(True))
        )

        daily_joined = daily_wide.join(
            threshold_wide,
            on=["station_id", "target_calendar_day"],
            how="left",
        )

        # ----------------------------------------------------
        # 4. DAILY WEATHER METRICS BY STATION AND YEAR
        # ----------------------------------------------------

        print("Calculating station-year weather metrics...")

        complete_day = (
            F.col("TMAX").isNotNull()
            & F.col("TMIN").isNotNull()
            & F.col("PRCP").isNotNull()
        )

        daily_metrics = (
            daily_joined
            .groupBy("station_id", "year")
            .agg(
                F.max("days_in_year").alias("_days_in_year"),

                # Strict completeness: all three variables observed
                # on the same calendar day.
                F.sum(
                    F.when(complete_day, 1).otherwise(0)
                ).alias("_complete_days"),

                # Threshold-based counts are initially calculated
                # only where both the observation and threshold exist.
                F.sum(
                    F.when(
                        F.col("TMAX").isNotNull()
                        & F.col("TMAX_threshold").isNotNull()
                        & (
                            F.col("TMAX")
                            > F.col("TMAX_threshold")
                        ),
                        1,
                    ).otherwise(0)
                ).alias("_hot_days_raw"),

                F.sum(
                    F.when(
                        F.col("TMIN").isNotNull()
                        & F.col("TMIN_threshold").isNotNull()
                        & (
                            F.col("TMIN")
                            < F.col("TMIN_threshold")
                        ),
                        1,
                    ).otherwise(0)
                ).alias("_cold_days_raw"),

                F.sum(
                    F.when(
                        F.col("PRCP").isNotNull()
                        & F.col("PRCP_threshold").isNotNull()
                        & (
                            F.col("PRCP")
                            > F.col("PRCP_threshold")
                        ),
                        1,
                    ).otherwise(0)
                ).alias("_extreme_rain_days_raw"),

                # Directly observed precipitation metrics.
                F.max("PRCP").alias("max_daily_prcp_mm"),
                F.sum("PRCP").alias("prcp_total_mm"),

                # Annual mean TMAX based on available TMAX values.
                F.avg("TMAX").alias("_annual_mean_tmax"),
            )
            .withColumn(
                "days_present_pct",
                F.round(
                    F.col("_complete_days")
                    / F.col("_days_in_year")
                    * 100.0,
                    2,
                ),
            )
        )

        # Apply threshold completeness rules.
        # Missing or incomplete threshold coverage produces null,
        # not a misleading zero event-day count.
        daily_metrics = (
            daily_metrics
            .join(
                threshold_coverage,
                on="station_id",
                how="left",
            )
            .withColumn(
                "hot_days",
                F.when(
                    F.col("_tmax_threshold_complete") == True,
                    F.col("_hot_days_raw"),
                ).otherwise(F.lit(None).cast("long")),
            )
            .withColumn(
                "cold_days",
                F.when(
                    F.col("_tmin_threshold_complete") == True,
                    F.col("_cold_days_raw"),
                ).otherwise(F.lit(None).cast("long")),
            )
            .withColumn(
                "extreme_rain_days",
                F.when(
                    F.col("_prcp_threshold_complete") == True,
                    F.col("_extreme_rain_days_raw"),
                ).otherwise(F.lit(None).cast("long")),
            )
            .drop(
                "_days_in_year",
                "_complete_days",
                "_hot_days_raw",
                "_cold_days_raw",
                "_extreme_rain_days_raw",
                "_tmax_threshold_days",
                "_tmin_threshold_days",
                "_prcp_threshold_days",
                "_tmax_threshold_complete",
                "_tmin_threshold_complete",
                "_prcp_threshold_complete",
            )
        )

        # ----------------------------------------------------
        # 5. BASELINE TEMPERATURE ANOMALY
        # ----------------------------------------------------

        print("Calculating baseline temperature anomalies...")

        baseline_tmax = (
            baseline
            .filter(F.col("element") == "TMAX")
            .groupBy("station_id")
            .agg(
                F.avg("value").alias("_baseline_mean_tmax")
            )
        )

        daily_metrics = (
            daily_metrics
            .join(
                baseline_tmax,
                on="station_id",
                how="left",
            )
            .withColumn(
                "tmax_anomaly_mean",
                F.round(
                    F.col("_annual_mean_tmax")
                    - F.col("_baseline_mean_tmax"),
                    3,
                ),
            )
            .drop(
                "_annual_mean_tmax",
                "_baseline_mean_tmax",
            )
        )

        # ----------------------------------------------------
        # 6. EVENT METRICS
        # ----------------------------------------------------

        print("Aggregating detected events by start year...")

        heatwave = event_metrics(
            events, "HEATWAVE", "heatwave"
        ).select(
            "station_id",
            "year",
            "heatwave_count",
            "heatwave_days",
            "heatwave_max_intensity",
        )

        cold_snap = event_metrics(
            events, "COLD_SNAP", "coldsnap"
        ).select(
            "station_id",
            "year",
            "coldsnap_count",
            "coldsnap_days",
        )

        dry_spell = event_metrics(
            events, "DRY_SPELL", "dry_spell"
        ).select(
            "station_id",
            "year",
            "dry_spell_count",
            "dry_spell_days",
            F.col("dry_spell_longest_duration").alias(
                "longest_dry_spell"
            ),
        )

        compound = event_metrics(
            events,
            "COMPOUND_HEATWAVE_DRY_SPELL",
            "compound",
        ).select(
            "station_id",
            "year",
            "compound_count",
            "compound_days",
        )

        result = daily_metrics

        for event_table in [
            heatwave,
            cold_snap,
            dry_spell,
            compound,
        ]:
            result = result.join(
                event_table,
                on=["station_id", "year"],
                how="left",
            )

        # Event metrics should be zero only for stations included
        # in the threshold station set. Stations without thresholds
        # have unknown event counts and therefore receive nulls.
        result = result.join(
            threshold_station_ids,
            on="station_id",
            how="left",
        )

        event_metric_columns = [
            "heatwave_count",
            "heatwave_days",
            "coldsnap_count",
            "coldsnap_days",
            "dry_spell_count",
            "dry_spell_days",
            "longest_dry_spell",
            "compound_count",
            "compound_days",
        ]

        for column_name in event_metric_columns:
            result = result.withColumn(
                column_name,
                F.when(
                    F.col("_has_threshold_station") == True,
                    F.coalesce(
                        F.col(column_name),
                        F.lit(0),
                    ),
                ).otherwise(F.lit(None).cast("long")),
            )

        # Heatwave intensity remains null when no heatwave intensity
        # was recorded. Its interpretation is still provisional.

        result = result.drop("_has_threshold_station")

        # Use the dashboard's requested column name.
        result = result.withColumnRenamed(
            "compound_count",
            "compound_event_count",
        )

        # ----------------------------------------------------
        # 7. JOIN STATION METADATA
        # ----------------------------------------------------

        print("Joining station metadata...")

        metadata = metadata.select(
            "station_id",
            "station_name",
            "lat",
            "lon",
            "elevation_m",
        )

        result = result.join(
            metadata,
            on="station_id",
            how="left",
        )

        # Region/country/continent mapping has not been verified.
        # Leave these fields null rather than inventing geography.
        result = (
            result
            .withColumn(
                "region_name",
                F.lit(None).cast("string"),
            )
            .withColumn(
                "country",
                F.lit(None).cast("string"),
            )
            .withColumn(
                "continent",
                F.lit(None).cast("string"),
            )
        )

        # ----------------------------------------------------
        # 8. SELECT OUTPUT COLUMNS
        # ----------------------------------------------------

        output_columns = [
            "station_id",
            "station_name",
            "lat",
            "lon",
            "elevation_m",
            "region_name",
            "country",
            "continent",
            "year",
            "days_present_pct",
            "hot_days",
            "heatwave_count",
            "heatwave_days",
            "heatwave_max_intensity",
            "cold_days",
            "coldsnap_count",
            "coldsnap_days",
            "extreme_rain_days",
            "max_daily_prcp_mm",
            "prcp_total_mm",
            "dry_spell_count",
            "dry_spell_days",
            "longest_dry_spell",
            "compound_event_count",
            "compound_days",
            "tmax_anomaly_mean",
        ]

        result = result.select(*output_columns)

        # ----------------------------------------------------
        # 9. VALIDATE OUTPUT
        # ----------------------------------------------------

        print("Validating station-year output...")

        duplicate_count = (
            result
            .groupBy("station_id", "year")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_count > 0:
            raise ValueError(
                f"Found {duplicate_count} duplicate station-year keys."
            )

        invalid_completeness = result.filter(
            F.col("days_present_pct").isNotNull()
            & (
                (F.col("days_present_pct") < 0)
                | (F.col("days_present_pct") > 100)
            )
        ).count()

        if invalid_completeness > 0:
            raise ValueError(
                f"Found {invalid_completeness} invalid completeness values."
            )

        # Confirm one row per station-year and non-empty output.
        output_pdf = result.orderBy(
            "station_id", "year"
        ).toPandas()

        if output_pdf.empty:
            raise ValueError("Station-year export is empty.")

        expected_columns = output_columns
        if list(output_pdf.columns) != expected_columns:
            raise ValueError(
                "Output columns do not match the expected dashboard schema."
            )

        # ----------------------------------------------------
        # 10. EXPORT CSV
        # ----------------------------------------------------

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_pdf.to_csv(OUTPUT_FILE, index=False)

        print("\nStation-year export completed successfully.")
        print(f"Output file: {OUTPUT_FILE}")
        print(f"Rows exported: {len(output_pdf):,}")
        print(
            "Unique stations: "
            f"{output_pdf['station_id'].nunique():,}"
        )
        print(
            f"Year range: {output_pdf['year'].min()} "
            f"to {output_pdf['year'].max()}"
        )
        print(f"Duplicate station-year keys: {duplicate_count}")
        print(f"Invalid completeness values: {invalid_completeness}")

        print("\nThreshold-based metric availability:")
        for column_name in [
            "hot_days",
            "cold_days",
            "extreme_rain_days",
        ]:
            print(
                f"{column_name}: "
                f"{output_pdf[column_name].notna().sum():,} "
                "non-null station-year rows"
            )

        print("\nEvent metric availability:")
        for column_name in [
            "heatwave_count",
            "coldsnap_count",
            "dry_spell_count",
            "compound_event_count",
        ]:
            print(
                f"{column_name}: "
                f"{output_pdf[column_name].notna().sum():,} "
                "non-null station-year rows"
            )

        print("\nOutput columns:")
        print(", ".join(output_pdf.columns))

        print("\nFirst five rows:")
        print(output_pdf.head(5).to_string(index=False))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()