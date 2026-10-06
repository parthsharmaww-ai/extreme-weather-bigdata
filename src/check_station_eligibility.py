"""
Station baseline eligibility and valid-year counts.

Rules from docs/event_definitions.md (v2), section 2:

- Valid year: days_present_pct >= 90, where days_present_pct is the share of
  days in the year on which TMAX, TMIN and PRCP are all present on the same
  day. This is the same definition (and the same 2-decimal rounding) as
  days_present_pct in the station-year export. A valid year belongs to the
  station, not to a single variable.
- Station baseline eligibility, two conditions:
    1. each variable has at least 80% of the 10,958 baseline days
       (from prepare_baseline.py), and
    2. at least 25 of the 30 baseline years (1991-2020) are valid years.
- Trend eligibility needs at least 30 valid years inside 1991-2025. This
  script reports that count; the trend script applies the rule.

Output:
- data/parquet/station_eligibility (read by calculate_thresholds.py)
- data/export/station_eligibility.csv

Raw, baseline and threshold datasets are not modified.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DAILY_PATH = PROJECT_ROOT / "data" / "parquet" / "ghcn_daily"
COMPLETENESS_PATH = (
    PROJECT_ROOT / "data" / "parquet" / "station_completeness_1991_2020"
)
OUTPUT_PATH = PROJECT_ROOT / "data" / "parquet" / "station_eligibility"
CSV_PATH = PROJECT_ROOT / "data" / "export" / "station_eligibility.csv"

VALID_YEAR_PCT = 90.0

BASELINE_FIRST_YEAR = 1991
BASELINE_LAST_YEAR = 2020
MIN_BASELINE_VALID_YEARS = 25

TREND_FIRST_YEAR = 1991
TREND_LAST_YEAR = 2025
MIN_TREND_VALID_YEARS = 30


def main():
    for path in (DAILY_PATH, COMPLETENESS_PATH):
        if not path.exists():
            print(f"ERROR: Required input not found: {path}")
            print("Run src/prepare_raw.py and src/prepare_baseline.py first.")
            return False

    spark = (
        SparkSession.builder
        .appName("ExtremeWeatherStationEligibility")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 65)
        print("STATION ELIGIBILITY")
        print("=" * 65)
        print(f"Valid year: days_present_pct >= {VALID_YEAR_PCT:.0f} "
              "(TMAX, TMIN and PRCP all present on the same day)")
        print(f"Baseline eligibility: >= 80% of baseline days per variable "
              f"AND >= {MIN_BASELINE_VALID_YEARS} valid years in "
              f"{BASELINE_FIRST_YEAR}-{BASELINE_LAST_YEAR}")
        print(f"Trend window: {TREND_FIRST_YEAR}-{TREND_LAST_YEAR}, "
              f"{MIN_TREND_VALID_YEARS} valid years needed")

        daily = spark.read.parquet(str(DAILY_PATH))

        # One row per station and date, with the three variables side by side.
        wide = (
            daily
            .filter(F.col("element").isin("TMAX", "TMIN", "PRCP"))
            .groupBy("station_id", "date")
            .pivot("element", ["TMAX", "TMIN", "PRCP"])
            .agg(F.first("value"))
        )

        complete_days = wide.filter(
            F.col("TMAX").isNotNull()
            & F.col("TMIN").isNotNull()
            & F.col("PRCP").isNotNull()
        )

        yearly = (
            complete_days
            .withColumn("year", F.year("date"))
            .groupBy("station_id", "year")
            .agg(F.count("*").alias("complete_days"))
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
            .withColumn(
                "days_present_pct",
                F.round(
                    F.col("complete_days")
                    / F.col("days_in_year")
                    * 100.0,
                    2,
                ),
            )
            .filter(F.col("days_present_pct") >= VALID_YEAR_PCT)
        )

        baseline_counts = (
            yearly
            .filter(
                F.col("year").between(
                    BASELINE_FIRST_YEAR, BASELINE_LAST_YEAR
                )
            )
            .groupBy("station_id")
            .agg(F.count("*").alias("baseline_valid_years"))
        )

        trend_counts = (
            yearly
            .filter(
                F.col("year").between(TREND_FIRST_YEAR, TREND_LAST_YEAR)
            )
            .groupBy("station_id")
            .agg(F.count("*").alias("valid_years_1991_2025"))
        )

        # 80% rule per variable: all three variables must pass.
        completeness = (
            spark.read.parquet(str(COMPLETENESS_PATH))
            .groupBy("station_id")
            .agg(
                F.sum(
                    F.when(F.col("meets_80_percent") == "YES", 1)
                    .otherwise(0)
                ).alias("_passing_variables")
            )
            .withColumn(
                "meets_80_all_variables",
                F.col("_passing_variables") == 3,
            )
            .drop("_passing_variables")
        )

        result = (
            daily.select("station_id").distinct()
            .join(baseline_counts, "station_id", "left")
            .join(trend_counts, "station_id", "left")
            .join(completeness, "station_id", "left")
            .fillna(
                {
                    "baseline_valid_years": 0,
                    "valid_years_1991_2025": 0,
                    "meets_80_all_variables": False,
                }
            )
            .withColumn(
                "baseline_eligible",
                F.col("meets_80_all_variables")
                & (
                    F.col("baseline_valid_years")
                    >= MIN_BASELINE_VALID_YEARS
                ),
            )
            .withColumn(
                "enough_years_for_trend",
                F.col("valid_years_1991_2025") >= MIN_TREND_VALID_YEARS,
            )
            .select(
                "station_id",
                "baseline_valid_years",
                "valid_years_1991_2025",
                "meets_80_all_variables",
                "baseline_eligible",
                "enough_years_for_trend",
            )
            .orderBy("station_id")
        )

        pdf = result.toPandas()
        if pdf.empty:
            print("ERROR: No stations found.")
            return False

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        result.write.mode("overwrite").parquet(str(OUTPUT_PATH))
        CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        pdf.to_csv(CSV_PATH, index=False)

        stations = len(pdf)
        eligible = int(pdf["baseline_eligible"].sum())
        fail_80 = int((~pdf["meets_80_all_variables"]).sum())
        fail_years = int(
            (pdf["baseline_valid_years"] < MIN_BASELINE_VALID_YEARS).sum()
        )

        print(f"\nStations checked: {stations:,}")
        print(f"Baseline eligible: {eligible:,}")
        print(f"Failing the 80% rule (any variable): {fail_80:,}")
        print(f"Failing the {MIN_BASELINE_VALID_YEARS}-of-30 valid-years "
              f"rule: {fail_years:,}")
        print(f"With >= {MIN_TREND_VALID_YEARS} valid years in "
              f"{TREND_FIRST_YEAR}-{TREND_LAST_YEAR}: "
              f"{int(pdf['enough_years_for_trend'].sum()):,}")
        print(f"\nSaved: {OUTPUT_PATH}")
        print(f"Saved: {CSV_PATH}")

        not_eligible = pdf[~pdf["baseline_eligible"]]
        if not not_eligible.empty:
            print("\nNot baseline eligible (first 20):")
            print(not_eligible.head(20).to_string(index=False))

        return True

    finally:
        spark.stop()


if __name__ == "__main__":
    if not main():
        sys.exit(1)
