# Run order (v2 definitions)

Run everything inside the Docker container (`docker compose up --build`), from
the repository root. `data/` is not in git, so every step below recreates its
output. Steps depend on the ones before them.

Status: each step has been tested on synthetic data. The full chain has not yet
been run end to end on real data.

| # | Step | Script | Reads | Writes |
|---|---|---|---|---|
| 0 | Choose usable stations | `src/screen_candidate_stations.py` | NOAA inventory and `.dly` files of candidates | `data/screening/`, `data/raw/ghcn_d/global_pilot_station_ids.txt`, the chosen `.dly` files |
| 1 | Download raw data | `src/download_data.py` | NOAA | `data/raw/ghcn_d/*.dly`, `ghcnd-stations.txt`, Storm Events file |
| 2 | Parse and clean | `src/prepare_raw.py` | `data/raw` | `data/parquet/ghcn_daily`, `storm_events` |
| 3 | Baseline and 80% rule | `src/prepare_baseline.py` | `ghcn_daily` | `ghcn_baseline_1991_2020`, `station_completeness_1991_2020` |
| 4 | Valid years, 25-of-30 rule | `src/check_station_eligibility.py` | `ghcn_daily`, `station_completeness_1991_2020` | `station_eligibility`, `data/export/station_eligibility.csv` |
| 5 | Thresholds | `src/calculate_thresholds.py` | `ghcn_baseline_1991_2020`, `station_completeness_1991_2020`, `station_eligibility` | `extreme_thresholds_1991_2020` |
| 6 | Baseline calendar-day means | `src/calculate_baseline_means.py` | `ghcn_baseline_1991_2020` | `baseline_means_1991_2020` |
| 7 | Detect events | `src/detect_events.py` | `ghcn_daily`, `extreme_thresholds_1991_2020` | `extreme_events` |
| 8 | Station metadata | `src/export_station_metadata.py` | `ghcnd-stations.txt`, `global_pilot_station_ids.txt` | `data/export/station_metadata.csv` |
| 9 | Region, country, continent | `src/build_station_geography.py` | `station_metadata.csv`, `ghcnd-stations.txt`, `reference/` | `data/export/station_geography.csv` |
| 10 | Table A and events | `src/export_station_year_metrics.py` | steps 2, 5, 6, 7, 8, 9 | `station_year_metrics.csv`, `station_events.csv` |
| 11 | Table B (trends) | `src/compute_station_summary.py` | `station_year_metrics.csv` | `station_summary.csv` |
| 12 | Risk score and hotspots | `src/compute_risk_and_hotspots.py` | Tables A and B | `station_summary.csv` (filled in), `global_stats.csv` |
| 13 | Validate against Storm Events | `src/validate_against_storm_events.py` | `extreme_events`, `storm_events`, `station_geography.csv` | `storm_events_validation.csv` |

Run `python src/<script>.py` for each step. Step 0 already puts the chosen stations' `.dly` files in
`data/raw/ghcn_d`, so step 1 only needs to run for the station metadata and the Storm Events file. The data folders are
`data/parquet/` (Spark outputs) and `data/export/` (CSV for Tableau).

## Checks after the steps

| After step | Check | What to look for |
|---|---|---|
| 5 | `src/validate_thresholds.py` | 366 rows per element per station, so 1,098 rows per station. The expected total in that script is hard-coded (lines 89 and 94) and must be updated to 1,098 x the number of stations. |
| 7 | `src/validate_events.py`, `src/validate_compound_events.py` | Both end with a pass message. Compound events must be overlaps of a heatwave and a dry spell. |
| 10 | Row count and columns | One row per station and year. Columns match Table A in `docs/dashboard_storyboard.md`. |
| 11, 12 | Row counts | Table B has 6 rows per station. Only trend-eligible stations (30 valid years in 1991-2025) have a trend and a risk score. |

## Tests (synthetic data, no real data needed)

```
python tests/check_station_eligibility.py
python tests/check_station_summary.py
python tests/check_station_geography.py
python tests/check_risk_and_hotspots.py
python tests/check_storm_events_validation.py
python tests/check_candidate_screening.py
```

## Files for Tableau

`station_year_metrics.csv` (Table A), `station_summary.csv` (Table B),
`global_stats.csv` (Table C), `station_events.csv` (Table E), and
`tableau/enso_monthly.csv` (Table D).

## Choosing the stations (step 0)

Most stations that cover 1991-2025 on paper do not pass the rules, because the
rules need TMAX, TMIN and PRCP all present on the same day. A first list of 100
stations produced only 15 stations with thresholds and 8 with trends. Step 0
tests candidates against the same rules before the pipeline is run.

```
python src/screen_candidate_stations.py --dry-run     # sizes the candidate pool
python src/screen_candidate_stations.py               # screens and installs the best stations
```

- Candidates: stations with all three variables covering 1991 to 2024, up to 5 per 10-degree cell.
- Selection: at most 2 stations per cell, at least 300 km apart, up to 120 in total.
  Los Angeles and New York Central Park are kept when they pass (their threshold was checked by hand).
- Results are saved in `data/screening/`, so a rerun only screens what is missing.
- The old `.dly` files are moved to `data/raw/ghcn_d_archive`, and the previous list is saved as
  `global_pilot_station_ids_previous.txt`.
- Check that screening agrees with the pipeline:
  `python src/screen_candidate_stations.py --compare data/export/station_eligibility.csv --source-dir data/raw/ghcn_d`
  must report "Differences: 0".

## Not confirmed

- `src/select_global_pilot.py` reads `data/raw/ghcn_d/eligible_station_ids.txt`, and no script in the
  repository creates that file. `src/screen_candidate_stations.py` replaces that step.
