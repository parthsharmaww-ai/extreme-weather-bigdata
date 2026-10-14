# Global Extreme Weather Intelligence

**Where extreme weather happens, how it's changing, and which places are most at risk.**

This is a big data pipeline and a five-page Tableau dashboard built on daily NOAA weather station records. It takes raw GHCN-Daily files and runs them through PySpark. Out the other end come extreme events, long-term trends, spatial hotspots and a risk ranking for 102 stations from 1991 to 2025.

It's our Group 6 project for the Big Data and Data Visualization course in the M.Sc. Data Science & Management program (IIT Ropar × IIM Amritsar).

---

## What we found

- **Warm days have roughly doubled.** The average station went from about 11 unusually warm days a year in 1991–95 to about 24 in 2021–25. 2024 was the worst year, at 28.3 per station.
- **Daily highs are running hot.** Max temperatures now sit about 1°C above the 1991–2020 normal for the same calendar day.
- **Extremes cluster.** Warm-day values are strongly clustered in space (Moran's I = 0.72, p < 0.001). Hotspots show up in the Arctic and northern Europe, and cold spots across the Pacific islands.
- **Each hazard has its own map.** Heavy-rain hotspots sit along the Pacific Northwest coast and the western Pacific islands. Dry-spell and compound hot-and-dry hotspots sit in the US Southwest and inland Australia.
- **Wet and dry extremes shift locally.** Averaged across the network, heavy-rain days and the longest dry spell barely move. The trend maps show where they do.

---

## The idea: "extreme" is relative

35°C is a normal summer day in Phoenix and a record in Oslo. So we never use fixed cut-offs. Every threshold is set **per station, against its own 1991–2020 climate**. For temperature it's also set per calendar day, so a warm day is warm *for the season*.

| Event | Definition |
|---|---|
| Warm day | Max temp above that station's 95th percentile for the calendar day (±7-day window) |
| Warm spell | 3+ warm days in a row |
| Cold snap | 3+ days in a row with min temp below the 5th percentile |
| Heavy rain day | Rain above the station's 95th percentile of wet days (≥ 1 mm) |
| Dry spell | 10+ days in a row under 1 mm (the headline measure is the year's longest dry spell) |
| Compound event | A warm spell overlapping a dry spell |

The full rules are in [`docs/event_definitions.md`](docs/event_definitions.md). That covers baseline, completeness checks and edge cases like missing days and events that cross New Year.

---

## How it works

```
NOAA GHCN-Daily ──► PySpark (parse, clean, Parquet)
                        │
                        ├─► completeness checks (valid years, 80% baseline, 25-of-30 rule)
                        ├─► station-relative thresholds (1991–2020, per calendar day)
                        ├─► event detection (run-length spells, compound overlaps)
                        │
                        ├─► trends: Theil-Sen slope + Mann-Kendall test (30+ complete years)
                        ├─► hotspots: Getis-Ord Gi*  ·  clustering: Moran's I
                        ├─► provisional risk score
                        └─► validation against NOAA Storm Events
                                        │
                                        ▼
                         CSV exports ──► Tableau dashboard (5 pages)
```

**Stack:** PySpark (in Docker), pandas, NumPy, SciPy, pymannkendall, PySAL (`esda`, `libpysal`), GeoPandas, scikit-learn, ruptures, LightGBM, Tableau.

**Data sources:**
- [NOAA GHCN-Daily](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily) for daily max temp, min temp and rain
- [NOAA Oceanic Niño Index](https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ONI_v5.php) for El Niño / La Niña phases
- [NOAA Storm Events](https://www.ncdc.noaa.gov/stormevents/) for checking detected events against reported ones

---

## The dashboard

The packaged workbook is [`tableau/extreme_weather_dashboard.twbx`](tableau/extreme_weather_dashboard.twbx). Every page filters by continent, country, region, year range and event type.

| Page | Question it answers |
|---|---|
| 1. Global overview | Where does extreme weather happen? |
| 2. Hotspots and risk | Which places are most at risk, and are the clusters statistically real? |
| 3. Trends | How is it changing over time, with ENSO phases for context? |
| 4. Heavy rain and dry spells | Are places getting wetter, drier, or both? |
| 5. Region comparison | How do two places compare across all six event types? |

The design notes behind each page are in [`docs/dashboard_storyboard.md`](docs/dashboard_storyboard.md).

---

## Running it

You'll need Docker. Everything runs inside the container, so you don't need Spark or Java installed locally.

```bash
git clone https://github.com/parthsharmaww-ai/extreme-weather-bigdata.git
cd extreme-weather-bigdata
docker compose up --build
```

That starts a PySpark + JupyterLab container. JupyterLab is on `localhost:8888` and the Spark UI on `localhost:4040`. Then run the pipeline scripts in order from the repo root inside the container:

```bash
python src/screen_candidate_stations.py      # step 0: pick usable stations
python src/download_data.py                  # step 1: metadata + Storm Events
python src/prepare_raw.py                    # step 2: parse and clean
# ... through ...
python src/build_dashboard_data.py           # step 14: tables for Tableau
```

The full 15-step run order is in [`docs/RUNBOOK.md`](docs/RUNBOOK.md). It lists what each script reads and writes, plus the checks to run along the way. The `data/` folder is kept out of git, and the pipeline rebuilds it.

### Tests

The tests run on synthetic data, so you don't need to download anything:

```bash
python tests/check_station_eligibility.py
python tests/check_station_summary.py
python tests/check_station_geography.py
python tests/check_risk_and_hotspots.py
python tests/check_storm_events_validation.py
python tests/check_candidate_screening.py
python tests/check_dashboard_data.py
```

---

## Repo layout

```
src/          pipeline scripts (ingest → thresholds → events → trends → hotspots → exports)
tests/        checks on synthetic data
docs/         event definitions, dashboard storyboard, runbook
reference/    country → continent and region-name lookups
tableau/      packaged workbook, ENSO data, mock data, PDF/PNG exports
data/         raw, parquet and export folders (not in git)
```

---

## Limitations

- **Coverage isn't global yet.** The 102 stations are 47 in North America, 30 in Oceania, 24 in Europe and 1 in Asia, with none in Africa or South America. Most stations fail the completeness rules, because those rules need max temp, min temp and rain all on the same day.
- **The risk score is provisional.** Right now it's 0.6 × the percentile rank of a station's average yearly level plus 0.4 × the percentile rank of its trend. The weights are still up for discussion.
- **Regions differ in size.** For example, all Australian stations form one region, while many regions have a single station. So compare region averages with care.
- **Percentiles are approximate.** They come from Spark's `percentile_approx`.

---

## Team

- **Parth Sharma**: data analytics and ML (event detection, trends, hotspots, risk index)
- **Sarthak Manav**: dashboard design and Tableau
- **Tanuj**: event definitions

Guided by **Prof. Sunil Reddy Kunduru**, Big Data and Data Visualization, MSDSM Batch 03, IIT Ropar × IIM Amritsar.
