# Dashboard Storyboard

Owner: Sarthak Manav
Tool: Tableau Public / Desktop
Status: v4. Aligned with `event_definitions.md` v2 (Phase 5a). Station grain.

## 1. The story in one line

**Where extreme weather happens, how it is changing, and which regions are most at risk.**

The viewer moves from *where* (global map) → *who is most at risk* (hotspots) → *how it's changing* (trends) → *wet vs dry extremes* (heavy rain and dry spells) → *compare two places* (region comparison).

Unit of analysis: **weather station** (`station_id`). There is no grid and no `cell_id`. Maps plot stations directly, and Gi* uses distance-based neighbours between stations.

"Extreme" always means extreme **for that station's own climate** (season-relative for temperature). The dashboard must never present a warm spell as an absolute temperature.

## 2. Global filters (on every page)

| Filter | Type | Source |
|---|---|---|
| Location | Dropdown: continent → country → region | `continent`, `country`, `region_name` |
| Year range | Range slider, default 1991–2025 | `year` |
| Event type | Single select (parameter that switches the measure) | see table below |
| Season | Single select: All / DJF / MAM / JJA / SON | `start_date` in Table E. Event measures only, see Table E in Section 4 |
| Complete years only | Fixed filter, always on | `days_present_pct >= 90` |

**Event type options and the measure each one shows**

| Label on dashboard | Yearly measure (Table A) | Trend `event_type` (Table B) |
|---|---|---|
| Warm days (unusually warm for the season) | `hot_days` | `hot_days` |
| Warm spells (3+ warm days) | `heatwave_days` | `heatwave` |
| Cold snaps (3+ unusually cold days) | `coldsnap_days` | `cold` |
| Heavy rain (top 5% of wet days) | `heavy_rain_days` | `heavy_rain` |
| Dry spells (longest run of dry days) | `longest_dry_spell` | `dry_spell` |
| Compound (warm spell during a dry spell) | `compound_days` | `compound` |

**Interactions**
- Clicking a station on any map → filter action updates every other chart on the page.
- Hover → tooltip with station name, region, years of data, and key metrics.
- "Reset" button clears the selection.
- Info (i) icon on each page shows the plain-English definition (Section 6).

**Display rules (from the definitions)**
- Yearly counts and charts use **complete years only**. 2026 is partial (data to 29 Sep) and is excluded automatically.
- Stations with `trend_eligible = false` (fewer than 30 complete years) are shown **grey with the label "Not enough years for a trend"**. Their `trend_slope` is empty.
- `compound_days` are also inside `heatwave_days` and `dry_spell_days`. **Never stack them** in one bar or area chart. Show them side by side or separately.
- Trends are significant when `trend_p_value < 0.05`: drawn solid, others faded.

## 3. Pages

### Page 1: Global overview map
**Answers:** Where does extreme weather happen?
- Map of stations, colour = average yearly value of the selected measure over the chosen years.
- KPI tiles: stations shown, average events per station-year, change vs 1991–2020 average, worst year.
- Bar: top 10 regions by the selected measure.
- For **Dry spells**, this page's map uses the *trend* in `longest_dry_spell` (days per decade), not the raw value, so naturally dry stations (LAX, Niamey) don't dominate.

### Page 2: Hotspots and risk
**Answers:** Which regions are most at risk, and are the clusters statistically real?
- Map coloured by `hotspot_class` (Hot spot 99/95/90%, Not significant, Cold spot 90/95/99%).
- Ranked bar: top 20 stations by `risk_score` for the selected event type.
- Text tile: global Moran's I and p-value for the selected event type (Table C).
- Note on the page: "Risk score formula is provisional" until it is decided.

### Page 3: Trends
**Answers:** How is extreme weather changing over time?
- Line chart: yearly value of the selected measure (all selected stations, or the clicked one).
- ENSO shading behind the line: El Niño / La Niña periods from `enso_monthly`.
- Map of `trend_slope` per decade, diverging colours. Significant solid, others faded, ineligible grey.
- Small line: `tmax_anomaly_mean` by year ("°C warmer or colder than the 1991–2020 normal for that day").

### Page 4: Heavy rain and dry spells
**Answers:** Are places getting wetter, drier, or both?
- Dual line by year: `heavy_rain_days` vs `longest_dry_spell` (both in days).
- Map: trend in `heavy_rain_days` per decade.
- Scatter per station: average `heatwave_days` vs average `longest_dry_spell`, size = average `compound_days` (compound view).
- Caption: "Heavy rain = top 5% of the station's wet days. Dry stations get few heavy-rain days by design."

### Page 5: Region comparison
**Answers:** How do two places compare?
- Two station or region pickers → side-by-side yearly lines, KPI tiles and a difference table for all six measures.

## 4. Export tables requested from the pipeline

Format: CSV, UTF-8, lowercase snake_case headers, same `station_id` in every table, missing values left empty (not -999).

### Table A: `station_year_metrics` (one row per station per year)
Matches the output of `src/export_station_year_metrics.py`, with the v2 rename.

| Column | Type | Description | Used on |
|---|---|---|---|
| station_id | string | Station ID | all |
| station_name | string | For tooltips | all |
| lat, lon | float | Station location | 1–5 |
| elevation_m | float | For tooltips | 1 |
| region_name, country, continent | string | From station→region lookup | filters, 5 |
| year | int | Calendar year. Events are assigned to their start year | all |
| days_present_pct | float | % of days with valid data. Valid year = ≥ 90 | filter, tooltips |
| hot_days | int | Days with TMAX above that calendar day's p95 (season-relative) | 1, 3, 5 |
| heatwave_count | int | Warm spells (3+ consecutive warm days) | tooltips |
| heatwave_days | int | Days inside warm spells | 1, 3, 4, 5 |
| heatwave_max_intensity | float | Max of (TMAX − threshold), °C. Empty if no warm spell | tooltips |
| cold_days | int | Days with TMIN below that calendar day's p5 | 1, 3, 5 |
| coldsnap_count, coldsnap_days | int | Cold snaps (3+ consecutive cold days) | 1, 3, 5 |
| **heavy_rain_days** | int | Days above the station's annual p95 of baseline wet days (was `extreme_rain_days`) | 1, 3, 4, 5 |
| max_daily_prcp_mm | float | Wettest day of the year | tooltips |
| prcp_total_mm | float | Annual total | 4 |
| dry_spell_count, dry_spell_days | int | Dry spells (10+ consecutive days under 1 mm) | tooltips |
| longest_dry_spell | int | Longest run of dry days. **Headline dry measure** | 1, 3, 4, 5 |
| compound_event_count | int | Warm spells overlapping a dry spell | tooltips |
| compound_days | int | Overlapping days only (also counted in heatwave and dry-spell columns) | 1, 3, 4, 5 |
| tmax_anomaly_mean | float | Mean of (TMAX − baseline mean TMAX for that calendar day), °C | 3 |

### Table B: `station_summary` (one row per station per event type)
| Column | Type | Description |
|---|---|---|
| station_id, lat, lon, region_name, country, continent | | as Table A |
| event_type | string | `hot_days` / `heatwave` / `cold` / `heavy_rain` / `dry_spell` / `compound` |
| trend_slope | float | Change per decade (Theil-Sen). `dry_spell` uses `longest_dry_spell`. Empty if not eligible |
| trend_p_value | float | Mann-Kendall p-value. Empty if not eligible |
| years_used | int | Number of valid (≥ 90%) years **inside 1991–2025** actually used for this trend |
| trend_eligible | bool | `years_used >= 30` |
| risk_score | float 0–100 | Combined risk (formula to be agreed) |
| risk_rank | int | Rank within the event type |
| gi_z_score, gi_p_value | float | Getis-Ord Gi* output |
| hotspot_class | string | "Hot spot 99%" … "Not significant" … "Cold spot 99%" |

The yearly measure behind each `event_type` is in the Section 2 table.

**Common trend window: 1991–2025 for every station.** Table A may hold a station's full history (NYC starts in 1869), so the Table B script must filter explicitly before fitting:
`year BETWEEN 1991 AND 2025 AND days_present_pct >= 90`.
Then `years_used` = rows left after that filter, and `trend_eligible = years_used >= 30`. Without the filter, long-record stations would get a 150-year trend while others get 30 years, and the trend map would compare unlike things. 2026 is outside the window (and partial), so it never enters a trend. The mock script applies the same filter (`TREND_FIRST_YEAR`, `TREND_LAST_YEAR`).

### Table C: `global_stats` (one row per event type)
`event_type, morans_i, morans_p_value, n_stations`

### Table D: `enso_monthly` (supplied by Sarthak, `src/prepare_enso.py`)
`year, month, season, nino34_anomaly, enso_phase, provisional`
- Source: NOAA CPC Oceanic Niño Index (3-month running mean of Niño 3.4 anomalies), 1950 onward.
- `month` is the centre month of the 3-month season (DJF → 1).
- `enso_phase` uses NOAA's rule: El Niño (La Niña) when ONI ≥ +0.5 (≤ −0.5) for 5+ consecutive seasons, otherwise Neutral.
- `provisional = true` for the latest seasons whose phase can still change: an open run at the end of the record that is past ±0.5 but not yet 5 seasons long. Currently AMJ–JAS 2026 (+0.95 to +2.16), shown as Neutral until the run reaches 5 seasons. On the dashboard, provisional months are drawn hatched with the tooltip "Provisional: ONI above +0.5 for 4 seasons, El Niño not yet confirmed". NOAA also revises recent ONI values, so re-download before the final presentation.

### Table E: `station_events` (one row per event, for drill-down and the season filter)
`station_id, event_type, start_date, end_date, duration_days, intensity_value, intensity_measure`

This is the event table `detect_events.py` already builds, exported as CSV with its existing column names. `intensity_measure` is a text label so tooltips can show the unit next to the number.

**Event-type mapping** between Table E (pipeline names) and Table B (dashboard names):

| Table E `event_type` | Table B `event_type` | Dashboard label | Intensity (v2) |
|---|---|---|---|
| `HEATWAVE` | `heatwave` | Warm spells | Max of (TMAX − threshold), °C |
| `COLD_SNAP` | `cold` | Cold snaps | Max of (threshold − TMIN), °C |
| `HEAVY_RAIN` (currently `EXTREME_RAIN`) | `heavy_rain` | Heavy rain | Rain above threshold, mm (one row per day) |
| `DRY_SPELL` | `dry_spell` | Dry spells | Duration, days |
| `COMPOUND_HEATWAVE_DRY_SPELL` | `compound` | Compound | Overlap duration, days |
| (no events) | `hot_days` | Warm days | n/a: single days, not events |

In Tableau this mapping is a small calculated field (CASE on `event_type`), so no extra file is needed.

**Which measures the season filter applies to.** Table E only has rows for heatwaves, cold snaps, heavy-rain days, dry spells of 10+ days and compound events. Each event is placed in the season of its `start_date`.

| Measure | Season filter? | Why |
|---|---|---|
| Warm spells, cold snaps, compound | Yes | Events in Table E |
| Heavy rain days | Yes | One Table E row per day |
| Dry spells (count / days, 10+ days) | Yes | Events in Table E |
| Warm days (`hot_days`), cold days | **No** | Single days aren't in Table E |
| Longest dry spell | **No** | Runs under 10 days aren't in Table E, and a 145-day run spans seasons |
| `tmax_anomaly_mean`, trends, risk, hotspots | **No** | Annual or per-station values |

When the season is not "All", measures marked No are greyed out with the note "Season filter not available for this measure (yearly value)". Seasons are meteorological (DJF, MAM, JJA, SON) and are not flipped for southern-hemisphere stations; the tooltip shows the station's hemisphere.

## 5. Mock data for the Tableau prototype

Generated by `src/make_mock_data.py` (fixed seed, rerun any time). **All values are fake placeholders, not climate data.** Station IDs start with `MOCK`.

| File | Matches | Rows |
|---|---|---|
| `tableau/mock_station_year_metrics.csv` | Table A, same columns in the same order | 13 stations × 1991–2026 (458 rows) |
| `tableau/mock_station_summary.csv` | Table B | 13 stations × 6 event types |
| `tableau/mock_global_stats.csv` | Table C | 6 rows |
| `tableau/enso_monthly.csv` | Table D (**real NOAA data**, latest 4 seasons provisional) | 920 months |

What the mock covers:
- 2026 is a partial year (74.6%), so the "complete years only" filter can be tested.
- MOCK0012 Nairobi (record starts 2001) and MOCK0013 Ulaanbaatar (patchy years) have fewer than 30 valid years, so they show `trend_eligible = false`.
- Trends in Table B are calculated from the Table A mock rows with the real rules (Theil-Sen, Mann-Kendall, valid years only, `longest_dry_spell` for dry spells).
- Internal rules hold: `heatwave_days ≤ hot_days`, runs ≥ 3 or ≥ 10 days, `compound_days ≤` both `heatwave_days` and `dry_spell_days`.
- Mock LAX and NYC sit close to the pilot numbers (LAX ~1–2 heavy-rain days and ~145-day longest dry spell; NYC ~5 and ~16).
- `risk_score`, `gi_*` and `hotspot_class` are placeholders until those phases run.

## 6. Plain-English definitions for the info icons

- **Warm day:** hotter than 95% of the same dates in 1991–2020 at this station. Warm *for the season*, not a fixed temperature, so a mild winter day can count.
- **Warm spell:** 3 or more warm days in a row.
- **Cold snap:** 3 or more days in a row colder (night-time minimum) than 95% of the same dates in 1991–2020.
- **Heavy rain day:** rain heavier than 95% of this station's rainy days in 1991–2020.
- **Dry spell / longest dry spell:** days in a row with less than 1 mm of rain. A dry spell is 10+ days; the longest dry spell is the longest run that year.
- **Compound event:** a warm spell happening during a dry spell.
- **Missing data:** a missing day ends a spell and counts as unknown, not as dry or normal.
- **Trend:** change per decade over at least 30 complete years. Grey stations don't have enough years.

## 7. Decisions and open questions

**Decided**
- Station grain. Definitions follow `event_definitions.md` v2.
- `heatwave_*` column names kept; labels say "warm spell (unusually warm for the season)".
- Dry-spell maps and trends use `longest_dry_spell`.

**Open**
1. **Pipeline rename not done yet.** `export_station_year_metrics.py` still writes `extreme_rain_days`, and `detect_events.py` still uses `EXTREME_RAIN`. Needs changing to `heavy_rain_days` / `HEAVY_RAIN` (Tanuj).
2. **Table B script.** No trend/summary export exists yet. The mock generator shows the intended method, including the 1991–2025 window filter.
3. **Table E export.** `detect_events.py` writes Parquet; the dashboard needs it as CSV (`station_events.csv`) with `intensity_measure` kept.
4. Risk score formula: inputs and weights.
