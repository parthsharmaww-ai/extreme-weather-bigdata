# Dashboard Storyboard (Phase 1)

Owner: Sarthak Manav
Tool: Tableau Public / Desktop
Status: Draft v3. Station grain decided. Table A is the single source of truth for the mock data and the pipeline exports.

## 1. The story in one line

**Where extreme weather happens, how it is changing, and which regions are most at risk.**

The viewer moves from *where* (global map) → *who is most at risk* (hotspots) → *how it's changing* (trends) → *wet vs dry extremes* (rainfall and dry spells) → *compare two places* (region comparison).

Unit of analysis: **weather station** (`station_id`), as in `event_definitions.md`. **Decision: we work at station grain.** There is no grid aggregation and no `cell_id`. Maps plot stations directly, and Gi* uses distance-based neighbours between stations. Changing this later would mean revising Table A first.

## 2. Global filters (on every page)

| Filter | Type | Source column |
|---|---|---|
| Location | Dropdown: continent → country → region | `continent`, `country`, `region_name` |
| Year range | Range slider | `year` |
| Event type | Single select: Hot days / Heatwaves / Cold snaps / Extreme rain / Dry spells / Compound (heatwave + dry spell) | parameter that switches the measure shown |

**Interactions**
- Clicking a station or region on any map → filter action updates every other chart.
- Hover → tooltip with station name, region and key metrics.
- "Reset" button clears selection.
- Info (i) icon on each page shows the plain-English event definition.

## 3. Pages

### Page 1: Global overview map
**Answers:** Where does extreme weather happen?
- Map of stations coloured by the selected event metric (average per year over the chosen range).
- KPI tiles: total events, % change vs the 1991–2020 baseline, worst year.
- Bar: top 10 countries by event days.

### Page 2: Hotspots and risk
**Answers:** Which regions are most at risk, and are the clusters statistically real?
- Map coloured by Gi* hotspot class (hot spot 99/95/90%, not significant, cold spot).
- Ranked bar: top 20 regions by `risk_score`.
- Text tile: global Moran's I and p-value for the selected event type.

### Page 3: Trends
**Answers:** How is extreme weather changing over time?
- Line chart: yearly event days (global or selected region).
- Map of `trend_slope` (diverging colour). Significant trends (p < 0.05) solid, others faded.
- ENSO overlay: Niño 3.4 phase shading behind the line (El Niño / La Niña years).

### Page 4: Rainfall and dry spells
**Answers:** Are places getting wetter, drier, or both?
- Dual line: extreme rain days vs dry-spell days by year.
- Map: change in extreme rain days.
- Scatter: heatwave days vs dry-spell days per station (compound-event view).

### Page 5: Region comparison
**Answers:** How do two regions compare?
- Two region pickers → side-by-side lines and KPI tiles, plus a difference table.

## 4. Export tables requested from the pipeline (the important part)

### Table A: `station_year_metrics` (one row per station per year)
| Column | Type | Description | Used on |
|---|---|---|---|
| station_id | string | Station ID | all |
| station_name | string | For tooltips | all |
| lat, lon | float | Station location | 1, 2, 3, 4 |
| elevation_m | float | For tooltips | 1 |
| region_name, country, continent | string | From station→region lookup | all filters, 5 |
| year | int | Calendar year (event assigned to its start year) | all |
| days_present_pct | float | % of days with data that year (to flag incomplete years) | QA, tooltips |
| hot_days | int | Days with TMAX > p95 | 1, 3 |
| heatwave_count | int | Number of heatwaves | 1, 3 |
| heatwave_days | int | Total days in heatwaves | 1, 3, 4 |
| heatwave_max_intensity | float | Max TMAX anomaly above threshold (°C) | tooltips |
| cold_days | int | Days with TMIN < p5 | 1, 3 |
| coldsnap_count, coldsnap_days | int | Cold snaps | 1, 3 |
| extreme_rain_days | int | Days with PRCP > p99 of wet days | 1, 3, 4 |
| max_daily_prcp_mm | float | Wettest day of the year | 4 |
| prcp_total_mm | float | Annual total | 4 |
| dry_spell_count, dry_spell_days | int | Dry spells (≥10 dry days) | 4 |
| longest_dry_spell | int | Days | 4 |
| compound_event_count | int | Heatwave overlapping a dry spell | 1, 4 |
| compound_days | int | Overlapping days | 1, 4 |
| tmax_anomaly_mean | float | Mean TMAX anomaly vs baseline (°C) | 1, 3 |

### Table B: `station_summary` (one row per station per event type)
| Column | Type | Description |
|---|---|---|
| station_id, lat, lon, region_name, country, continent | | as above |
| event_type | string | hot_days / heatwave / cold / extreme_rain / dry_spell / compound |
| trend_slope | float | Change in event days **per decade** |
| trend_p_value | float | Significance (e.g. Mann-Kendall) |
| years_used | int | Years of data behind the trend |
| risk_score | float 0–100 | Combined risk (formula to be agreed) |
| risk_rank | int | Rank among all stations |
| gi_z_score, gi_p_value | float | Getis-Ord Gi* output |
| hotspot_class | string | e.g. "Hot spot 95%", "Not significant" |

### Table C: `global_stats` (one row per event type)
`event_type, morans_i, morans_p_value, n_stations`

### Table D: `enso_monthly` (lookup, supplied by Sarthak)
`year, month, nino34_anomaly, enso_phase` (El Niño ≥ +0.5, La Niña ≤ −0.5, else Neutral)

### Table E: `station_events` (optional, one row per event, for drill-down)
`station_id, event_type, start_date, end_date, duration_days, intensity`

**Format:** CSV or Parquet, UTF-8, lowercase snake_case headers, the same `station_id` in every table, missing values left empty (not -999).

## 5. Mock data for the Tableau prototype

| File | Matches | Rows |
|---|---|---|
| `tableau/mock_station_year_metrics.csv` | Table A, exactly the same columns in the same order | 10 stations × 24 years (2000–2023) |
| `tableau/mock_station_summary.csv` | Table B, exactly the same columns in the same order | 10 stations × 6 event types |

The mock values are **placeholders for wiring up the charts, not real climate data**. Station names are real cities so the map looks sensible, and annual precipitation is set to roughly realistic levels (e.g. Delhi ≈ 800 mm, Kochi ≈ 3000 mm). Event counts are random but internally consistent (e.g. `heatwave_days` ≤ `hot_days`, `compound_days` ≤ both `heatwave_days` and `dry_spell_days`). Station IDs are prefixed `MOCK` so they can't be confused with real ones.

## 6. Decisions and open questions

**Decided**
- Station grain (see Section 1). There is no `cell_id`.
- Naming follows `event_definitions.md`: "dry spell", not "drought".

**Still open**
1. Value of N (minimum years) in the completeness rule. Suggest ≥ 25 of 30 baseline years, plus ≥ 30 years overall for trends.
2. Risk score formula: which inputs and weights?
3. See review notes on `event_definitions.md` (sent separately).
