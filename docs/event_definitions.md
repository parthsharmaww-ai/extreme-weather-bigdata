# Extreme Event Definitions (v2)

Status: Decided in Phase 5a (October 2026) by Parth, Tanuj and Sarthak.
These are pilot definitions, tested on two stations (NYC Central Park and LAX).
Review them again when the station set is expanded.

Everything is station-based: "extreme" means extreme relative to that
station's own climate.

## 1. Baseline and thresholds

- Baseline period: 1991-01-01 to 2020-12-31 (10,958 days).
- Calendar: 366 positions. February 29 has its own position (leap reference
  year 2000).
- Temperature thresholds (TMAX p95, TMIN p5): per station and calendar day,
  from a +/-7-day window across the baseline years.
- Rain threshold: per station, ONE annual value, the p95 of baseline wet days
  (PRCP >= 1 mm). The threshold file keeps 366 rows per station with the same
  value repeated on every calendar day, so the event-detection join does not
  change.
- Baseline mean TMAX: per station and calendar day, same +/-7-day window.
  Used only for `tmax_anomaly_mean`.
- Percentiles are computed with Spark `percentile_approx` (accuracy 10000),
  so they are approximate, not exact.

## 2. Completeness

Three separate checks, kept distinct:

- **Station baseline eligibility:** the station-variable has at least 80% of
  the 10,958 baseline days AND at least 25 of the 30 baseline years are valid
  years.
- **Valid year:** a year with `days_present_pct` >= 90%. Yearly counts and
  charts use valid years only, so incomplete years, including 2026, are
  excluded.
- **Trend eligibility:** at least 30 valid years. The `trend_eligible` flag
  marks stations that qualify. `years_used` is the number of valid years
  actually used in that station's trend (per station and event type).

## 3. Event definitions

| Event | Variable | Definition |
|---|---|---|
| Warm day (`hot_days`) | TMAX | Above that calendar day's p95 (season-relative) |
| Heatwave / warm spell (`heatwave_*`) | TMAX | 3 or more consecutive warm days |
| Cold day (`cold_days`) | TMIN | Below that calendar day's p5 |
| Cold snap (`coldsnap_*`) | TMIN | 3 or more consecutive cold days |
| Wet day | PRCP | At least 1 mm |
| Heavy rain day (`heavy_rain_days`) | PRCP | Above the station's annual p95 of baseline wet days |
| Dry day | PRCP | Under 1 mm |
| Dry spell (`dry_spell_*`) | PRCP | 10 or more consecutive dry days |
| Longest dry spell (`longest_dry_spell`) | PRCP | Longest run of dry days in the year. Headline dry indicator and the basis for dry trends |
| Compound event (`compound_*`) | TMAX + PRCP | A heatwave overlapping a dry spell |

The warm-day and heatwave thresholds are season-relative: each day is
compared with the 95th percentile for its own calendar day at that station.
There is no absolute temperature threshold, so a warm day in winter can still
be a mild day. The same applies to cold days and cold snaps (5th percentile).

Column names `heatwave_*` and `coldsnap_*` stay unchanged for compatibility.
Legends and tooltips describe heatwaves as "warm spell (unusually warm for the
season)".

## 4. Run rules

- A run is made only of consecutive dated days that meet the condition.
- A day with no valid data (missing value or failed quality flag) ends the
  run. It counts as unknown, not as "dry" or "no event".
- Events are assigned to their start year. A spell crossing New Year counts in
  the year it started.
- Intensity:
  - Heatwave: maximum of (TMAX - threshold) in degrees C.
  - Cold snap: maximum of (threshold - TMIN) in degrees C.
  - Dry spell: duration in days.
- `compound_days` counts only the overlapping days. Those days also stay in
  the heatwave and dry-spell columns, so charts must not stack them.
- `tmax_anomaly_mean`: mean over the year's days of (TMAX - baseline mean
  TMAX for that calendar day), in degrees C.

## 5. Changes to the export tables

- Table A: `extreme_rain_days` is renamed `heavy_rain_days`.
- Table B: event type `extreme_rain` is renamed `heavy_rain`. Add
  `trend_eligible`. For `dry_spell`, the trend is calculated on
  `longest_dry_spell` per year, in days per decade.
- Dashboard: the filter label becomes "Heavy rain (top 5% of wet days)".
  The map for dry spells shows the trend in `longest_dry_spell`, not the raw
  value.

## 6. Known limitations

- **Season-relative warm spells.** A warm spell can start in any season. The
  p95 threshold swings by 21.2 C at NYC (Jan 14.4, Jul 35.6) but only 1.1 C at
  LAX (Jan 27.2, Jul 28.3). A station floor (also above the station's annual
  p90) was tested: 97 to 72 heatwaves, but NYC went 49 to 26 and LAX only
  48 to 46, so it does not work as a global rule. The dashboard offers a
  month/season filter instead.
- **Heavy rain is not "extreme".** The p95 was chosen because p99 leaves dry
  stations with mostly zero counts. Expected heavy-rain days per year (30-year
  baseline): LAX 1.35 (about 26% zero years, Poisson estimate), NYC 5.02
  (under 1%). Because the threshold is annual, events cluster in each
  station's wet season. The earlier +/-7-day rain window was dropped because
  every LAX window had fewer than 100 wet-day observations (median 24) and 26
  had none.
- **Flat dry spells describe the dry regime.** LAX: 7.23 spells of 10+ days
  per year, in 100% of years, median longest spell 145.5 days. NYC: 4.53 per
  year, 97% of years, median longest 16.5 days. A station-relative bar (p90 of
  spell lengths) was tested and flagged every year at both stations, so it
  was not adopted. Use `longest_dry_spell` for trends.
- **Compound events** are expected to be frequent at naturally dry stations.
  Check this when event detection runs.
- **Pilot evidence** comes from two stations. Rerun the checks when more
  stations are added.

## 7. Not adopted / open

- Optional dry-spell year flag (longest spell above the p90 of the station's
  30 yearly maxima, flags about 3 of 30 years): not adopted unless the
  dashboard needs it.
- Risk score formula: decided in a later phase.
