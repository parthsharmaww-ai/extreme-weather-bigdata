# Extreme Event Definitions

All thresholds are computed per station and per calendar day, using a
baseline period, so "extreme" always means extreme relative to that
station's own normal climate.

## Baseline
- Period: 1991 to 2020
- For each station and calendar day, use a window of ±7 days around that day
- Percentiles are computed from this window across all baseline years

## Station completeness rule
A station is kept only if it has:
- at least 80% of days present in the baseline period, and
- at least N years of data (to be agreed by the team)

## Definitions

| Event | Variable | Definition |
|---|---|---|
| Hot day | TMAX | Above the 95th percentile of the baseline |
| Heatwave | TMAX | 3 or more consecutive hot days |
| Cold day | TMIN | Below the 5th percentile of the baseline |
| Cold snap | TMIN | 3 or more consecutive cold days |
| Wet day | PRCP | At least 1 mm |
| Extreme rain day | PRCP | Above the 99th percentile of baseline wet days |
| Dry day | PRCP | Under 1 mm |
| Dry spell | PRCP | 10 or more consecutive dry days |
| Compound event | TMAX + PRCP | A heatwave overlapping a dry spell |

## Recorded for each event
- Start date, end date, duration (days)
- Intensity (for example, mean or maximum anomaly above the threshold)

## Status
Proposed by Parth. To be reviewed by Tanuj and Sarthak before implementation.