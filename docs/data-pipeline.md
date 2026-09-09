# Data pipeline

Every input is a file under `data/`. This page lists where each file came
from, what it contains and the quirks the parsers have to handle.

```
data/
├── raw/
│   ├── garmin/
│   │   ├── tcx/activity_<id>.tcx        365 files
│   │   ├── gpx/activity_<id>.gpx        365 files
│   │   ├── kml/activity_<id>.kml        365 files
│   │   ├── summarized_activities.json   660 activity summaries, 2016-2021
│   │   ├── daily_summary_<from>_<to>.json  3 files, Dec 2020 - Oct 2021
│   │   └── sleep_<from>_<to>.json          3 files, Dec 2020 - Oct 2021
│   ├── weather/
│   │   ├── hko_daily_weather_extract.json
│   │   ├── weather_history.json
│   │   ├── weather_warning_history.json
│   │   └── sun_moon_rise_set_history.json
│   └── weight/
│       ├── 2021_daily_weight.txt
│       └── activities.xlsx
└── processed/                           git-ignored, rebuilt by the CLI
    ├── activities_tcx.jsonl
    ├── activities_gpx.jsonl
    └── activities_kml.jsonl
```

## Garmin activity exports

**Source.** Each run was exported from Garmin Connect in all three formats.
File names carry the Garmin activity id, which is the join key across
formats.

**Parsers.** `run365days.activities.parsers` has one class per format, all
deriving from `BaseActivityParser`. A parser raises `ValueError` to skip a
file (wrong year or not a running activity) and `parse_all()` swallows
those plus XML errors, so a directory can contain stray files safely.

| Format | What it contributes | Notes |
|---|---|---|
| TCX | lap distance, time and calories; per-point altitude, cumulative distance, speed and cadence | Primary source. `distance_km` is the device figure. Indoor runs have no `Position` element, so `lat`/`lon` are `None`. |
| GPX | per-point ambient temperature (`ns3:atemp`) and cadence | No device distance; `distance_km` is `0.0` and `distance_by_coord_km` is used instead. |
| KML | lap statistics as an HTML table inside the lap placemark; coordinates only | 8 of the 365 files have no `Track Points` folder (treadmill runs) and are skipped, leaving 357. Not used by the dashboard. |

**Timestamps.** `run365days.common.time.parse_datetime` accepts the four
formats that appear across the exports (UTC ISO with milliseconds, ISO with
offset, Unix milliseconds, naive local) and always returns Hong Kong local
time.

**Distance from coordinates.** `run365days.common.geo.total_track_distance`
sums the haversine distance between consecutive points. It returns a numpy
integer for the segment count, which the CLI converts before writing JSON.

## Garmin account export (summaries, daily wellness, sleep)

Garmin's account data export was trimmed to the three files that add
information the activity files lack. Everything else in the export (profile,
consent history, social data, golf, Connect IQ, pre-2021 records and the raw
FIT bundle) was removed from the working tree on `develop` (unreleased) because it is
personal data with no analytical value.

| File | Content | Status |
|---|---|---|
| `summarized_activities.json` | 660 activities from 2016 to 2021 with elevation gain / loss, min / max temperature, location name, steps and stride length | Not yet consumed |
| `daily_summary_*.json` | Daily steps, active and resting kilocalories, intensity minutes, stress and floors climbed | Not yet consumed |
| `sleep_*.json` | Nightly deep / light / awake seconds and sleep window | Not yet consumed |

These are the inputs for the wellness views listed in [roadmap.md](roadmap.md).

## Weather

All weather files are JSON Lines (one object per line) with the column
names produced by the original v1 scrapers. The collectors in
`run365days.weather.collectors` rebuild them with `run365-weather`.

| File | Source | Rows | Fields |
|---|---|---|---|
| `hko_daily_weather_extract.json` | Hong Kong Observatory daily extract (`weather.gov.hk/cis/dailyExtract`) | 363 days | max / avg / min temperature, humidity, rainfall, wind, sunrise / sunset, moon times |
| `weather_history.json` | freemeteo.hk hourly history for Hong Kong (station 10400) | 17,984 hours | temperature, wind, humidity, sky description |
| `weather_warning_history.json` | HKO warning database (`warndb_ea.pl`) | 461 warnings | type, signal, start / end time, icon |
| `sun_moon_rise_set_history.json` | HKO astronomical data | 365 days | sunrise, solar noon, sunset, day length, moonrise, moon transit, moonset, illumination |

**Joining weather to runs.** `dashboard.builder.hourly_at()` picks the
hourly row on the same date whose time is closest to the run's start; there
is no interpolation. Warnings are grouped per date, so a run is tagged with
every signal that was in force at any point that day, not only during the
run. The sun / moon file is currently unused because the HKO daily extract
already carries sunrise and sunset.

**Known gaps.** The daily extract is missing two days at the end of
December and the hourly history stops on 30 December; the builder tolerates
missing rows and simply leaves `wx` as `null`.

## Weight

`2021_daily_weight.txt` is a hand-written log with one line per day in the
form `154.8 lbs (1/5)` (day/month). `run365days.weight.analysis.parse_weight_file`
handles the two quirks of the file: the final line is often written without
a date and is taken as the day after the previous entry, and any line that
matches neither form is ignored. Weight in kilograms and BMI are derived
using `config.BODY_HEIGHT_CM`.

`activities.xlsx` is the spreadsheet the challenge was originally tracked
in (date, time, distance, duration, pace, weight). It is kept as a record
of the source data and is not read by the package.

## Processed output

`data/processed/` is git-ignored and rebuilt by the CLI.

| Command | Output | Consumer |
|---|---|---|
| `run365-export` | `run365.db` (SQLite, about 11 MB) and `static/` (split JSON, about 7 MB: `meta.json`, `activities.json`, `weight.json`, `weather.json`, `warnings.json`, `tracks/<id>.json`) | the GraphQL API reads the database; the static dashboard build fetches the JSON |
| `run365-activities` | `activities_<fmt>.jsonl`, every `Activity` field except the track points | ad-hoc analysis |

Both export writers consume one intermediate structure,
`run365days.export.records.ExportRecords`, so the database and the JSON
files always agree. Tracks are downsampled to 600 points per run at export
time (four times the v2 dashboard's 150); the API can downsample further
per request.
