# Run365Days

Personal analytics for a #365DaysChallenge — one run every day of 2021.
Garmin exports (TCX / GPX / KML), daily weight and Hong Kong Observatory
weather are parsed into a Python package and rendered as a static dashboard.

## Layout

```
run365days/                    Python project (pip install -e "./run365days[dev]")
  pyproject.toml               package metadata, CLI entry points, ruff + pytest config
  src/                         imported as `run365days`, organised by feature
    activities/                TCX / GPX / KML parsers, Activity model, MET metrics
    weather/                   HKO + freemeteo collectors and weather models
    weight/                    daily weight parsing and BMI
    dashboard/                 payload builder + static/index.html (the web dashboard)
    common/                    config (data paths), geo and time helpers
    cli/                       run365-activities, run365-weather, run365-dashboard
  tests/                       pytest suite, one file per feature module
data/
  raw/garmin/{tcx,gpx,kml}/    366 Garmin activity exports for 2021
  raw/garmin/*.json            Garmin activity summaries, daily summaries, sleep
  raw/weather/                 HKO daily extract, hourly history, warnings, sun/moon
  raw/weight/                  daily weight text file, activity spreadsheet
  processed/                   generated JSON Lines (git-ignored, rebuilt by the CLI)
legacy/                        original v1 scripts, reference only (see tag v1.0.0)
docs/                          project documentation
.github/workflows/pages.yml    CI (ruff + pytest) and GitHub Pages deploy
```

Version history: `release/v1` (tag `v1.0.0`) holds the original standalone
scripts, `release/v1.5` (`v1.5.0`) the first modular package, `release/v2`
(`v2.0.0`) the dashboard mockup. `develop` is the integration branch.

## Setup

```bash
pip install -e "./run365days[dev]"
pre-commit install                 # optional: run ruff on every commit
pytest run365days/tests
ruff check run365days/src run365days/tests
```

Data paths default to `<repo>/data`; set `RUN365_DATA_DIR` to override.

## CLI

```bash
run365-activities --format all --year 2021   # data/raw/garmin -> data/processed/*.jsonl
run365-weather --source all --year 2021      # scrape weather into data/raw/weather
run365-dashboard --single-file               # build the dashboard data file
```

Each command also runs as `python -m run365days.cli.<module>`.

## Dashboard

Live site: **https://dcwhung.github.io/Python-Project-Run365Days/**

GitHub Pages is built by `.github/workflows/pages.yml`: every push to `develop`
runs ruff and pytest, regenerates the data file and deploys
`run365days/src/dashboard/static/`. The generated files (`data.js`,
`run365days.html`) are git-ignored; never commit them.

Local build:

```bash
run365-dashboard                # -> run365days/src/dashboard/static/data.js
run365-dashboard --single-file  # -> run365days/src/dashboard/static/run365days.html
```

Open `static/index.html` next to `data.js`, or share `run365days.html`
(everything inlined). Chart.js is loaded from cdnjs, so an internet connection
is needed for the Chart.js charts; the route map and activity charts are plain
Canvas and work offline.

Views:

- **Overview** — KPIs, distance heatmap (click a day to open it), monthly
  distance and pace, recent runs with weather and HKO warning tags, personal
  bests, 42/7-day training load, weight vs distance, temperature vs pace.
- **Year in Review** — the year-end infographic: days, distance, monthly
  time, calories, weight loss.
- **Activity** — any of the 365 runs: pace-coloured route that draws itself
  from start to finish (play / speed / scrub / hover), synced elevation,
  pace, cadence and temperature charts with a live readout. Indoor runs show
  the charts without a map.

`run365-dashboard` options: `--year`, `--tcx-dir`, `--gpx-dir`,
`--weight-file`, `--points` (track points kept per run, default 150), `--out`,
`--single-file`.
