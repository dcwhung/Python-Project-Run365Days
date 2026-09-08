# Run365Days

Personal analytics for a #365DaysChallenge — one run every day of 2021.
Garmin exports (TCX / GPX / KML), daily weight and Hong Kong Observatory
weather are parsed into a Python package and rendered as a static dashboard.

## Layout

```
run365days/          library: parsers, collectors, analysis, export
  parsers/           TCX / GPX / KML → Activity dataclass
  collectors/        freemeteo hourly weather, HKO warnings, HKO daily extract
  analysis/          MET / kcal, daily weight
  export/dashboard   builds the dashboard payload
scripts/             CLIs (process.py, collect_weather.py, build_dashboard.py)
dashboard/           static web dashboard (index.html + generated data.js)
tests/               pytest suite
Garmin/, Weather/    raw data
```

## Setup

```bash
pip install -e ".[dev]"     # or: pip install pandas numpy pytz beautifulsoup4 requests pytest
pytest
```

## Dashboard

Live site: **https://dcwhung.github.io/Python-Project-Run365Days/**

GitHub Pages is built by `.github/workflows/pages.yml`: every push that touches
the data, the package or `dashboard/index.html` runs the tests, regenerates the
data file and deploys the `dashboard/` folder. The generated files
(`dashboard/data.js`, `dashboard/run365days.html`) are git-ignored — never
commit them.

Local build:

```bash
python scripts/build_dashboard.py                # → dashboard/data.js
python scripts/build_dashboard.py --single-file  # → dashboard/run365days.html (self-contained)
```

Open `dashboard/index.html` next to `data.js`, or share `dashboard/run365days.html`
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

`build_dashboard.py` options: `--year`, `--tcx-dir`, `--gpx-dir`,
`--weight-file`, `--points` (track points kept per run, default 150), `--out`.
