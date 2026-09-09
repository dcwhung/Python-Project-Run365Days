# Run365Days

[![CI and GitHub Pages](https://github.com/dcwhung/Python-Project-Run365Days/actions/workflows/pages.yml/badge.svg?branch=develop)](https://github.com/dcwhung/Python-Project-Run365Days/actions/workflows/pages.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)

In 2021 I ran every single day. This repository is the analytics side of
that challenge: a Python package that parses 365 Garmin exports, a daily
weight log and a year of Hong Kong Observatory weather, and a dashboard that
puts the results on one page.

**Live dashboard:** https://dcwhung.github.io/Python-Project-Run365Days/

It started as a handful of scripts in 2022 and has been rebuilt twice since:
first into a tested package, then into a feature-organised codebase with a
static dashboard and CI. The next step, an API-backed dashboard, is in
[docs/roadmap.md](docs/roadmap.md).

## Contents

- [What it does](#what-it-does)
- [Stack](#stack)
- [Repository layout](#repository-layout)
- [Getting started](#getting-started)
- [Command-line tools](#command-line-tools)
- [Dashboard](#dashboard)
- [How the data flows](#how-the-data-flows)
- [Testing and code quality](#testing-and-code-quality)
- [Continuous integration and deployment](#continuous-integration-and-deployment)
- [Versioning and branches](#versioning-and-branches)
- [Privacy](#privacy)
- [Documentation](#documentation)
- [Licence](#licence)

## What it does

| Input | What the package extracts |
|---|---|
| Garmin TCX / GPX / KML exports (365 runs) | distance, time, pace, calories, cadence, altitude, per-point speed and ambient temperature, GPS track |
| Daily weight log (365 entries) | weight in lb and kg, BMI, day-over-day change, monthly and weekday trends |
| HKO daily extract, hourly history and warning database | temperature, humidity, wind and rain at run time; warning signals in force that day |
| Garmin account export (trimmed) | activity summaries, daily wellness and sleep for the challenge period, reserved for the next version |

The dashboard combines them into an overview (KPIs, distance heatmap,
monthly totals, personal bests, training load), a year-in-review
infographic, an animated per-run view with route playback and synced
charts, and dedicated performance, weight, weather-impact and training-load
pages.

## Stack

| Area | Choice |
|---|---|
| Language | Python 3.10+ (`X \| None` unions, `dataclasses`) |
| Parsing | `xml.etree.ElementTree` for TCX / GPX / KML, BeautifulSoup + lxml for HTML embedded in KML and for scraping |
| Numerics | pandas and numpy for lap tables, descriptive statistics and date ranges |
| Storage | SQLite via SQLAlchemy 2.0 (typed ORM models), generated at build time |
| API | Flask 3 + Strawberry GraphQL (code-first schema), read-only over the SQLite export |
| Front end (v3, in progress) | React 19 + TypeScript on Vite, TanStack Query + graphql-request with GraphQL Codegen, Tailwind CSS 4; runs in `api` or `static` data mode |
| HTTP | requests |
| Front end | Single HTML page, vanilla JS, Chart.js 4 from cdnjs, Canvas for the route map |
| Packaging | setuptools with a src-layout, console scripts in `pyproject.toml` |
| Quality | ruff (lint, import order, format, docstrings), pytest, pre-commit |
| Delivery | GitHub Actions, GitHub Pages |

## Repository layout

```
pyproject.toml                 package metadata, console scripts, ruff and pytest config
src/                           Python package, imported as `run365days`, one sub-package per feature
  activities/                  Activity / TrackPoint models, parsers/ (tcx, gpx, kml), metrics (MET)
  weather/                     weather models, collectors/ (hko_daily, hourly, warnings)
  weight/                      daily weight parsing, year table, summaries
  dashboard/                   builder (payload functions), static/index.html (the page)
  common/                      config (all paths and constants), geo, time
  cli/                         process_activities, collect_weather, build_dashboard
tests/                         pytest suite, one file per feature module
data/
  raw/garmin/{tcx,gpx,kml}/    365 Garmin activity exports for 2021
  raw/garmin/*.json            activity summaries, daily wellness, sleep
  raw/weather/                 HKO daily extract, hourly history, warnings, sun and moon
  raw/weight/                  daily weight log, original tracking spreadsheet
  processed/                   run365.db, static/*.json, JSON Lines (git-ignored)
frontend/                      React dashboard (v3): src/data (api + static sources), src/views, src/app
docs/                          architecture, data pipeline, changelog, roadmap
legacy/                        original 2022 scripts, reference only
.github/workflows/pages.yml    CI and deployment
LICENSE                        MIT, with a personal-data exclusion for data/
```

## Getting started

```bash
git clone https://github.com/dcwhung/Python-Project-Run365Days.git
cd Python-Project-Run365Days
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install                      # optional: ruff on every commit

pytest tests                 # 93 tests, well under a second
run365-dashboard --single-file          # build the dashboard from data/
open src/dashboard/static/run365days.html
```

The package finds its data relative to the repository. To point it
elsewhere, set `RUN365_DATA_DIR` to a directory with the same
`raw/` and `processed/` structure.

## Command-line tools

Installing the package registers three console scripts. Each can also be
run as `python -m run365days.cli.<module>`.

| Command | What it does | Reads | Writes |
|---|---|---|---|
| `run365-activities --format all --year 2021` | Parse every activity file into normalised records | `data/raw/garmin/{tcx,gpx,kml}/` | `data/processed/activities_<fmt>.jsonl` |
| `run365-weather --source all --year 2021` | Scrape hourly weather, HKO warnings and the HKO daily extract | the web | `data/raw/weather/*.json` |
| `run365-dashboard [--single-file]` | Parse activities, weight and weather and build the dashboard payload | `data/raw/**` | `src/dashboard/static/data.js` and optionally `run365days.html` |
| `run365-export [--points 600]` | Parse everything once and write the processed data set for the API and the static build | `data/raw/**` | `data/processed/run365.db` and `data/processed/static/*.json` |

### GraphQL API

```bash
run365-export                                   # build data/processed/run365.db once
flask --app run365days.api.app:create_app run   # GraphiQL at http://127.0.0.1:5000/api/graphql
```

The schema exposes `meta`, `activities(fromDate, toDate, minKm, hasGps)`,
`activity(id)` with a downsampled `track(points)`, `weight`, `weather`,
`warnings` and a `year` aggregate (totals, monthly, weekly, daily distance,
training load, personal bests). `api/graphql.py` exports the same app for
Vercel; `RUN365_DB_PATH` points it at the bundled database.

### React front end (v3)

```bash
cd frontend && npm install
npm run dev                       # http://localhost:5173, /api proxied to Flask on :5000
npm test                          # vitest (runs codegen first)
npm run build                     # api mode  -> dist/
npm run build:static              # static mode -> dist/, reads /data/*.json
```

The app talks to one `DataSource` interface. `VITE_DATA_MODE=api` (default)
queries the GraphQL endpoint with documents type-checked against
`frontend/schema.graphql` (written by `run365-schema`, checked in CI).
`VITE_DATA_MODE=static` fetches the JSON written by
`run365-export --static-dir frontend/public/data` and computes the year
aggregates in the browser with a TypeScript port of `dashboard/stats.py`;
both ports share the same test fixtures. Views never know which mode is
active.

`run365-dashboard` options: `--year`, `--tcx-dir`, `--gpx-dir`,
`--weight-file`, `--points` (track points kept per run, default 150),
`--out`, `--single-file`.

## Dashboard

`src/dashboard/static/index.html` is the whole front end. It
reads one script, `data.js`, which assigns the payload to `window.RUN365`.
Chart.js is loaded from cdnjs; the route map and per-run charts are plain
Canvas and work offline. `--single-file` inlines the payload so the page can
be shared as a single HTML file.

Views:

- **Overview**: KPIs, distance heatmap (click a day to open it), monthly
  distance and pace, recent runs with weather and HKO warning icons,
  personal bests, 42-day / 7-day training load, weight against distance,
  temperature against pace.
- **Year in Review**: the year-end infographic with days, distance, monthly
  time, calories and weight change.
- **Activity**: any of the 365 runs with a pace-coloured route that draws
  itself from start to finish (play, speed, scrub, hover) and synced
  elevation, pace, cadence and temperature charts with a live readout.
  Indoor runs show the charts without a map.
- **Performance, Weight, Weather Impact, Training Load, Activities,
  Settings**: one page per topic.

## How the data flows

```
raw Garmin files ──► activities.parsers ──► Activity[]        ─┐
weight log       ──► weight.analysis    ──► WeightRecord[]    ─┼─► dashboard.builder ──► data.js ──► index.html
weather JSONL    ──► (loaded as rows)                          ─┘
```

TCX is the primary source for every run. GPX is joined by activity id for
its per-point ambient temperature. For each run the builder attaches the
hourly observation nearest to the start time and the HKO warning signals in
force that day. Every builder function is pure, so the payload is covered
by unit tests without files on disk. The full account, including the
quirks of each source, is in [docs/data-pipeline.md](docs/data-pipeline.md)
and the reasoning behind the layout in
[docs/architecture.md](docs/architecture.md).

## Testing and code quality

```bash
pytest tests
ruff check src tests
ruff format --check src tests
```

- 93 tests cover the geo and time helpers, MET and calorie maths, weight
  parsing (including the undated-last-line quirk), every dashboard builder
  function, the export records, SQLite and JSON writers, and the CLI
  serialiser.
- Front end: 35 Vitest tests cover the TypeScript stats port (pinned to the
  Python numbers), the static JSON mappers and source, the API source,
  data-mode resolution, formatting helpers and the app shell.
- ruff enforces pycodestyle, pyflakes, isort, pyupgrade, bugbear,
  simplify, pep8-naming and Google-style docstrings on every public
  symbol. Line length is 100.
- `.pre-commit-config.yaml` runs the same checks locally; `legacy/` is
  excluded everywhere.

## Continuous integration and deployment

`.github/workflows/pages.yml` runs on every push and pull request to
`develop`:

1. **Lint and test**: install the package, `ruff check`, `ruff format
   --check`, `pytest`, and check `frontend/schema.graphql` matches the
   Strawberry schema.
2. **Frontend**: `npm ci`, ESLint, codegen + `tsc`, Vitest, and a Vite
   build in both data modes.
3. **Build dashboard** (push only): `run365-dashboard --single-file`, then
   upload `src/dashboard/static/` as the Pages artifact.
4. **Deploy to GitHub Pages** (push only). Until the React views reach
   parity this still publishes the v2 page.

Generated files (`data.js`, `run365days.html`, `data/processed/`) are
git-ignored and rebuilt on every deploy.

## Versioning and branches

| Branch | Tag | Contents |
|---|---|---|
| `master` | `v1.0.0` | Original 2022 scripts and raw data |
| `release/v1.5` | `v1.5.0` | First modular package with tests |
| `release/v2` | `v2.0.0` | Static dashboard mockup on top of v1.5 |
| `develop` | unreleased | Feature-organised package, CI, docs; deploys the live dashboard |

Release branches are frozen snapshots. New work lands on `develop`; the
history is in [docs/CHANGELOG.md](docs/CHANGELOG.md).

## Privacy

`data/` contains my own activity, weight and scraped weather records and is
kept in the repository so the project builds end to end. The Garmin account
export was trimmed on `develop` to the three files the roadmap needs;
earlier tags still contain the full export (profile, device and consent
records) because the history has not been rewritten. Treat everything under
`data/` as personal data: see the exclusion at the end of [LICENSE](LICENSE).

## Documentation

| Page | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Package layout, dependency rules, data flow, design decisions |
| [docs/data-pipeline.md](docs/data-pipeline.md) | Every data source: origin, format, row counts, parser quirks |
| [docs/CHANGELOG.md](docs/CHANGELOG.md) | What changed in each version |
| [docs/roadmap.md](docs/roadmap.md) | Flask + GraphQL API, wellness views, smaller items |
| [legacy/README.md](legacy/README.md) | Map from the 2022 scripts to their replacements |

## Licence

Code is released under the [MIT License](LICENSE). The personal records
under `data/` are excluded; see the note at the end of that file.
