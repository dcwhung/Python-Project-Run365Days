# Run365Days

[![CI and GitHub Pages](https://github.com/dcwhung/Python-Project-Run365Days/actions/workflows/pages.yml/badge.svg?branch=develop)](https://github.com/dcwhung/Python-Project-Run365Days/actions/workflows/pages.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](run365days/pyproject.toml)

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
| HTTP | requests |
| Front end | Single HTML page, vanilla JS, Chart.js 4 from cdnjs, Canvas for the route map |
| Packaging | setuptools with a src-layout, console scripts in `pyproject.toml` |
| Quality | ruff (lint, import order, format, docstrings), pytest, pre-commit |
| Delivery | GitHub Actions, GitHub Pages |

## Repository layout

```
run365days/                    Python project
  pyproject.toml               metadata, console scripts, ruff and pytest config
  src/                         imported as `run365days`, one sub-package per feature
    activities/                Activity / TrackPoint models, parsers/ (tcx, gpx, kml), metrics (MET)
    weather/                   weather models, collectors/ (hko_daily, hourly, warnings)
    weight/                    daily weight parsing, year table, summaries
    dashboard/                 builder (payload functions), static/index.html (the page)
    common/                    config (all paths and constants), geo, time
    cli/                       process_activities, collect_weather, build_dashboard
  tests/                       pytest suite, one file per feature module
data/
  raw/garmin/{tcx,gpx,kml}/    365 Garmin activity exports for 2021
  raw/garmin/*.json            activity summaries, daily wellness, sleep
  raw/weather/                 HKO daily extract, hourly history, warnings, sun and moon
  raw/weight/                  daily weight log, original tracking spreadsheet
  processed/                   generated JSON Lines (git-ignored)
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
pip install -e "./run365days[dev]"
pre-commit install                      # optional: ruff on every commit

pytest run365days/tests                 # 53 tests, well under a second
run365-dashboard --single-file          # build the dashboard from data/
open run365days/src/dashboard/static/run365days.html
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
| `run365-dashboard [--single-file]` | Parse activities, weight and weather and build the dashboard payload | `data/raw/**` | `run365days/src/dashboard/static/data.js` and optionally `run365days.html` |

`run365-dashboard` options: `--year`, `--tcx-dir`, `--gpx-dir`,
`--weight-file`, `--points` (track points kept per run, default 150),
`--out`, `--single-file`.

## Dashboard

`run365days/src/dashboard/static/index.html` is the whole front end. It
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
pytest run365days/tests
ruff check run365days/src run365days/tests
ruff format --check run365days/src run365days/tests
```

- 53 tests cover the geo and time helpers, MET and calorie maths, weight
  parsing (including the undated-last-line quirk), every dashboard builder
  function and the CLI serialiser.
- ruff enforces pycodestyle, pyflakes, isort, pyupgrade, bugbear,
  simplify, pep8-naming and Google-style docstrings on every public
  symbol. Line length is 100.
- `.pre-commit-config.yaml` runs the same checks locally; `legacy/` is
  excluded everywhere.

## Continuous integration and deployment

`.github/workflows/pages.yml` runs on every push and pull request to
`develop`:

1. **Lint and test**: install the package, `ruff check`, `ruff format
   --check`, `pytest`.
2. **Build dashboard** (push only): `run365-dashboard --single-file`, then
   upload `run365days/src/dashboard/static/` as the Pages artifact.
3. **Deploy to GitHub Pages** (push only).

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
