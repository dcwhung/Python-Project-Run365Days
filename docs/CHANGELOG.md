# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Release branches
are named `release/vX` and tags `vX.Y.Z`.

## [Unreleased] - `develop`

### Added
- Deployment: `vercel.json`, `requirements.txt` and `scripts/vercel-build.sh`
  run the export at build time and serve the React app in API mode with
  `api/graphql_api.py` as a Python serverless function; the GitHub Pages
  workflow now builds the React app in static mode under `/<repo>/` with a
  `404.html` fallback for deep links.
- React views: Overview (KPIs, daily-distance heatmap, monthly distance and
  pace, recent runs, personal bests, training load, weight vs distance,
  temperature vs pace, weather strip), Activity (pace-coloured Canvas route
  with playback, scrubbing and hover, synced elevation / pace / cadence /
  temperature charts, previous / next navigation, keyboard shortcuts) and
  Activities (filter, sort, CSV export), Year in Review (infographic with
  hours-per-month stairs, diamond-per-day grid, weight summary),
  Performance (KPIs, pace trend, distance histogram, weekday and
  time-of-day pace, cadence vs pace, monthly table), Weight (KPIs, daily and
  7-day average, monthly change, weekday change, weekly km vs change,
  up/down table), Weather Impact (KPIs, HKO range, sky conditions,
  temperature bands, humidity vs pace, warning and extremes tables),
  Training Load (CTL/ATL/TSB, weekly distance with 4-week average and week
  drill-down, weekday distance, biggest weeks) and Settings (preferences
  form, data summary). All nine v2 views now have React equivalents. HKO
  warning icons and the v2 preferences store (localStorage) are ported.
- `frontend/`: Vite + React 19 + TypeScript scaffold for the v3 dashboard
  with a `DataSource` abstraction (GraphQL via graphql-request + Codegen,
  or static JSON), TanStack Query hooks, a TypeScript port of the year
  statistics, Tailwind CSS 4 theme tokens from v2, the nine-view shell and
  the Overview KPI row; 35 Vitest tests; CI job for lint, typecheck, tests
  and both builds.
- `run365-schema`: prints the GraphQL SDL; CI checks
  `frontend/schema.graphql` is current.
- `run365days.api`: Flask application factory with a Strawberry GraphQL
  schema (`meta`, `activities`, `activity`, `weight`, `weather`,
  `warnings`, `year` aggregates) served at `/api/graphql`, reading the
  exported SQLite database read-only; `api/graphql_api.py` is the Vercel entry.
- `run365days.dashboard.stats`: pure year aggregations (totals, monthly,
  weekly, daily distance, training load, personal bests).
- `run365days.export`: one intermediate record set written two ways by
  `run365-export`, a SQLite database (SQLAlchemy 2.0 models) for the API
  and split JSON files for the static dashboard build.
- `run365days.cli` console scripts: `run365-activities`, `run365-weather`,
  `run365-dashboard`, `run365-export`.
- `RUN365_DATA_DIR` environment variable to relocate the data directory.
- Google-style docstrings on every public module, class and function,
  enforced by ruff's pydocstyle rules.
- `.pre-commit-config.yaml` running ruff lint and format.
- A lint-and-test job in GitHub Actions that gates the Pages deployment and
  also runs on pull requests.
- Garmin activity summaries, daily wellness summaries and sleep data for the
  challenge period under `data/raw/garmin/`.
- MIT licence with a personal-data exclusion for `data/`.
- `docs/` with architecture, data-pipeline, changelog and roadmap pages.

### Changed
- Package moved to `src/` at the repository root (with `tests/` and
  `pyproject.toml` beside it) and reorganised by feature
  (`activities`, `weather`, `weight`, `dashboard`, `common`, `cli`) instead
  of by layer (`models`, `parsers`, `collectors`, `analysis`, `export`).
- Raw data moved under `data/raw/{garmin,weather,weight}/` with snake_case
  file names; generated files go to git-ignored `data/processed/`.
- The dashboard page now lives at `src/dashboard/static/index.html`
  and GitHub Pages publishes that folder.
- Code formatted with ruff (line length 100, modern typing syntax).
- README rewritten as a full engineering guide.

### Removed
- The v2 static page (`src/dashboard/static/index.html`), the
  `run365-dashboard` CLI and the `data.js` payload functions
  (`build_payload`, `payload_to_js`, `write_data_js`, `inline_data`); the
  React app has reached parity with all nine views.
- The original Garmin account export folder (profile, consent history,
  social data, golf, Connect IQ, pre-2021 records, raw FIT bundle). Earlier
  tags still contain it; see the privacy note in the README.
- Generated JSON that had been committed (`run365_*_data.json`,
  `DailyWeightExtract.json`).
- The empty `run365days.viz` package.

### Fixed
- `run365-activities` failed with `TypeError: Object of type int64 is not
  JSON serializable` because the coordinate-distance helper returns a numpy
  integer segment count.

### Deprecated
- The v1 scripts are parked in `legacy/` for reference and will be deleted
  once `develop` reaches feature parity.

## [2.0.0] - 2026-09-09 - `release/v2`

Dashboard mockup built on the v1.5 package.

### Added
- Static dashboard (`dashboard/index.html`) with Overview, Year in Review,
  Activity (animated route playback), Performance, Weight, Weather Impact,
  Training Load, Activities and Settings views.
- `run365days.export.dashboard`: pure functions that build the dashboard
  payload from activities, weight and weather, with tests.
- `scripts/build_dashboard.py` producing `data.js` or a self-contained
  `run365days.html`.
- GitHub Pages deployment via GitHub Actions.
- `.gitignore` for `__pycache__` and generated dashboard files.
- HKO warning signals rendered as icons.

## [1.5.0] - 2026-04-03 - `release/v1.5`

First modular package.

### Added
- `run365days` package with `models`, `parsers` (TCX, GPX, KML),
  `collectors` (HKO daily, freemeteo hourly, HKO warnings), `analysis`
  (MET, weight), `utils` (geo, time) and `config`.
- `scripts/process.py` and `scripts/collect_weather.py` CLIs.
- pytest suite for geo, time, MET and weight (51 tests).
- `pyproject.toml`.

## [1.0.0] - 2022-09-28 - `release/v1`

Original standalone scripts written during and after the 2021 challenge.

### Added
- `01`-`04` weather scrapers for freemeteo hourly history, HKO warnings, sun
  and moon times and the HKO daily extract.
- `05_GetDailyWeightSummary.py` for the weight log.
- `Run365Days.py` and `met.py` for activity parsing and MET-based calorie
  estimates.
- Raw Garmin exports and weather JSON.

[Unreleased]: https://github.com/dcwhung/Python-Project-Run365Days/compare/v2.0.0...develop
[2.0.0]: https://github.com/dcwhung/Python-Project-Run365Days/compare/v1.5.0...v2.0.0
[1.5.0]: https://github.com/dcwhung/Python-Project-Run365Days/compare/v1.0.0...v1.5.0
[1.0.0]: https://github.com/dcwhung/Python-Project-Run365Days/releases/tag/v1.0.0
