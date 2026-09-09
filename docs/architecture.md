# Architecture

Run365Days turns three kinds of personal records (Garmin activity exports,
a daily weight log and scraped Hong Kong weather) into a single JSON payload
that a static web dashboard renders. This page describes how the code is
organised and why.

## Package layout

The Python code lives in `run365days/src/` and is imported as `run365days`.
`pyproject.toml` maps the package name onto the `src/` directory
(`package-dir = { "run365days" = "src" }`), so the import path stays short
while the project keeps a conventional src-layout that prevents tests from
importing an uninstalled tree by accident.

Sub-packages are split **by feature**, not by layer. Each feature owns its
models, its readers and its derived metrics:

| Package | Owns | Depends on |
|---|---|---|
| `run365days.common` | `config` (all paths and constants), `geo` (haversine), `time` (timestamp parsing, pace formatting) | nothing inside the project |
| `run365days.activities` | `Activity` / `TrackPoint` dataclasses, `parsers/` for TCX, GPX and KML, `metrics` (MET and kcal) | `common` |
| `run365days.weather` | `HourlyWeather`, `WeatherWarning`, `DailyWeather`, `SunMoon` dataclasses and `collectors/` that scrape HKO and freemeteo | `common` |
| `run365days.weight` | `WeightRecord` and the year table / summaries built from the text log | `common` |
| `run365days.dashboard` | `builder` (pure functions that shape the payload) and `static/index.html` (the dashboard itself) | `activities`, `weight` |
| `run365days.cli` | Three console scripts that wire the features together | everything above |

Dependencies only point downwards in that table. Nothing under `activities`,
`weather` or `weight` knows the dashboard exists, which keeps each feature
testable on its own and leaves room for a second consumer (see
[roadmap.md](roadmap.md)).

## Data flow

```
data/raw/garmin/tcx/*.tcx ─┐
data/raw/garmin/gpx/*.gpx ─┼─► activities.parsers ──► Activity[] ─┐
data/raw/garmin/kml/*.kml ─┘   (one parser per format)            │
                                                                  │
data/raw/weight/2021_daily_weight.txt ─► weight.analysis ─► WeightRecord[]
                                                                  │
data/raw/weather/*.json ────────────────────────── hourly / warnings / daily rows
                                                                  │
                                                                  ▼
                                               dashboard.builder.build_payload()
                                                                  │
                                                                  ▼
                                   run365days/src/dashboard/static/data.js
                                   (window.RUN365 = {...}, ~2.7 MB)
                                                                  │
                                                                  ▼
                                   static/index.html  (Chart.js + Canvas)
```

Two details are worth calling out:

- **TCX is the primary source.** It carries device distance, calories and
  per-point speed, cadence and altitude. GPX is joined by `activity_id`
  only for its per-point ambient temperature. KML is parsed for parity
  but is not used by the dashboard.
- **Weather is joined at build time, not stored per run.** For each
  activity, `builder.hourly_at()` picks the hourly observation nearest to
  the start time and `builder.warnings_by_date()` attaches the HKO signals
  active that day. The raw weather files stay untouched.

## Design decisions

### Feature-based packages instead of `models/ parsers/ analysis/`

The first modular version (tag `v1.5.0`) grouped code by layer. Adding a
data source meant touching four folders. Grouping by feature keeps a source
and everything that understands it in one place, and it maps directly onto
the sub-resources a future API will expose.

### Pure builder functions

Everything in `dashboard.builder` takes plain Python values and returns
plain Python values; file I/O is limited to `load_jsonl` and
`write_data_js`. That is what makes the payload unit-testable without
fixtures on disk (`run365days/tests/test_dashboard_builder.py`).

### A static dashboard first

Version 2 deliberately ships as a single HTML file plus a generated
`data.js`. It needs no server, deploys to GitHub Pages from CI, and can be
handed over as one self-contained `run365days.html`. The trade-off is that
every view has to ship the whole year's data (about 2.7 MB) and that
interactions such as filtering are done client-side. The roadmap moves the
data behind a Flask + GraphQL API for exactly that reason.

### Paths live in one module

`run365days.common.config` is the only place that knows where data is.
`RUN365_DATA_DIR` overrides the root so the same package can run in CI,
in a container or against a different year's exports.

## Quality gates

- **ruff** with `E W F I UP B SIM N D` rule sets, line length 100, Google
  docstring convention. Configuration is in `run365days/pyproject.toml`.
- **pytest** covering geo and time helpers, MET metrics, weight parsing,
  every builder function and the CLI serialiser (53 tests).
- **pre-commit** runs the same ruff checks locally.
- **GitHub Actions** (`.github/workflows/pages.yml`) runs lint and tests on
  every push and pull request to `develop`, then builds and deploys the
  dashboard on pushes only.
