# Architecture

Run365Days turns three kinds of personal records (Garmin activity exports,
a daily weight log and scraped Hong Kong weather) into a single JSON payload
that a static web dashboard renders. This page describes how the code is
organised and why.

## Package layout

The Python code lives in `src/` at the repository root and is imported as
`run365days`. `pyproject.toml` maps the package name onto the `src/` directory
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
| `run365days.dashboard` | `builder` (pure per-run calculations shared by export and API) and `stats` (year aggregates) | `activities`, `weight` |
| `run365days.export` | `records` (one intermediate structure), `models` (SQLAlchemy), `sqlite` and `static_json` writers | `dashboard.builder`, `activities`, `weight` |
| `run365days.api` | `app` (Flask factory), `schema` (Strawberry types and `Query`), `service` (SQLAlchemy queries), `db` (engine and session helpers) | `export.models`, `dashboard.stats` |
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
                                          export.records.build_records()  (run365-export)
                                                   │                  │
                                                   ▼                  ▼
                                   data/processed/run365.db    data/processed/static/*.json
                                   (SQLite, ~11 MB)            (split JSON, ~7 MB)
                                                   │                  │
                                                   ▼                  ▼
                                   api.app  Flask + Strawberry    frontend static mode
                                   /api/graphql  (Vercel)         (GitHub Pages)
                                                   │                  │
                                                   └──────► frontend/ React ◄──────┘
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

## Front end (v3)

`frontend/` is a Vite + React + TypeScript app. Everything a view renders
comes through one interface, `DataSource` (`frontend/src/data/types.ts`):

| Mode | Source | Aggregates |
|---|---|---|
| `api` (default) | `src/data/api/source.ts`: graphql-request against `/api/graphql`; documents in `src/data/api/queries.ts` are type-checked by GraphQL Codegen against `schema.graphql` | computed by the API (`dashboard.stats`) |
| `static` | `src/data/static/source.ts`: fetches the JSON written by `run365-export --static-dir` and maps snake_case to the same types | computed in the browser by `src/data/stats.ts`, a port of `dashboard.stats` pinned to the same test values |

Views live in `src/views/<view>/` with a pure `model.ts` (filtering,
sorting, derived numbers) beside the components, so the logic is tested
without rendering. Charts use Chart.js through react-chartjs-2; the route
map and the four per-run series charts are hand-drawn on Canvas because they
redraw sixty times a second during playback.

TanStack Query hooks in `src/data/hooks.ts` key every query by mode.
`DataProvider` picks the source from `VITE_DATA_MODE`, so the Vercel build
and the GitHub Pages build differ only by an environment variable.

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
fixtures on disk (`tests/test_dashboard_builder.py`).

### One front end, two data modes

Version 2 was a single HTML file plus a generated `data.js`: no server, but
every visitor downloaded the whole year (about 2.7 MB, 94 % of it GPS
tracks) before seeing anything. Version 3 keeps the zero-infrastructure
demo and adds the API: the React app is built twice from the same source,
once for GitHub Pages reading split JSON (a track is fetched only when its
run is opened) and once for Vercel querying the GraphQL endpoint. The
`DataSource` interface is the seam; the year statistics exist in Python and
in TypeScript with shared test fixtures so both modes agree.

### Build-time data, read-only runtime

Parsing 730 Garmin files takes about 50 seconds, far too long for a
serverless function. `run365-export` therefore runs at build time on both
platforms and the API only reads the SQLite file it produced. Nothing at
runtime writes to disk, which is exactly what Vercel's read-only filesystem
requires.

### Paths live in one module

`run365days.common.config` is the only place that knows where data is.
`RUN365_DATA_DIR` overrides the root so the same package can run in CI,
in a container or against a different year's exports.

## Quality gates

- **ruff** with `E W F I UP B SIM N D` rule sets, line length 100, Google
  docstring convention. Configuration is in `pyproject.toml`.
- **pytest** covering geo and time helpers, MET metrics, weight parsing,
  every builder function, the year statistics, the export writers, the
  GraphQL API (Flask test client against a temporary database) and the
  CLI serialiser (93 tests).
- **pre-commit** runs the same ruff checks locally.
- **GitHub Actions** (`.github/workflows/pages.yml`) runs lint and tests on
  every push and pull request to `develop`, then builds and deploys the
  static dashboard on pushes only. Vercel builds the API-mode deployment
  from the same commits; both are described in [deployment.md](deployment.md).
