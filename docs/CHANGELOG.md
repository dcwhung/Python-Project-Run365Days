# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Release branches
are named `release/vX` and tags `vX.Y.Z`.

## [3.3.0] - 2026-09-17 - `develop`

Six tickets and twenty-one review items, in three batches. One change is
breaking: a coordinate outside the range of the globe is now refused rather
than turned into a number that looks like a distance. The rest close the
supply-chain gate CUI-0036 left half-built, give a bounds refusal a
machine-readable name for the argument it refused, and pay off a run of
figures that had been quoted in comments with nothing holding them to a
number.

### Breaking
- `haversine_distance` screens for a *range* now, not only for finiteness: a
  latitude outside `-90..90` or a longitude outside `-180..180` raises
  `ValueError` reading `out-of-range coordinate`, so `parse_all` logs the file
  by name at WARNING and drops it rather than letting it contribute. Before,
  `lat=400` answered 4,447.8 km and nothing downstream could tell that from a
  real reading; push the magnitude to around `1e308` -- finite, so CUI-0008's
  screen passed it -- and `np.sin` overflowed back to `nan`, which `pandas`
  drops out of a sum, reopening the exact CUI-0001 shape where a track
  silently reports short. The bounds are inclusive: the poles and the
  antimeridian are real places and stay valid. The four range tests *replace*
  the four `math.isfinite` calls rather than joining them -- `abs(nan)` is
  `nan` and loses every comparison, and no infinity survives `<= 90.0` -- so
  the check count is unchanged and the `nan`/`±inf` message is unchanged byte
  for byte. On the real export (356 tracks, 97,003 points) the old and new
  guards answer identically for every one, so this breaks corrupt input only
  (CUI-0047).

### Added
- A bounds refusal now carries `extensions.argument`, naming which argument
  was refused: `"limit"`, `"offset"` or `"points"`. `path`, `locations` and
  `extensions.code` are identical for `limit` and `offset` on the same field,
  so branching on the code -- which the SDL tells clients to do -- could not
  tell a client which of the two to correct. The value is a hard-coded literal
  at each of the three raise sites, never interpolated from the request, and
  it is the schema name rather than the alias (CUI-0050, following W-035).
- `pip-audit` runs in CI, as the last step of `lint-test`, preceded by
  `python -m pip install --upgrade pip setuptools`. It audits the whole
  environment -- the ten runtime dependencies, the `[dev]` extras and
  pip-audit's own closure -- and reports none. Flask and Strawberry are core
  dependencies that go straight into the Vercel runtime, and nothing had been
  auditing them. An `--ignore-vuln` allowlist was considered and rejected on
  the grounds W-034 and S-095 already established: a list of exceptions drifts,
  and the gate then reports on the list rather than on the tree (CUI-0044).

### Changed
- `npm audit` runs over the whole tree; the `--omit=dev` CUI-0036 shipped with
  is gone. The fourteen advisories that flag hid were real, and they are fixed
  rather than filtered: `@graphql-codegen/cli` 5 to 7, `client-preset` 4 to 6
  and `vitest` 3 to 5 take `lodash` out of the dependency tree entirely, and
  the full-tree audit now reports zero. This closes the last open half of
  CUI-0036 (CUI-0049).
- `client-preset` 6 no longer emits schema-wide types. `Maybe`, `InputMaybe`,
  `Scalars` and every schema type are gone from `frontend/src/gql/graphql.ts`,
  which keeps operation and fragment types and the `*Document` constants. Only
  `frontend/src/data/api/queries.ts` imports from `@/gql` today, and only for
  the `graphql` helper, so `tsc -b` is unaffected; an `import type { Activity }
  from "@/gql"` added later will not resolve and belongs in
  `src/data/types.ts`. An `ID` input is now `string | number` (CUI-0049).

### Fixed
- The key set a refusal publishes in `extensions` has a gate. CUI-0050 turned
  a literal `extensions={code}` -- where "nothing but the code" held by
  construction -- into a dict built up across two statements, which makes the
  invariant one that can be lost; a mutant adding a third key left all 573
  tests of the day green and still reached the wire. The existing guard is a
  denylist of three counter names, so a key under any other spelling walked
  past it. The new test is an allowlist over the same documents the codes and
  the arguments are held over, and it fails at every one of its parameters
  under that mutant (W-050).

### Documented
- `tracks()` treats `-0.0` as zero, which is what the Args section always
  said; `-0.0 < 0` is `False` and `-0.0` is falsy, so both paths agree. That
  was filed as a defect and settled as not one, with tests pinning the five
  values that mean "every row" and the negatives that are refused. A `float`
  that reaches the sampler raises `TypeError`, which nothing had written down
  -- and it is any such `float`, not only a fractional one: `2.0` raises as
  surely as `2.5`, while `1.5`, `1000.5` and `0.0` do not, each for a
  different reason now recorded in the docstring (CUI-0048, S-106, S-130).
- The SDL says which of `limit` and `offset` a refusal names when both are
  out of range -- `limit` is checked first -- and where a request with several
  bad windows is refused: at the first such field in document order, not at a
  privileged field. Both are pinned by tests that run the orders in both
  directions, because a test on one order records the wrong rule and stays
  green (S-103, S-104, S-129).
- Figures quoted in comments are pinned to the commit they were measured at,
  or removed. The CUI-0038 comment no longer quotes byte counts without saying
  which document produced them; `CLAUDE.md` §6 no longer offers codegen line
  counts as a baseline, since they were short by the six-line preamble and
  even the percentage moves with the counting method; and three suite sizes
  quoted in `tests/test_api.py` now name the commit where collecting the suite
  reproduces them (CUI-0051, W-060, W-061, S-105, S-128).
- `docs/deployment.md` states what the audit gates actually cover, including
  pip-audit's own closure, and that both sit upstream of `deploy` -- so an
  advisory in a package this project never touches can hold a deployment,
  including a hotfix. `CLAUDE.md` §3 carries the pip upgrade line that CI runs,
  without which the same command reports fourteen advisories locally while CI
  is green (S-120, S-121, S-122).

### Known
- Both supply-chain gates run upstream of `deploy`, and this repository
  deploys on every push to `develop`. The trade is deliberate and both
  documents now say so, but it means an unrelated upstream advisory can stop
  every deployment. Decoupling it would mean a separate `schedule:` workflow;
  `continue-on-error` is not the answer, since it retires the gate rather than
  moving it (CUI-0052, open).

## [3.2.0] - 2026-09-16 - `develop`

Twelve tickets and twenty-seven review items, in four batches. Three of the
changes are breaking: two of them close a cost a public unauthenticated
endpoint was paying, and the third makes a ceiling bound what a request
receives rather than only what it may ask for.

### Breaking
- A single request may now read at most 4,000 list rows across every list
  field it names, `MAX_LIST_ROWS_PER_REQUEST`. Nothing bounded the list
  fan-out before: 166 aliased `activities` fields carrying no track at all
  took about 3.1 s on the real export, eleven times the worst legal track
  shape and within 4.9x of the 15 s function limit, on a public endpoint
  that needs no authentication. The budget is charged before the SQL runs
  and counts rows materialised rather than rows scanned, so a refusal costs
  nothing. A document that reads more than 4,000 rows, legal until now, is
  refused with a readable sentence (CUI-0027).
- `track(points: 0)` is refused in both deployment modes, with the same
  sentence. The static mode returned every stored point for `0` while the
  API refused it -- the API's bound is what closed AU-001's denial-of-service
  vector, so the static mode is what moves. Non-integer and beyond-`Int32`
  values are likewise refused on both sides now; their two messages differ,
  which the source records deliberately, because the API refuses them in
  `Int` coercion before any resolver sees them (CUI-0025).
- `track()` with `points` omitted returns at most `MAX_TRACK_POINTS` rows.
  `track(id, 1200)` was refused while `track(id)` returned 1,200, so the
  constant bounded what could be asked for and not what came back. No
  currently exported track exceeds 600 rows, so today's data is unchanged --
  verified point for point over 134,041 track rows across all 365
  activities, and again against the static writer, which this change does
  not touch. An export built with `--points` above 1,000 would be capped
  where it previously was not (CUI-0033).

### Fixed
- A budget refusal no longer writes a traceback to the server log. Strawberry
  logged the `ValueError` these budgets raised as an unexpected error, at
  `ERROR` with `exc_info`, which put nine frames of absolute source paths --
  the repository layout and the interpreter's `site-packages` directory among
  them -- into the log for every refused request, on a public endpoint, under
  no authentication and in a deployment's default logging configuration. The
  refusal now carries the GraphQL error itself and is logged at `INFO`, below
  the threshold an unconfigured deployment applies, so what the log gains per
  refused request goes to zero rather than to one line. The client is told
  exactly what it was told before (CUI-0029).
- Every view routes a failed query through `readableError()` instead of
  rendering `error.message`. Four views put the raw message on the page: a
  serialised blob of up to about 1,250 characters carrying the whole GraphQL
  document and its variables' values (CUI-0030).

### Added
- `MAX_TRACK_POINTS` is now held across both languages in both directions.
  Changing the TypeScript copy already went red; changing the Python one and
  regenerating the SDL went green with the two sides disagreeing. The
  generated `schema.graphql` is the link that closes it (CUI-0034).
- `SAMPLE_KEY_SEPARATOR`'s load-bearing invariant has tests. It could be set
  to a decimal digit, or dropped entirely, with the whole suite still green
  (CUI-0028).
- `pytest-cov` is in the `dev` extra with its coverage configuration, rather
  than being assumed present (CUI-0032).
- `docs/deployment.md` says how to see budget refusals in a deployment's log,
  with a snippet that works as written -- they are below the default
  threshold by design (S-076).

### Documented
- The test inventory in `README.md` and `docs/architecture.md` reads as the
  partial list it is, and points at `tests/` as the source of truth rather
  than repeating counts that drift (CUI-0031, CUI-0024).
- Twenty-seven review items, almost all of them a sentence that explained why
  correct code was correct and got the reason wrong: a wall clock bound to
  the wrong document, a cost attributed to per-field dispatch that the
  measurements put at 2% rather than half, a byte size quoted from a minimal
  reproduction as though it were the real file, a fixture described as
  captured from a real run that was built by hand (S-053 through S-060,
  S-063 through S-082, W-027 through W-029).
- `graphql-core` is declared third-party so its import sorts with the other
  packages. Ruff read a bare `graphql` import as this repository's own module,
  because `api/` is on its source path and Vercel matches only
  `api/graphql.py` as a function (S-077).

### Known
- The budget refusals are fixed; the bounds refusals beside them are not.
  `_page` and `_track_points` still raise a bare `ValueError`, so
  `{ activities(offset: -1) { id } }` -- 33 bytes, eleven tokens, no
  authentication -- still writes 2,016 bytes of traceback with the same nine
  absolute paths. That is about five times cheaper to trigger than the
  cheapest document that reaches a budget refusal, five aliased `activities`
  fields at `limit: 1000`: eleven tokens against fifty-seven, 33 bytes against
  173 (CUI-0037). Corrected on 2026-09-17: this bullet first said seven tokens
  and 140 times cheaper. Seven was a miscount, and 140 was 998 divided by it --
  998 being the size of CUI-0027's worst-case query-cost document rather than
  the cost of triggering a budget refusal, so the two were never like for like.
  CUI-0037 carries the working; the conclusion it draws is unchanged.
- Two mutants survive the whole suite behind 100% line coverage on
  `api/schema.py`: raising `REFUSAL_LOG_LEVEL` to `WARNING` undoes what
  CUI-0029 buys in an unconfigured deployment, and handing `process_errors`
  the unfiltered list puts refusals back on `ERROR`. No test names an
  operation carrying a refusal and a fault at once, which is the only shape
  that tells the two lists apart (CUI-0038, CUI-0039).
- The Vercel entry's start-up failure path has no tests and is outside the
  coverage source, so the code that explains a broken deploy is unguarded
  (CUI-0035). `npm audit` runs in no gate; the fourteen advisories it reports
  today are all in the build toolchain, and `--omit=dev` reports none
  (CUI-0036).

## [3.1.1] - 2026-09-16 - `develop`

Nine tickets from the pending queue, cleared in three batches. Two behaviour
changes; everything else is documentation the measurements had outgrown, and
the tests that now hold those claims to the code.

### Fixed
- `_sample_filter` no longer builds one `OR` arm per track. SQLite had to
  evaluate all of them against every row of the `row_number()` subquery, so
  the batch AU-050 introduced cost O(batch squared): 64 tracks took 140 ms
  against the real export where the per-track implementation it replaced took
  65 ms, and 365 tracks took 3.8 s. One `IN` over a composite key restores a
  flat cost per row -- 64 tracks in 41 ms, 0.62x of the pre-AU-050 figure at
  every width measured -- while keeping the two statements AU-050 bought. The
  batch reads fewer bound parameters than before, not more (CUI-0019).
- `parse_datetime` converts an ISO timestamp carrying an offset into the
  requested zone, and recognises a negative one. The branch tested for `"+"`,
  so `-05:00` reached neither it nor the UTC branch and died in the naive
  `strptime` below, taking the whole file with it. GPX and TCX write UTC `Z`
  and were converted; KML writes the local offset and was not, so the three
  exports of one run disagreed by an hour anywhere outside Hong Kong -- and
  `_TIMESTAMP_FORMAT` carries no offset, so the evidence was gone by the time
  the row was written. Verified byte-identical over 1,087 activity records and
  328,752 track points (CUI-0006).
- The static sampler rounds half to even, as Python does, instead of half up.
  22.6% of the `(length, points)` pairs the sampler can be asked for over the
  123 track lengths the export holds land on a tie, where the two deployment
  modes drew different indices. None is reachable while `TRACK_POINTS` is 600,
  which is both above every stored track and even -- an odd `limit` is what
  makes a tie possible at all (CUI-0021).

### Documented
- `track(points: 1)` returns the last row alone rather than the first, in the
  docstring and in the SDL description, matching `downsample` deliberately
  (CUI-0004).
- `MAX_QUERY_DEPTH` stays 5 and says outright that it cannot fire against this
  schema; an alias flood is wide rather than deep, and `MAX_QUERY_TOKENS` is
  what refuses one. Two tests hold the limiter to its edge, one of which goes
  red if anyone deepens the type graph (CUI-0003).
- Every wall clock quoted by `MAX_TRACK_FIELDS_PER_REQUEST` names the scale it
  was read at, and the Vercel headroom is read off the export rather than
  inferred from a 600-row fixture (CUI-0020, CUI-0017).
- A missing string column reads as empty text rather than `None`, recorded
  where the choice is made and pinned by tests (CUI-0014).
- The CI prose no longer repeats `pages.yml`'s branch list or job steps, which
  had gone stale twice in one session, and points at the workflow instead
  (CUI-0015).

### Known
- Nothing bounds the list fan-out: 166 aliased `activities` fields with no
  track at all take about 3.1 s on the real export, eleven times the worst
  legal track shape and within 4.9x of the 15 s function limit (CUI-0027).

## [3.1.0] - 2026-09-15 - `develop`

### Added
- `Activity.track` resolves a page of activities in two statements instead of
  two per field, using `row_number()` over a per-activity partition. Sample
  positions are still computed in Python, so the reference downsampler stays
  the single sampler and the export is byte-identical (AU-050).

### Fixed
- The activity view no longer reports a failed track query as "Activity not
  found." The header, weather and KPIs the activity query already returned
  stay on screen and only the map and charts are replaced, carrying the
  GraphQL message rather than the serialised request (CUI-0016).
- `parse_datetime` converts 13-digit epoch milliseconds into the target zone
  instead of relabelling UTC, which had put that path 8 hours early. The test
  constant pinning it was Garmin's `startTimeLocal` -- the local wall clock
  re-encoded as if it were UTC -- which had made the defect look correct. No
  exported data changes: no parser feeds this path (AU-048).

### Changed
- Documentation a measurement had outrun: the field cap's docstring no longer
  reads AU-050's statement-count win as an argument for raising the cap --
  measured against the real export, raising it is the direction this
  implementation is worst in -- and the wall-clock readings taken against a
  600-row fixture now say so rather than standing in for production
  (CUI-0019, CUI-0020).

### Known issues
- The batch sample predicate carries one OR arm per track and is evaluated
  against every row the numbered subquery scans, so its cost grows with the
  square of the batch width: at the 64-field cap a fan-out costs 2.4x what it
  did before batching. No client sends that shape -- the dashboard reads one
  track at a time -- and the worst legal shape stays some 38x inside the 15 s
  function limit (CUI-0019).

## [3.0.0] - 2026-09-09 - `develop`

API-backed React dashboard, deployed to Vercel (API mode) and GitHub Pages
(static mode) from the same source.

### Added
- Deployment: `vercel.json`, `requirements.txt` and `scripts/vercel-build.sh`
  run the export at build time and serve the React app in API mode with
  `api/graphql.py` as a Python serverless function; the GitHub Pages
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
  exported SQLite database read-only; `api/graphql.py` is the Vercel entry.
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
- `docs/` with architecture, data-pipeline, deployment, changelog and
  roadmap pages.
- "Tag release" workflow (`workflow_dispatch`) that creates an annotated
  tag and a GitHub Release with notes taken from this changelog.

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
- Vercel deployment: invalid `runtime` key, PEP 668 pip refusal (uv venv),
  export anchored to the checkout via `RUN365_DATA_DIR`, function bundle
  size (venv under `/tmp`, `excludeFiles`), Flask missing at runtime
  (extras are not installed, so Flask and Strawberry became core
  dependencies) and the module-level `app` assignment Vercel's function
  detection requires. See `docs/deployment.md`.
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

[3.0.0]: https://github.com/dcwhung/Python-Project-Run365Days/compare/v2.0.0...v3.0.0
[2.0.0]: https://github.com/dcwhung/Python-Project-Run365Days/compare/v1.5.0...v2.0.0
[1.5.0]: https://github.com/dcwhung/Python-Project-Run365Days/compare/v1.0.0...v1.5.0
[1.0.0]: https://github.com/dcwhung/Python-Project-Run365Days/releases/tag/v1.0.0
