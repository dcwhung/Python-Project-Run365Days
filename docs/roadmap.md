# Roadmap

Items are listed in the order they are planned. Nothing here has a date;
the project is developed in spare time.

## Done: API-backed dashboard (v3)

`run365-export` (SQLite + static JSON), the Flask + Strawberry GraphQL
API, the React dashboard with all nine views in both data modes, and the
Vercel / GitHub Pages deployments are live. See `docs/deployment.md`.

## Next

- Container image and a `docker compose` file for local development.
- A smoke test in CI against the deployed API (`/api/health` and one
  query) after each Vercel deployment.
- Code-split the React bundle (Chart.js and the Activity view are the
  bulk of the 650 kB main chunk).
- Playwright end-to-end test for the Overview to Activity flow.

## Wellness data

`data/raw/garmin/` already holds daily summaries (steps, active calories,
stress, intensity minutes) and nightly sleep for the challenge period, and
660 activity summaries with elevation gain and location names. Planned
views once the API exists:

- Sleep duration and deep-sleep share against next-day pace.
- Daily stress and resting calories over the year.
- Elevation gain per run from Garmin's own figure, compared with the
  smoothed ascent the builder computes from the track.

## Smaller items

- Read `sun_moon_rise_set_history.json` to tag runs as pre-dawn, daylight
  or after dark.
- Port the v1 sun / moon scraper into `run365days.weather.collectors` and
  then delete `legacy/`.
- Type checking with mypy in CI.
