# Roadmap

Items are listed in the order they are planned. Nothing here has a date;
the project is developed in spare time.

## In progress: API-backed dashboard (v3)

Landed on `develop`: `run365-export` (SQLite + static JSON), the Flask +
Strawberry GraphQL API, the React dashboard with all nine views in both
data modes, and the Vercel / GitHub Pages deployment configuration.

Remaining before tagging `v3.0.0`:

- First Vercel deployment and any fixes it needs.
- Screenshots in the README.
- Container image and a `docker compose` file for local development.
- API tests through the deployed endpoint (smoke test in CI).

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
- Screenshots of the dashboard in the README once the v3 layout settles.
