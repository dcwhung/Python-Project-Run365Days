# Roadmap

Items are listed in the order they are planned. Nothing here has a date;
the project is developed in spare time.

## Next: API-backed dashboard (v3)

The v2 dashboard is a static page that loads the whole year as one 2.7 MB
`data.js`. The next version moves the data behind a Python service so the
front end can query only what a view needs.

Planned shape:

- **Flask** application under `src/api/`, reusing the existing
  feature packages unchanged; the builder functions become resolvers.
- **GraphQL** schema (Strawberry or Ariadne) exposing `activities`,
  `activity(id)`, `weight`, `weather(date)` and aggregate fields such as
  monthly totals and training load, so the client asks for exactly the
  fields a chart renders.
- A build step that can still emit the static `data.js`, so GitHub Pages
  keeps working as a zero-infrastructure demo.
- Container image and a `docker compose` file for local development.
- API tests with the Flask test client alongside the existing unit tests.

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
