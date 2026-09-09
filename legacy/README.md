# Legacy scripts (v1)

These are the original standalone scripts from the first iteration of the
project (tag `v1.0.0`). They are kept here for reference only and are **not**
part of the `run365days` package, the test suite, or the lint configuration.

They depend on a private `com.lib` helper package that was never committed,
so they do not run as-is. Their behaviour has been re-implemented in
`run365days/src/`:

| Legacy script                        | Replacement                                   |
|--------------------------------------|-----------------------------------------------|
| `01_GetWeatherHistory.py`            | `run365days.weather.collectors.hourly`        |
| `02_GetWeatherWarningHistory.py`     | `run365days.weather.collectors.warnings`      |
| `03_GetSunMoonRiseSetHistory.py`     | not ported (data kept in `data/raw/weather/`) |
| `04_GetHKODailyWeatherExtract.py`    | `run365days.weather.collectors.hko_daily`     |
| `05_GetDailyWeightSummary.py`        | `run365days.weight`                           |
| `Run365Days.py`                      | `run365days.activities` + `run365days.cli`    |
| `met.py`                             | `run365days.activities.metrics`               |
| `test.py`                            | `run365days/tests/`                           |

This folder will be removed once the `develop` branch reaches feature parity.
