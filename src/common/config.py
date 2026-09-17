"""Centralised paths and constants for the run365days package.

All data lives under ``<repo>/data``. Set the ``RUN365_DATA_DIR`` environment
variable to point the package at a different data directory (for example in
CI or when the package is installed outside the repository).
"""

import os
from pathlib import Path

# <repo>/src/common/config.py -> parents[2] == <repo>
ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.environ.get("RUN365_DATA_DIR", ROOT_DIR / "data"))

RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Garmin activity exports
GARMIN_DIR = RAW_DIR / "garmin"
TCX_DIR = GARMIN_DIR / "tcx"
GPX_DIR = GARMIN_DIR / "gpx"
KML_DIR = GARMIN_DIR / "kml"
SUMMARIZED_ACTIVITIES_JSON = GARMIN_DIR / "summarized_activities.json"

# Normalised activity output (one JSON object per line)
TCX_JSON = PROCESSED_DIR / "activities_tcx.jsonl"
GPX_JSON = PROCESSED_DIR / "activities_gpx.jsonl"
KML_JSON = PROCESSED_DIR / "activities_kml.jsonl"

# Weather sources (one JSON object per line)
WEATHER_DIR = RAW_DIR / "weather"
WEATHER_HISTORY_JSON = WEATHER_DIR / "weather_history.json"
WEATHER_WARNING_JSON = WEATHER_DIR / "weather_warning_history.json"
SUN_MOON_JSON = WEATHER_DIR / "sun_moon_rise_set_history.json"
HKO_DAILY_JSON = WEATHER_DIR / "hko_daily_weather_extract.json"

# Body weight
WEIGHT_DIR = RAW_DIR / "weight"
DAILY_WEIGHT_JSON = PROCESSED_DIR / "daily_weight.jsonl"

# Processed data set (run365-export)
PROCESSED_DB = PROCESSED_DIR / "run365.db"
STATIC_JSON_DIR = PROCESSED_DIR / "static"

# Body metrics
BODY_HEIGHT_CM: float = 170.0

# Challenge defaults
DEFAULT_YEAR = 2021

# Weather collector HTTP timeouts
#
# A socket with no timeout can hang forever, and the collectors pay that cost
# once per day across a whole year of history. The connect budget is the short
# one, so an unreachable host fails fast instead of stalling the run; the read
# budget is the longer one, because it has to cover the slowest single payload
# without letting one bad day dominate the range (AU-014). Held here rather
# than once per collector so retiming them cannot leave one behind (CUI-0013).
HTTP_CONNECT_TIMEOUT_SEC = 5
HTTP_READ_TIMEOUT_SEC = 30
HTTP_REQUEST_TIMEOUT = (HTTP_CONNECT_TIMEOUT_SEC, HTTP_READ_TIMEOUT_SEC)


def daily_weight_file(year: int) -> Path:
    """Return the raw daily-weight text file for *year*."""
    return WEIGHT_DIR / f"{year}_daily_weight.txt"
