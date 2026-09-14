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
# No SUMMARIZED_ACTIVITIES_JSON. The file is there and docs/data-pipeline.md
# lists it as not yet consumed, which is the honest place to say so; a constant
# here says something stronger and false, because this module is where a reader
# looks to find out where the data it loads comes from (AU-036).

# Normalised activity output (one JSON object per line)
TCX_JSON = PROCESSED_DIR / "activities_tcx.jsonl"
GPX_JSON = PROCESSED_DIR / "activities_gpx.jsonl"
KML_JSON = PROCESSED_DIR / "activities_kml.jsonl"

# Weather sources (one JSON object per line)
#
# SUN_MOON_JSON was on the same dead-path list as the two constants deleted
# above and came off it rather than being spared: AU-037 gave SunMoon a
# collector, so run365-weather --source sun-moon now writes this file. Had the
# two tickets been settled in the other order it would have been deleted and
# immediately added back.
WEATHER_DIR = RAW_DIR / "weather"
WEATHER_HISTORY_JSON = WEATHER_DIR / "weather_history.json"
WEATHER_WARNING_JSON = WEATHER_DIR / "weather_warning_history.json"
SUN_MOON_JSON = WEATHER_DIR / "sun_moon_rise_set_history.json"
HKO_DAILY_JSON = WEATHER_DIR / "hko_daily_weather_extract.json"

# Body weight
#
# No DAILY_WEIGHT_JSON. It named data/processed/daily_weight.jsonl, which no
# code wrote and no code read; that directory has never held anything but a
# .gitkeep. The weigh-ins go straight from the text file below into the export
# (AU-036).
WEIGHT_DIR = RAW_DIR / "weight"

# Processed data set (run365-export)
PROCESSED_DB = PROCESSED_DIR / "run365.db"
STATIC_JSON_DIR = PROCESSED_DIR / "static"

# Body metrics
#
# One owner each, and both of them used. BODY_HEIGHT_CM sat here with no
# importer while weight/analysis.py kept a private copy of the same 170.0, and
# the pound conversion was worse: 0.454 inline in the parser against the exact
# 0.45359237 the frontend already used, so the API and the browser disagreed
# about the same weigh-in by ~0.09 kg at 70 kg (AU-008).
BODY_HEIGHT_CM: float = 170.0
"""Height the BMI column is computed against when a caller names none."""

LBS_TO_KG: float = 0.45359237
"""Pounds to kilograms. Not a measurement: the international pound is defined
as exactly this many kilograms, so the digits are the definition and rounding
them is simply wrong."""

# Track sampling
#
# Two questions, so two numbers -- how many samples a caller gets when it asks
# for no particular count, and how many the export writes to disk -- but they
# are not independent: the export cannot serve a default it never stored, so
# the stored count must stay at or above the served one. The served default is
# owned by dashboard/builder.py, next to the downsampler that applies it;
# tests/test_dashboard_builder.py holds the pair in order. This one lives here
# rather than in cli/export_data.py because the GraphQL schema publishes it in
# the track(points:) description, and the API must not import the CLI's
# parsing stack to read a number (tests/test_api_imports.py).
EXPORT_TRACK_POINTS = 600
"""Track samples ``run365-export`` keeps per run unless ``--points`` says otherwise."""

# Challenge defaults
DEFAULT_YEAR = 2021

# Scraper HTTP timeouts, as the ``(connect, read)`` pair ``requests`` expects.
#
# A socket with no timeout can hang forever, and the collectors pay that cost
# once per request in a loop: hourly and warnings issue one per day -- 365 for a
# full year -- and the HKO daily extract up to 13 in sequence. Five seconds is
# generous for a TCP handshake to a reachable host, so an unreachable one fails
# fast; thirty covers the slowest of the three payloads without letting one
# stalled socket dominate the whole run (AU-014).
#
# One owner, three callers. The pair lived in all three collector modules at
# once because AU-014 was scoped to that package; three copies of a tunable is
# how two get raised and the third quietly keeps the old ceiling (CUI-0013).
# It sits here rather than in the collector package because this module is
# already where a deployment-tunable constant is looked for, and because these
# are plain numbers: no import of ``requests`` follows them in, so the
# stdlib-only footprint that keeps the API importable without the parsing stack
# is unchanged (``tests/test_api_imports.py``).
HTTP_CONNECT_TIMEOUT_SEC = 5
HTTP_READ_TIMEOUT_SEC = 30
HTTP_TIMEOUT = (HTTP_CONNECT_TIMEOUT_SEC, HTTP_READ_TIMEOUT_SEC)


def daily_weight_file(year: int) -> Path:
    """Return the raw daily-weight text file for *year*."""
    return WEIGHT_DIR / f"{year}_daily_weight.txt"
