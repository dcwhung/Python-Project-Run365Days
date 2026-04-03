"""Centralised configuration for Run365Days."""
from pathlib import Path

# Repository root (directory containing this file's parent package)
ROOT_DIR = Path(__file__).parent.parent

# Garmin activity data
GARMIN_DIR = ROOT_DIR / "Garmin"
GPX_DIR = GARMIN_DIR / "gpx"
KML_DIR = GARMIN_DIR / "kml"
TCX_DIR = GARMIN_DIR / "tcx"

GPX_JSON = GARMIN_DIR / "run365_gpx_data.json"
KML_JSON = GARMIN_DIR / "run365_kml_data.json"
TCX_JSON = GARMIN_DIR / "run365_tcx_data.json"

# Weather data
WEATHER_DIR = ROOT_DIR / "Weather"
WEATHER_HISTORY_JSON = WEATHER_DIR / "WeatherHistory.json"
WEATHER_WARNING_JSON = WEATHER_DIR / "WeatherWarningHistory.json"
SUN_MOON_JSON = WEATHER_DIR / "SunMoonRiseSetHistory.json"
HKO_DAILY_JSON = WEATHER_DIR / "HKODailyWeatherExtract.json"

# Weight data
DAILY_WEIGHT_JSON = ROOT_DIR / "DailyWeightExtract.json"

# Body metrics
BODY_HEIGHT_CM: float = 170.0
