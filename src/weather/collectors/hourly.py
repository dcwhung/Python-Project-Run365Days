"""Fetch hourly weather history from freemeteo.hk."""

import pandas as pd
import requests
from bs4 import BeautifulSoup

from run365days.common.config import HTTP_REQUEST_TIMEOUT
from run365days.common.numeric import to_float
from run365days.weather.collectors._parsing import child_string
from run365days.weather.models import HourlyWeather

_DESCRIPTION_MAP = {
    "1": "Clear weather",
    "2": "Few clouds",
    "3": "Partly cloudy skies",
    "4": "Cloudy skies",
    "7": "Rain",
    "10": "Thunderstorm",
    "26": "Snow",
    "28": "Snowstorm",
}
"""freemeteo weather-icon code -> description. Anything else reads "Unknown"."""

_UNKNOWN_DESCRIPTION = "Unknown"

_URL = "https://freemeteo.hk/weather/hong-kong/history/daily-history/"
_PARAMS_BASE = {
    "gid": "1819729",
    "station": "10400",
    "language": "english",
    "country": "hong-kong",
}

# Column layout of the daily-history table, whose header reads
# Time | Temperature | Feels Like | Wind | Gust | Humidity | Pressure |
# Dew Point | Visibility | Weather.
_TIME_COL = 0
_TEMPERATURE_COL = 1
_WIND_COL = 3
_HUMIDITY_COL = 5
_WEATHER_COL = 9

# Units freemeteo appends to the numbers, stripped by name so that a cell which
# arrives without its unit keeps its digits instead of losing its last few.
_TEMPERATURE_UNIT = "°C"
_WIND_SPEED_UNIT = " Km/h"
_HUMIDITY_UNIT = "%"

# A wind cell reads either "Northeast 50° 24 Km/h" or, when the direction keeps
# shifting, "Variable at 20 Km/h" -- the second form carries no bearing and so
# no degree sign to split on.
_BEARING_SEPARATOR = "°"
_VARIABLE_DIRECTION_PREFIX = "Variable at "

# The weather cell states its icon through a script call that reads
# ``...n(<code>, 'CurrentWeather...``; the code between the two is the only part
# this collector wants.
_ICON_CALL_PREFIX = "n("
_ICON_CALL_SUFFIX = ", 'CurrentWeather"


def _icon_code(script_text: str | None) -> str | None:
    """Return the weather-icon code carried by a weather cell's script.

    Args:
        script_text: The cell's script body, or ``None`` when the cell has no
            script at all.

    Returns:
        The icon code, or ``None`` when the cell carries no script or the
        script does not carry the expected call.
    """
    if script_text is None:
        return None
    start = script_text.find(_ICON_CALL_PREFIX)
    end = script_text.find(_ICON_CALL_SUFFIX)
    # Both are str.find() results, so both report "absent" as -1 -- an index a
    # slice accepts without complaint. Letting either through would read some
    # arbitrary run of characters and call it an icon code (CUI-0012).
    if start < 0 or end < start:
        return None
    return script_text[start + len(_ICON_CALL_PREFIX) : end]


def _wind_speed_kmh(cell_text: str) -> float | None:
    """Return the wind speed from a freemeteo wind cell.

    Args:
        cell_text: The raw text of the wind column.

    Returns:
        The speed in km/h, or ``None`` when the cell holds no readable number.
    """
    if _BEARING_SEPARATOR in cell_text:
        cell_text = cell_text.split(_BEARING_SEPARATOR, 1)[1]
    speed = cell_text.replace(_VARIABLE_DIRECTION_PREFIX, "").strip()
    return to_float(speed.removesuffix(_WIND_SPEED_UNIT))


def fetch_day(date_str: str) -> list[HourlyWeather]:
    """Fetch the hourly observations for one day.

    Args:
        date_str: The day to query, ``YYYY-MM-DD``.

    Returns:
        Hourly records in time order, or an empty list if the page has no
        history table. A row whose weather cell is unreadable still yields a
        record, with the description reading "Unknown".
    """
    html = requests.get(
        _URL, params={**_PARAMS_BASE, "date": date_str}, timeout=HTTP_REQUEST_TIMEOUT
    ).text
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", {"class": "daily-history"})
    if not tables:
        return []

    records = []
    num_cols = 0
    for tr in tables[0].find_all("tr"):
        ths = tr.find_all("th")
        if ths:
            num_cols = len(ths)
            continue
        tds = tr.find_all("td")
        if not tds or len(tds) != num_cols:
            continue

        # The temperature, wind and humidity of an hour do not stop being true
        # because its weather icon is missing, so an unreadable weather cell
        # costs the row its description and nothing else (CUI-0012).
        icon_code = _icon_code(child_string(tds[_WEATHER_COL], "script"))
        records.append(
            HourlyWeather(
                date=date_str,
                time=tds[_TIME_COL].text.strip(),
                temperature_c=to_float(
                    tds[_TEMPERATURE_COL].text.strip().removesuffix(_TEMPERATURE_UNIT)
                ),
                wind_kmh=_wind_speed_kmh(tds[_WIND_COL].text),
                humidity_pct=to_float(tds[_HUMIDITY_COL].text.strip().removesuffix(_HUMIDITY_UNIT)),
                description=_DESCRIPTION_MAP.get(icon_code or "", _UNKNOWN_DESCRIPTION),
            )
        )
    return records


def fetch_range(start_date: str, end_date: str) -> list[HourlyWeather]:
    """Fetch hourly observations for every day between two dates, inclusive.

    Args:
        start_date: First day, ``YYYY-MM-DD``.
        end_date: Last day, ``YYYY-MM-DD``.

    Returns:
        All hourly records in date and time order.
    """
    all_records: list[HourlyWeather] = []
    for d in pd.date_range(start_date, end_date):
        all_records.extend(fetch_day(d.strftime("%Y-%m-%d")))
    return all_records
