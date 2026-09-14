"""Fetch hourly weather history from freemeteo.hk."""

import logging
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from run365days.common.numeric import to_float
from run365days.weather.collectors.html_reads import child_string
from run365days.weather.models import HourlyWeather

logger = logging.getLogger(__name__)

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
_UNKNOWN_DESCRIPTION = "Unknown"

# freemeteo names the icon inside writeWeatherIcon(<code>, 'CurrentWeather', ...).
# Matching the whole call is the only way to know the code was really read: the
# old find()/slice pair answered -1 for "call absent", -1 is a legal slice index,
# and the slice that followed handed back a code nobody wrote (CUI-0017).
_DESCRIPTION_CODE_RE = re.compile(r"n\(\s*(?P<code>[^,()]+?)\s*,\s*'CurrentWeather'")

# A socket with no timeout can hang forever, and fetch_range() pays that cost
# once per day -- 365 times for a full year. Five seconds is generous for a TCP
# handshake to a reachable host, so an unreachable one fails fast; thirty covers
# freemeteo's slowest day page without stalling the rest of the range (AU-014).
_CONNECT_TIMEOUT_SEC = 5
_READ_TIMEOUT_SEC = 30
_REQUEST_TIMEOUT = (_CONNECT_TIMEOUT_SEC, _READ_TIMEOUT_SEC)

# Column order of the freemeteo daily-history table. Naming them keeps the
# guarded read below readable and makes a future column shuffle a one-line edit.
_COL_TIME = 0
_COL_TEMPERATURE = 1
_COL_WIND = 3
_COL_HUMIDITY = 5
_COL_WEATHER = 9

_URL = "https://freemeteo.hk/weather/hong-kong/history/daily-history/"
_PARAMS_BASE = {
    "gid": "1819729",
    "station": "10400",
    "language": "english",
    "country": "hong-kong",
}


def _description(script: str, date_str: str, observed_at: str) -> str:
    """Return the weather description the row's icon script names.

    Args:
        script: The weather cell's ``<script>`` body.
        date_str: The day being queried.
        observed_at: The row's observation time.

    Returns:
        The mapped description, or ``"Unknown"`` -- reported both when the
        script carries no icon call and when it names a code the map does not
        know, because in neither case did the page state the weather.
    """
    match = _DESCRIPTION_CODE_RE.search(script)
    if match is None:
        logger.warning(
            "%s %s: weather script carries no icon call, so no description was read",
            date_str,
            observed_at,
        )
        return _UNKNOWN_DESCRIPTION
    return _DESCRIPTION_MAP.get(match.group("code"), _UNKNOWN_DESCRIPTION)


def _observation_from_row(tds: list[Tag], date_str: str) -> HourlyWeather | None:
    """Build one record from a freemeteo daily-history row.

    Args:
        tds: The row's cells, already known to match the header width.
        date_str: The day being queried.

    Returns:
        The parsed observation, or ``None`` when the weather cell carries no
        readable ``<script>`` to take the description code from.
    """
    observed_at = tds[_COL_TIME].text.strip()
    script = child_string(tds[_COL_WEATHER], "script")
    if script is None:
        # Dropping the one unreadable row beats the old chained read, which
        # raised out of the loop and lost the whole day over a single cell.
        logger.warning(
            "Skipping %s %s: weather cell holds no <script> to read the code from",
            date_str,
            observed_at,
        )
        return None

    wind_text = tds[_COL_WIND].text
    return HourlyWeather(
        date=date_str,
        time=observed_at,
        temperature_c=to_float(tds[_COL_TEMPERATURE].text.strip()[:-2]),
        wind_kmh=to_float(wind_text[wind_text.find("°") + 1 :].replace("Variable at ", "")[:-5]),
        humidity_pct=to_float(tds[_COL_HUMIDITY].text.strip()[:-1]),
        description=_description(script, date_str, observed_at),
    )


def fetch_day(date_str: str) -> list[HourlyWeather]:
    """Fetch the hourly observations for one day.

    A row whose weather cell carries no readable ``<script>`` is logged and
    dropped on its own; the rest of the day still parses (CUI-0012). A script
    that carries no icon call keeps its row but reports ``"Unknown"``, rather
    than a code sliced out of an offset nobody read (CUI-0017).

    Args:
        date_str: The day to query, ``YYYY-MM-DD``.

    Returns:
        Hourly records in time order, or an empty list if the page has no
        history table.
    """
    html = requests.get(
        _URL, params={**_PARAMS_BASE, "date": date_str}, timeout=_REQUEST_TIMEOUT
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

        record = _observation_from_row(tds, date_str)
        if record is not None:
            records.append(record)
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
