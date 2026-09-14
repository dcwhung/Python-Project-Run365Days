"""Fetch hourly weather history from freemeteo.hk."""

import logging

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

    desc_key = script[script.find("n(") + 2 : script.find(", 'CurrentWeather")]
    wind_text = tds[_COL_WIND].text
    return HourlyWeather(
        date=date_str,
        time=observed_at,
        temperature_c=to_float(tds[_COL_TEMPERATURE].text.strip()[:-2]),
        wind_kmh=to_float(wind_text[wind_text.find("°") + 1 :].replace("Variable at ", "")[:-5]),
        humidity_pct=to_float(tds[_COL_HUMIDITY].text.strip()[:-1]),
        description=_DESCRIPTION_MAP.get(desc_key, "Unknown"),
    )


def fetch_day(date_str: str) -> list[HourlyWeather]:
    """Fetch the hourly observations for one day.

    A row whose weather cell carries no readable ``<script>`` is logged and
    dropped on its own; the rest of the day still parses (CUI-0012).

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
