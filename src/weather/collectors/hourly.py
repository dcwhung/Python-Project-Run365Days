"""Fetch hourly weather history from freemeteo.hk."""

import pandas as pd
import requests
from bs4 import BeautifulSoup

from run365days.common.numeric import to_float
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

# A socket with no timeout can hang forever, and fetch_range() pays that cost
# once per day -- 365 times for a full year. Five seconds is generous for a TCP
# handshake to a reachable host, so an unreachable one fails fast; thirty covers
# freemeteo's slowest day page without stalling the rest of the range (AU-014).
_CONNECT_TIMEOUT_SEC = 5
_READ_TIMEOUT_SEC = 30
_REQUEST_TIMEOUT = (_CONNECT_TIMEOUT_SEC, _READ_TIMEOUT_SEC)

_URL = "https://freemeteo.hk/weather/hong-kong/history/daily-history/"
_PARAMS_BASE = {
    "gid": "1819729",
    "station": "10400",
    "language": "english",
    "country": "hong-kong",
}


def fetch_day(date_str: str) -> list[HourlyWeather]:
    """Fetch the hourly observations for one day.

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

        script = str(tds[9].find("script").string)
        desc_key = script[script.find("n(") + 2 : script.find(", 'CurrentWeather")]
        records.append(
            HourlyWeather(
                date=date_str,
                time=tds[0].text.strip(),
                temperature_c=to_float(tds[1].text.strip()[:-2]),
                wind_kmh=to_float(
                    tds[3].text[tds[3].text.find("°") + 1 :].replace("Variable at ", "")[:-5]
                ),
                humidity_pct=to_float(tds[5].text.strip()[:-1]),
                description=_DESCRIPTION_MAP.get(desc_key, "Unknown"),
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
