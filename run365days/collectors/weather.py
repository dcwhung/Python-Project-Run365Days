"""Fetch hourly weather history from freemeteo.hk."""
from datetime import datetime
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup

from run365days.models.weather import HourlyWeather

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

_URL = "https://freemeteo.hk/weather/hong-kong/history/daily-history/"
_PARAMS_BASE = {
    "gid": "1819729",
    "station": "10400",
    "language": "english",
    "country": "hong-kong",
}


def fetch_day(date_str: str) -> List[HourlyWeather]:
    """Fetch all hourly records for *date_str* ('YYYY-MM-DD')."""
    html = requests.get(_URL, params={**_PARAMS_BASE, "date": date_str}).text
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
                temperature_c=_safe_float(tds[1].text.strip()[:-2]),
                wind_kmh=_safe_float(
                    tds[3].text[tds[3].text.find("°") + 1:]
                    .replace("Variable at ", "")[:-5]
                ),
                humidity_pct=_safe_float(tds[5].text.strip()[:-1]),
                description=_DESCRIPTION_MAP.get(desc_key, "Unknown"),
            )
        )
    return records


def fetch_range(start_date: str, end_date: str) -> List[HourlyWeather]:
    """Fetch all hourly records between *start_date* and *end_date* inclusive."""
    all_records: List[HourlyWeather] = []
    for d in pd.date_range(start_date, end_date):
        all_records.extend(fetch_day(d.strftime("%Y-%m-%d")))
    return all_records


def _safe_float(value: str):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
