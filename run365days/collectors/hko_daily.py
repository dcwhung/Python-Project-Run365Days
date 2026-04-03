"""Fetch HKO daily weather extract (temperature, rainfall, wind)."""
import json
from typing import List

import requests

from run365days.models.weather import DailyWeather

_BASE_URL = "https://www.weather.gov.hk/cis/dailyExtract/dailyExtract_"


def _safe(value: str):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def fetch_year(year: str) -> List[DailyWeather]:
    """Fetch all daily records for *year* (e.g. '2021')."""
    records: List[DailyWeather] = []
    content = requests.get(f"{_BASE_URL}{year}.xml").text
    res = json.loads(content)

    for month_data in res["stn"]["data"]:
        month = str(month_data["month"]).zfill(2)
        day_data = month_data["dayData"]

        if not day_data:
            # Fallback to per-month endpoint
            try:
                content2 = requests.get(f"{_BASE_URL}{year}{month}.xml").text
                res2 = json.loads(content2)
                day_data = res2["stn"]["data"][0]["dayData"]
            except Exception:
                continue

        for data in day_data:
            data = list(map(str.strip, data))
            if not data[0].isdigit():
                continue
            date_str = f"{year}-{month}-{data[0].zfill(2)}"
            records.append(
                DailyWeather(
                    date=date_str,
                    max_temp_c=_safe(data[2]),
                    avg_temp_c=_safe(data[3]),
                    min_temp_c=_safe(data[4]),
                    mean_humidity_pct=_safe(data[6]),
                    total_rainfall_mm=_safe(data[8]),
                    mean_wind_kmh=_safe(data[11]),
                )
            )
    return records
