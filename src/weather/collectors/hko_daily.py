"""Fetch HKO daily weather extract (temperature, rainfall, wind)."""

import json
import logging

import requests

from run365days.common import config
from run365days.common.numeric import to_float
from run365days.weather.models import DailyWeather

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.weather.gov.hk/cis/dailyExtract/dailyExtract_"


def fetch_year(year: str) -> list[DailyWeather]:
    """Fetch the HKO daily extract for a whole year.

    Months missing from the yearly endpoint are fetched one by one from the
    per-month endpoint. A month whose per-month payload carries no usable data
    is logged and skipped; a transport failure is raised, because an endpoint
    that cannot be reached at all says nothing about that one month.

    Args:
        year: Four-digit year as a string, e.g. ``"2021"``.

    Returns:
        One record per day, in calendar order.

    Raises:
        requests.RequestException: The HKO endpoint could not be reached.
    """
    records: list[DailyWeather] = []
    content = requests.get(f"{_BASE_URL}{year}.xml", timeout=config.HTTP_TIMEOUT).text
    res = json.loads(content)

    for month_data in res["stn"]["data"]:
        month = str(month_data["month"]).zfill(2)
        day_data = month_data["dayData"]

        if not day_data:
            # Fallback to per-month endpoint
            try:
                content2 = requests.get(
                    f"{_BASE_URL}{year}{month}.xml", timeout=config.HTTP_TIMEOUT
                ).text
                res2 = json.loads(content2)
                day_data = res2["stn"]["data"][0]["dayData"]
            # Only a payload that arrived and turned out unusable is a data gap.
            # Transport errors stay uncaught: swallowing them would turn one
            # unreachable host into twelve silently empty months (AU-013).
            except (json.JSONDecodeError, KeyError, IndexError) as exc:
                logger.warning(
                    "Skipping %s-%s: per-month HKO extract carries no usable data (%s: %s)",
                    year,
                    month,
                    type(exc).__name__,
                    exc,
                )
                continue

        for data in day_data:
            data = list(map(str.strip, data))
            if not data[0].isdigit():
                continue
            date_str = f"{year}-{month}-{data[0].zfill(2)}"
            records.append(
                DailyWeather(
                    date=date_str,
                    max_temp_c=to_float(data[2]),
                    mean_temp_c=to_float(data[3]),
                    min_temp_c=to_float(data[4]),
                    mean_humidity_pct=to_float(data[6]),
                    total_rainfall_mm=to_float(data[8]),
                    mean_wind_kmh=to_float(data[11]),
                )
            )
    return records
