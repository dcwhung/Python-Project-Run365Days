"""Fetch HKO daily weather extract (temperature, rainfall, wind)."""

import json
import logging

import requests

from run365days.common import config
from run365days.common.numeric import to_float
from run365days.weather.models import DailyWeather

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.weather.gov.hk/cis/dailyExtract/dailyExtract_"

# Column order of one ``dayData`` row. The endpoint serves each day as a bare
# array with no keys, so reading by position is the only read available -- but
# ``data[11]`` on its own sends the reader back to the HKO page to count columns
# before they can tell mean wind speed from wind direction (CUI-0013).
#
# In full, left to right: day of month, mean pressure, three air temperatures
# (max, mean, min), mean dew point, mean relative humidity, mean cloud amount,
# total rainfall, total bright sunshine, prevailing wind direction, mean wind
# speed. The six this collector keeps are named; the six it skips are not, and
# their absence from this block is what says they are skipped on purpose.
_COL_DAY_OF_MONTH = 0
_COL_MAX_TEMP_C = 2
_COL_MEAN_TEMP_C = 3
_COL_MIN_TEMP_C = 4
_COL_MEAN_HUMIDITY_PCT = 6
_COL_TOTAL_RAINFALL_MM = 8
_COL_MEAN_WIND_KMH = 11

# Twelve columns, so the widest index read is the last one. Reading a shorter
# row by position either raises or -- worse -- lands on a neighbouring column,
# so a short row is a data gap to report, not a row to salvage.
_ROW_COLUMNS = 12


def _day_record(row: list[str], year: str, month: str) -> DailyWeather | None:
    """Build one day's record from a ``dayData`` row.

    Args:
        row: One row of the extract, cells in :data:`_ROW_COLUMNS` order.
        year: Four-digit year.
        month: Two-digit month.

    Returns:
        The parsed day, or ``None`` for a row that names no day (the header and
        the monthly summary lines) or that arrived too short to read by
        position -- both are reported by the caller keeping the rest of the year.
    """
    if not row or not row[_COL_DAY_OF_MONTH].isdigit():
        return None

    date_str = f"{year}-{month}-{row[_COL_DAY_OF_MONTH].zfill(2)}"
    if len(row) < _ROW_COLUMNS:
        logger.warning(
            "Skipping %s: the HKO extract row carries %d of %d columns, "
            "so its readings cannot be told apart by position",
            date_str,
            len(row),
            _ROW_COLUMNS,
        )
        return None

    return DailyWeather(
        date=date_str,
        max_temp_c=to_float(row[_COL_MAX_TEMP_C]),
        mean_temp_c=to_float(row[_COL_MEAN_TEMP_C]),
        min_temp_c=to_float(row[_COL_MIN_TEMP_C]),
        mean_humidity_pct=to_float(row[_COL_MEAN_HUMIDITY_PCT]),
        total_rainfall_mm=to_float(row[_COL_TOTAL_RAINFALL_MM]),
        mean_wind_kmh=to_float(row[_COL_MEAN_WIND_KMH]),
    )


def _month_day_data(year: str, month: str) -> list | None:
    """Fetch one month's rows from the per-month endpoint.

    Args:
        year: Four-digit year.
        month: Two-digit month.

    Returns:
        The month's ``dayData`` rows, or ``None`` when the payload arrived and
        turned out unusable -- that is a data gap in one month, so the caller
        skips it and keeps the other eleven.

    Raises:
        requests.RequestException: The endpoint could not be reached. Left
            uncaught on purpose: swallowing it would turn one unreachable host
            into twelve silently empty months (AU-013).
    """
    content = requests.get(f"{_BASE_URL}{year}{month}.xml", timeout=config.HTTP_TIMEOUT).text
    try:
        return json.loads(content)["stn"]["data"][0]["dayData"]
    except (json.JSONDecodeError, KeyError, IndexError) as exc:
        logger.warning(
            "Skipping %s-%s: per-month HKO extract carries no usable data (%s: %s)",
            year,
            month,
            type(exc).__name__,
            exc,
        )
        return None


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
        day_data = month_data["dayData"] or _month_day_data(year, month)
        if not day_data:
            continue

        for row in day_data:
            record = _day_record(list(map(str.strip, row)), year, month)
            if record is not None:
                records.append(record)
    return records
