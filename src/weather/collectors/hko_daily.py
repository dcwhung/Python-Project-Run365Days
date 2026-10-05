"""Fetch HKO daily weather extract (temperature, rainfall, wind)."""

import json
import logging
from http import HTTPStatus
from typing import Any

import requests

from run365days.common.numeric import to_float
from run365days.weather.collectors._parsing import WeatherPageStructureError, fetch_text
from run365days.weather.models import DailyWeather

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.weather.gov.hk/cis/dailyExtract/dailyExtract_"

# Column layout of a daily-extract row: the day of the month, then the readings
# in the order the endpoint publishes them. The columns in between hold values
# this package does not store.
_DAY_COL = 0
_MAX_TEMP_COL = 2
_MEAN_TEMP_COL = 3
_MIN_TEMP_COL = 4
_MEAN_HUMIDITY_COL = 6
_TOTAL_RAINFALL_COL = 8
_MEAN_WIND_COL = 11


def _fetch_month_text(year: str, month: str) -> str | None:
    """Fetch one month's per-month extract, or ``None`` if HKO answers 404.

    The per-month endpoint is only asked for months the yearly file left
    empty, which in practice are the most recent ones. What HKO answers for a
    month it has not published yet has not been measured (W-066). If it is a
    404, raising here would fail the whole source every time the current year
    is collected, so a 404 is read defensively as "no extract for this month"
    and the month is skipped like any other gap. Every other error status
    (a refusal, a rate limit, a server fault) and an ``HTTPError`` that carries
    no response are re-raised unchanged: they say the endpoint is unavailable,
    not that this one month is missing (S-137).

    Args:
        year: Four-digit year as a string.
        month: Two-digit month as a string.

    Returns:
        The response body, or ``None`` when the endpoint answered 404.

    Raises:
        requests.RequestException: Any transport failure or any error status
            other than 404.
    """
    try:
        return fetch_text(f"{_BASE_URL}{year}{month}.xml")
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code != HTTPStatus.NOT_FOUND:
            raise
        logger.warning(
            "Skipping %s-%s: per-month HKO extract is absent (HTTP %s)",
            year,
            month,
            exc.response.status_code,
        )
        return None


def _yearly_months(res: Any, year: str, url: str) -> list:
    """Return the per-month entries of a decoded yearly payload.

    The yearly payload is JSON shaped ``{"stn": {"data": [<month>, ...]}}``.
    Like a payload that is not JSON at all (S-137), one that decodes but lacks
    that shape has no fallback: there is no year to read. Indexing it directly
    let a ``KeyError`` or ``TypeError`` out, which collect_weather does not
    catch, so the whole run crashed (S-149).

    Args:
        res: The decoded yearly payload.
        year: Four-digit year as a string, for the error message.
        url: The yearly endpoint, for the error message.

    Returns:
        The ``stn.data`` list, one entry per month.

    Raises:
        WeatherPageStructureError: The payload has no ``stn.data``, or it is
            not a list.
    """
    try:
        months = res["stn"]["data"]
    except (KeyError, TypeError) as exc:
        raise WeatherPageStructureError(
            f"HKO daily extract for {year} has no stn.data ({url}): {type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(months, list):
        raise WeatherPageStructureError(
            f"HKO daily extract for {year} has a stn.data that is a "
            f"{type(months).__name__}, not a list ({url})"
        )
    return months


def fetch_year(year: str) -> list[DailyWeather]:
    """Fetch the HKO daily extract for a whole year.

    Months missing from the yearly endpoint are fetched one by one from the
    per-month endpoint. A month whose per-month payload carries no usable data,
    or whose per-month request is answered 404, is logged and skipped (see
    ``_fetch_month_text``); a transport failure or any other error status is
    raised, because an endpoint that cannot be reached or refuses the request
    says nothing about that one month. A 404 on the yearly request is raised
    too: without the yearly file there is no year to read.

    Args:
        year: Four-digit year as a string, e.g. ``"2021"``.

    Returns:
        One record per day, in calendar order.

    Raises:
        requests.RequestException: The HKO endpoint could not be reached, or
            answered with an error status (``requests.HTTPError``).
        WeatherPageStructureError: The yearly payload arrived but is not JSON,
            or is JSON without a ``stn.data`` list (S-149), so the endpoint no
            longer serves the format this reads.
    """
    records: list[DailyWeather] = []
    url = f"{_BASE_URL}{year}.xml"
    content = fetch_text(url)
    # Unlike a per-month payload below, the yearly one has no fallback: without
    # it there is no year to read. JSONDecodeError is a ValueError, which
    # collect_weather does not catch, so letting it out crashed the whole run
    # and cost every source queued after this one (S-137).
    try:
        res = json.loads(content)
    except json.JSONDecodeError as exc:
        raise WeatherPageStructureError(
            f"HKO daily extract for {year} is not JSON ({url}): {exc}"
        ) from exc

    for month_data in _yearly_months(res, year, url):
        month = str(month_data["month"]).zfill(2)
        day_data = month_data["dayData"]

        if not day_data:
            # Fallback to per-month endpoint
            try:
                content2 = _fetch_month_text(year, month)
                if content2 is None:
                    continue
                res2 = json.loads(content2)
                day_data = res2["stn"]["data"][0]["dayData"]
            # Only a payload that arrived and turned out unusable (or a per-month
            # 404, handled above) is a data gap. Transport errors and every
            # other error status stay uncaught: swallowing them
            # would turn one unreachable or refusing host into twelve silently
            # empty months (AU-013, S-137).
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
            if not data[_DAY_COL].isdigit():
                continue
            date_str = f"{year}-{month}-{data[_DAY_COL].zfill(2)}"
            records.append(
                DailyWeather(
                    date=date_str,
                    max_temp_c=to_float(data[_MAX_TEMP_COL]),
                    mean_temp_c=to_float(data[_MEAN_TEMP_COL]),
                    min_temp_c=to_float(data[_MIN_TEMP_COL]),
                    mean_humidity_pct=to_float(data[_MEAN_HUMIDITY_COL]),
                    total_rainfall_mm=to_float(data[_TOTAL_RAINFALL_COL]),
                    mean_wind_kmh=to_float(data[_MEAN_WIND_COL]),
                )
            )
    return records
