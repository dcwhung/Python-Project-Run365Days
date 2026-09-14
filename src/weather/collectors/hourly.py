"""Fetch hourly weather history from freemeteo.hk."""

import logging
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from run365days.common import config
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

# The three numeric cells, each matched together with the unit it must be in.
#
# All three used to be read by counting characters off the end -- [:-5] for the
# wind, [:-2] for the temperature, [:-1] for the humidity -- which reads any
# cell of the right length as a value in the expected unit. That is how
# "Variable at 20 mph" was reported as 2.0 Km/h (CUI-0018); measured on the same
# fixtures, "52 °F" was reported as 52.0 °C and a bare "24" as 2.0 % by exactly
# the same trick. A character count cannot tell a changed unit from a changed
# reading, so freemeteo switching to units=us would have filled a year of
# history with numbers 20 degrees out and said nothing (CUI-0013).
#
# Naming the unit in the pattern makes that a non-match, which the caller
# reports rather than converts: this collector's job is to read what the page
# states, and a page stating Fahrenheit is not a Celsius reading to be salvaged.
#
# The wind cell alone has two accepted forms: "Northeast 50° 24 Km/h" names a
# bearing, "Variable at 20 Km/h" has no degree sign at all. Spelling both out as
# alternatives means a third form fails to match and says so, instead of parsing
# by accident the way find("°") + 1 did on the -1 sentinel (CUI-0018).
_TEMPERATURE_C_RE = re.compile(r"(?P<value>-?\d+(?:\.\d+)?)\s*°\s*C(?![a-zA-Z])")
_WIND_SPEED_RE = re.compile(r"(?:\w+°|Variable at)\s*(?P<value>\d+(?:\.\d+)?)\s*Km/h")
_HUMIDITY_PCT_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*%")

_TEMPERATURE_UNIT = "temperature in °C"
_WIND_UNIT = "wind speed in Km/h, after a bearing or 'Variable at'"
_HUMIDITY_UNIT = "humidity in %"

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


def _reading(
    cell_text: str,
    pattern: re.Pattern[str],
    expected: str,
    date_str: str,
    observed_at: str,
) -> float | None:
    """Return the number a numeric cell states, in the unit *pattern* requires.

    Args:
        cell_text: The cell's text, unit and all.
        pattern: Pattern naming the unit, capturing the number as ``value``.
        expected: What the cell should have stated, for the log message.
        date_str: The day being queried.
        observed_at: The row's observation time.

    Returns:
        The reading, or ``None`` when the cell states nothing in that unit --
        the rest of the observation is still good, so the gap stays in this one
        column rather than costing the hour its other three readings.
    """
    match = pattern.search(cell_text)
    if match is None:
        # The cell text goes in the message: naming only the column cannot say
        # whether the page changed units or dropped the reading altogether, and
        # those need different fixes.
        logger.warning(
            "%s %s: cell %r states no %s, so that column is a gap",
            date_str,
            observed_at,
            cell_text.strip(),
            expected,
        )
        return None
    # The pattern already fixed the digits, so the cast cannot fail and None
    # keeps one meaning here: the cell stated nothing in the expected unit.
    return float(match.group("value"))


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

    return HourlyWeather(
        date=date_str,
        time=observed_at,
        temperature_c=_reading(
            tds[_COL_TEMPERATURE].text, _TEMPERATURE_C_RE, _TEMPERATURE_UNIT, date_str, observed_at
        ),
        wind_kmh=_reading(tds[_COL_WIND].text, _WIND_SPEED_RE, _WIND_UNIT, date_str, observed_at),
        humidity_pct=_reading(
            tds[_COL_HUMIDITY].text, _HUMIDITY_PCT_RE, _HUMIDITY_UNIT, date_str, observed_at
        ),
        description=_description(script, date_str, observed_at),
    )


def fetch_day(date_str: str) -> list[HourlyWeather]:
    """Fetch the hourly observations for one day.

    A row whose weather cell carries no readable ``<script>`` is logged and
    dropped on its own; the rest of the day still parses (CUI-0012). A script
    that carries no icon call keeps its row but reports ``"Unknown"``, rather
    than a code sliced out of an offset nobody read (CUI-0017). A wind cell in
    neither of freemeteo's two forms keeps its row too, with ``wind_kmh`` as
    ``None`` instead of a number sliced out of an unexpected unit (CUI-0018);
    the temperature and humidity cells are read the same way, so a page served
    in Fahrenheit reports no temperature rather than a Celsius figure 20 degrees
    out (CUI-0013).

    Args:
        date_str: The day to query, ``YYYY-MM-DD``.

    Returns:
        Hourly records in time order, or an empty list if the page has no
        history table.
    """
    html = requests.get(
        _URL, params={**_PARAMS_BASE, "date": date_str}, timeout=config.HTTP_TIMEOUT
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
