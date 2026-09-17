"""Fetch weather warnings and tropical cyclone signals from HKO."""

from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from run365days.weather.collectors._parsing import child_attr, section_after
from run365days.weather.models import WeatherWarning

_SIGNALS_URL = "https://www.hko.gov.hk/en/wxinfo/climat/warndb/warndba.shtml"
_HISTORY_URL = "https://www.hko.gov.hk//cgi-bin/climat/warndb_ea.pl"

# A socket with no timeout can hang forever, and fetch_range() pays that cost
# once per day -- 365 times for a full year. Five seconds is generous for a TCP
# handshake to a reachable host, so an unreachable one fails fast; thirty covers
# the slowest warndb query without stalling the rest of the range (AU-014).
_CONNECT_TIMEOUT_SEC = 5
_READ_TIMEOUT_SEC = 30
_REQUEST_TIMEOUT = (_CONNECT_TIMEOUT_SEC, _READ_TIMEOUT_SEC)

# The heading the day's warning table sits under. It is page furniture, not a
# consequence of the day's weather: a day with no warning at all still renders
# it above an empty table, so a page that lacks it is a page that changed shape.
_WARNING_TABLE_MARKER = "Tropical Cyclone Warning_Signals"

# Column layout of the warndb result rows, whose header reads
# Signal | Name | From | Date | To | Date.
_ICON_COL = 0
_SIGNAL_COL = 1
_START_TIME_COL = 2
_START_DATE_COL = 3
_END_TIME_COL = 4
_END_DATE_COL = 5

# A warning row fills every column; the table's total line is shorter, and the
# header line carries th cells rather than td cells.
_WARNING_ROW_CELLS = 6

_TIMESTAMP_FORMAT = "%d/%b/%Y %H:%M"


def _load_signal_metadata() -> dict[str, dict]:
    """Return {signal_name: {Idx, Type}} from the HKO warnings reference page."""
    bs = BeautifulSoup(requests.get(_SIGNALS_URL, timeout=_REQUEST_TIMEOUT).text, "html.parser")
    result = {}
    for table in bs.find_all(class_="self_row2_table"):
        tds = table.find_all("td")
        for img in tds[0].find_all("img"):
            src = img.get("src", "")
            name = img.get("alt", "").lower().title()
            result[name] = {
                "Idx": src[src.rfind("/") + 1 : src.rfind(".")],
                "Type": tds[1].text.strip(),
            }
    return result


def fetch_day(date_str: str, signal_meta: dict[str, dict]) -> list[WeatherWarning]:
    """Fetch all warning records issued on one day.

    Args:
        date_str: The day to query, ``YYYY-MM-DD``.
        signal_meta: Signal legend from :func:`_load_signal_metadata`.

    Returns:
        One record per warning or tropical cyclone signal. A day on which no
        warning was in force yields an empty list.

    Raises:
        WeatherPageStructureError: The page does not carry the warning-table
            heading, so there is no way to tell the day's warnings from the
            other tables on the page.
    """
    html = requests.get(
        _HISTORY_URL,
        params={"start_ym": datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")},
        timeout=_REQUEST_TIMEOUT,
    ).text

    bs = BeautifulSoup(section_after(html, _WARNING_TABLE_MARKER), "html.parser")

    records = []
    for table in bs.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) != _WARNING_ROW_CELLS:
                continue
            signal = tds[_SIGNAL_COL].text.strip().upper()
            start = str(
                datetime.strptime(
                    tds[_START_DATE_COL].text.strip() + " " + tds[_START_TIME_COL].text.strip(),
                    _TIMESTAMP_FORMAT,
                )
            )
            end = str(
                datetime.strptime(
                    tds[_END_DATE_COL].text.strip() + " " + tds[_END_TIME_COL].text.strip(),
                    _TIMESTAMP_FORMAT,
                )
            )
            name_key = tds[_SIGNAL_COL].text.strip().lower().title()
            warning_type = signal_meta.get(name_key, {}).get("Type", "Unknown")
            # An icon is decoration: the signal name and both timestamps, which
            # are what the record is for, are already in hand. Dropping the row
            # over a missing img would lose a real warning to lose a picture.
            icon = child_attr(tds[_ICON_COL], "img", "src")

            records.append(
                WeatherWarning(
                    date=date_str,
                    warning_type=warning_type,
                    warning_signal=signal,
                    start_time=start,
                    end_time=end,
                    icon_url=icon,
                )
            )
    return records


def fetch_range(start_date: str, end_date: str) -> list[WeatherWarning]:
    """Fetch warnings for every day between two dates, inclusive.

    The HKO signal legend is downloaded once and reused for every day.

    Args:
        start_date: First day, ``YYYY-MM-DD``.
        end_date: Last day, ``YYYY-MM-DD``.

    Returns:
        All warnings in date order; days without warnings contribute nothing.

    Raises:
        WeatherPageStructureError: A day's page changed shape; see
            :func:`fetch_day`.
    """
    signal_meta = _load_signal_metadata()
    all_records: list[WeatherWarning] = []
    for d in pd.date_range(start_date, end_date):
        all_records.extend(fetch_day(d.strftime("%Y-%m-%d"), signal_meta))
    return all_records
