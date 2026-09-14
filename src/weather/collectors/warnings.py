"""Fetch weather warnings and tropical cyclone signals from HKO."""

import logging
from datetime import datetime
from pathlib import PurePosixPath

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from run365days.weather.collectors.html_reads import cells, child_attr
from run365days.weather.models import WeatherWarning

logger = logging.getLogger(__name__)

_SIGNALS_URL = "https://www.hko.gov.hk/en/wxinfo/climat/warndb/warndba.shtml"
_HISTORY_URL = "https://www.hko.gov.hk//cgi-bin/climat/warndb_ea.pl"

# A socket with no timeout can hang forever, and fetch_range() pays that cost
# once per day -- 365 times for a full year. Five seconds is generous for a TCP
# handshake to a reachable host, so an unreachable one fails fast; thirty covers
# the slowest warndb query without stalling the rest of the range (AU-014).
_CONNECT_TIMEOUT_SEC = 5
_READ_TIMEOUT_SEC = 30
_REQUEST_TIMEOUT = (_CONNECT_TIMEOUT_SEC, _READ_TIMEOUT_SEC)

# The warndb page repeats its layout above and below this heading; everything
# before it belongs to a previous query and must not be scraped.
_TROPICAL_CYCLONE_MARKER = "Tropical Cyclone Warning_Signals"
_MARKER_NOT_FOUND = -1

# Signal, name, start time, start date, end time, end date. Any other cell count
# is a header, a spacer or the totals line.
_WARNING_ROW_CELLS = 6
_CELL_ICON = 0
_CELL_SIGNAL = 1
_CELL_START_TIME = 2
_CELL_START_DATE = 3
_CELL_END_TIME = 4
_CELL_END_DATE = 5

_HKO_TIMESTAMP_FORMAT = "%d/%b/%Y %H:%M"

# A legend entry pairs the icon cell with the warning-type cell beside it.
# Anything shorter is a header or a spacer and names no signal.
_LEGEND_ROW_CELLS = 2
_LEGEND_CELL_ICONS = 0
_LEGEND_CELL_TYPE = 1


def _load_signal_metadata() -> dict[str, dict]:
    """Return {signal_name: {Idx, Type}} from the HKO warnings reference page.

    A legend table that arrived without both cells is logged and skipped; the
    remaining tables still load, so one shed column no longer costs the whole
    legend an ``IndexError`` (CUI-0017).

    Returns:
        One entry per legend icon, keyed by its title-cased alt text.
    """
    bs = BeautifulSoup(requests.get(_SIGNALS_URL, timeout=_REQUEST_TIMEOUT).text, "html.parser")
    result = {}
    for position, table in enumerate(bs.find_all(class_="self_row2_table")):
        tds = cells(table, _LEGEND_ROW_CELLS)
        if tds is None:
            logger.warning(
                "Skipping HKO legend table %d: it does not carry the %d cells a signal needs",
                position,
                _LEGEND_ROW_CELLS,
            )
            continue
        warning_type = tds[_LEGEND_CELL_TYPE].text.strip()
        for img in tds[_LEGEND_CELL_ICONS].find_all("img"):
            name = img.get("alt", "").lower().title()
            # The src is a URL path, so PurePosixPath answers the basename
            # without extension outright. ``rfind(".")`` answered -1 on a src
            # with no dot, and src[start:-1] is a legal slice, so the index used
            # to lose its last character in silence (CUI-0017).
            result[name] = {
                "Idx": PurePosixPath(str(img.get("src", ""))).stem,
                "Type": warning_type,
            }
    return result


def _rows_after_marker(html: str, date_str: str) -> str | None:
    """Return the part of the warndb page that holds *date_str*'s warning rows.

    Args:
        html: The whole warndb response body.
        date_str: The day being queried, used in the log message.

    Returns:
        Everything after the tropical cyclone heading, or ``None`` when the page
        does not carry that heading at all.
    """
    marker_at = html.find(_TROPICAL_CYCLONE_MARKER)
    if marker_at == _MARKER_NOT_FOUND:
        # ``str.find`` answers -1, and the old code added len(marker) to it and
        # sliced from character 31 -- an offset that still parses, so a renamed
        # HKO heading returned rows scraped from an unrelated table instead of
        # failing. An arrived-but-unreadable page is a data gap, handled the way
        # hko_daily handles an unusable month: say so, and yield nothing.
        logger.warning(
            "Skipping %s: HKO warning page carries no %r heading, so no row can be located",
            date_str,
            _TROPICAL_CYCLONE_MARKER,
        )
        return None
    return html[marker_at + len(_TROPICAL_CYCLONE_MARKER) :]


def _hko_timestamp(date_cell: Tag, time_cell: Tag) -> str:
    """Return ``"YYYY-MM-DD HH:MM:SS"`` from HKO's split date and time cells."""
    raw = f"{date_cell.text.strip()} {time_cell.text.strip()}"
    return str(datetime.strptime(raw, _HKO_TIMESTAMP_FORMAT))


def _warning_from_row(
    tds: list[Tag], date_str: str, signal_meta: dict[str, dict]
) -> WeatherWarning:
    """Build one record from a six-cell warndb row.

    Args:
        tds: The row's six cells.
        date_str: The day being queried.
        signal_meta: Signal legend from :func:`_load_signal_metadata`.

    Returns:
        The parsed warning.
    """
    name = tds[_CELL_SIGNAL].text.strip()
    icon = child_attr(tds[_CELL_ICON], "img", "src")
    if icon is None:
        # The signal and its times are all present, so the row is still a real
        # warning; only the decorative icon is gone. Dropping a hoisted typhoon
        # signal over missing markup would be the worse trade.
        logger.warning("%s %s: no signal icon in the first cell", date_str, name.upper())
        icon = ""

    return WeatherWarning(
        date=date_str,
        warning_type=signal_meta.get(name.lower().title(), {}).get("Type", "Unknown"),
        warning_signal=name.upper(),
        start_time=_hko_timestamp(tds[_CELL_START_DATE], tds[_CELL_START_TIME]),
        end_time=_hko_timestamp(tds[_CELL_END_DATE], tds[_CELL_END_TIME]),
        icon_url=icon,
    )


def fetch_day(date_str: str, signal_meta: dict[str, dict]) -> list[WeatherWarning]:
    """Fetch all warning records issued on one day.

    A page that arrives without the tropical cyclone heading is logged and
    yields no records, rather than being parsed from an arbitrary offset
    (CUI-0012).

    Args:
        date_str: The day to query, ``YYYY-MM-DD``.
        signal_meta: Signal legend from :func:`_load_signal_metadata`.

    Returns:
        One record per warning or tropical cyclone signal.
    """
    html = requests.get(
        _HISTORY_URL,
        params={"start_ym": datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")},
        timeout=_REQUEST_TIMEOUT,
    ).text

    rows_html = _rows_after_marker(html, date_str)
    if rows_html is None:
        return []

    bs = BeautifulSoup(rows_html, "html.parser")
    records = []
    for table in bs.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) != _WARNING_ROW_CELLS:
                continue
            records.append(_warning_from_row(tds, date_str, signal_meta))
    return records


def fetch_range(start_date: str, end_date: str) -> list[WeatherWarning]:
    """Fetch warnings for every day between two dates, inclusive.

    The HKO signal legend is downloaded once and reused for every day.

    Args:
        start_date: First day, ``YYYY-MM-DD``.
        end_date: Last day, ``YYYY-MM-DD``.

    Returns:
        All warnings in date order; days without warnings contribute nothing.
    """
    signal_meta = _load_signal_metadata()
    all_records: list[WeatherWarning] = []
    for d in pd.date_range(start_date, end_date):
        all_records.extend(fetch_day(d.strftime("%Y-%m-%d"), signal_meta))
    return all_records
