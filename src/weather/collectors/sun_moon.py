"""Fetch sunrise, sunset and moon times from timeanddate.com.

The port of ``legacy/03_GetSunMoonRiseSetHistory.py`` (AU-037). Until it
existed, :class:`~run365days.weather.models.SunMoon` was the one weather
dataclass with no writer, and ``config.SUN_MOON_JSON`` named a file nothing in
the package could produce, so re-collecting the weather could only ever shrink
what ``data/raw/weather/`` holds.

What the page layout below rests on
-----------------------------------
Two kinds of evidence, and it matters which is which.

The **output contract** is settled. Every column name, value format, the ``"/"``
marker and the trailing ``%`` are fixed by the committed
``sun_moon_rise_set_history.json``, which that legacy script produced.
:class:`SunMoon`'s raw-row pair owns them, and the tests compare the collector's
output against real rows of that file rather than an expectation written beside
the parser.

The **page layout** -- which table id, which cell index -- is read off the legacy
script and cannot be re-checked against timeanddate from here. The committed
file corroborates one branch of it: every day whose ``Moon Transit`` is ``"/"``
carries exactly ``100.0%``, which is what the merged-meridian branch below
answers (``TestTheCommittedSunMoonFile`` pins this). The rest is unverified, so
every positional read is guarded: a page that has moved reports a gap or drops
a row and says so, rather than returning a value from a neighbouring column the
way the legacy character slices could (CUI-0013, CUI-0018).
"""

import calendar
import logging
import re

from bs4 import BeautifulSoup, Tag

from run365days.weather.collectors._parsing import (
    WeatherPageStructureError,
    cells,
    colspan,
    fetch_text,
)
from run365days.weather.models import SunMoon

logger = logging.getLogger(__name__)

_SUN_URL = "https://timeanddate.com/sun/hong-kong/hong-kong"
_MOON_URL = "https://timeanddate.com/moon/hong-kong/hong-kong"

_SUN_TABLE_ID = "as-monthsun"
_MOON_TABLE_ID = "tb-7dmn"

# Cell order of one row of the monthly sun table. Left to right timeanddate
# serves: sunrise, sunset, daylength, difference, astronomical twilight start
# and end, nautical start and end, civil start and end, solar noon, and the
# sun's distance. The four this collector keeps are named; leaving the others
# unnamed is what says they are skipped on purpose.
_SUN_COL_SUNRISE = 0
_SUN_COL_SUNSET = 1
_SUN_COL_DAY_LENGTH = 2
_SUN_COL_SOLAR_NOON = 10

# Solar noon is the widest index read, so a row must reach it. A shorter row
# would either raise or land on a neighbouring column, so it is a gap to report
# rather than a row to salvage.
_SUN_ROW_CELLS = _SUN_COL_SOLAR_NOON + 1

# The moon table opens with three rise/set slots: a moonrise, a moonset and a
# second moonrise, because a calendar day is long enough to hold two. Each slot
# is either a time cell followed by a compass bearing, or one cell spanning both
# when the event does not happen that day.
_MOON_EVENT_SLOTS = 3
_MOON_SLOT_INDEX_SET = 1
_MOON_FILLED_SLOT_CELLS = 2
_MOON_EMPTY_SLOT_COLSPAN = 2

# After the slots come four cells: meridian passing time, its altitude, the
# moon's distance and the illuminated fraction. They are read from the end,
# because the slots in front of them are not a fixed width.
_MOON_COL_TRANSIT = -4
_MOON_COL_ILLUMINATION = -1
_MOON_TRAILING_CELLS = 4

# On a full moon the meridian passing falls outside the calendar day, and
# timeanddate merges that whole trailing block into one cell. The legacy script
# answered a flat 100.0% there, and the committed file shows it was right to.
_MOON_MERGED_TRAILING_COLSPAN = _MOON_TRAILING_CELLS
_FULL_MOON_ILLUMINATION_PCT = 100.0

# A 24-hour clock time. The guards on its two sides keep it from being cut out
# of something longer. "(?<![\d:])" on the left stops it starting mid-number or
# after a colon, so the "47:58" tail of a "10:47:58" daylength is not a time.
# "(?![\d:])" on the right stops it ending mid-number or before a colon: the
# colon half keeps it from taking the "10:47" off that daylength's front, the
# digit half from taking "07:05" off "07:051" (S-138). "(?!\s*[ap]\.?m)"
# refuses a 12-hour clock outright, dotted or not, because "7:05 pm" or
# "7:05 p.m." read this way is 07:05 -- a real time, twelve hours out, with
# nothing to show it was ever wrong (CUI-0018, W-064). The shape alone accepts
# "25:99"; _time() range-checks what it matched.
_TIME_RE = re.compile(
    r"(?<![\d:])(?P<hour>\d{1,2}):(?P<minute>\d{2})(?![\d:])(?!\s*[ap]\.?m)", re.IGNORECASE
)
_HOURS_PER_DAY = 24
"""One past the highest hour a 24-hour clock shows."""
_MINUTES_PER_HOUR = 60
"""One past the highest minute a clock shows."""
_DAY_LENGTH_RE = re.compile(r"(?<!\d)(?P<value>\d{1,2}:\d{2}:\d{2})(?!\d)")
_ILLUMINATION_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*%")

_FIRST_MONTH = 1
"""January, the first month :func:`fetch_year` requests."""
_LAST_MONTH = 12
"""December, the last month :func:`fetch_year` requests."""

_MISSING_TIME = ""
"""What a sun column answers when its cell states no time. The four sun fields
are a plain ``str`` on :class:`SunMoon`, so this is the same empty-text default
``weather.models`` gives every missing string column (CUI-0014)."""


def _table(url: str, year: str, month: str, table_id: str) -> Tag | None:
    """Fetch one month's page and return the table it should carry.

    Args:
        url: Page to fetch.
        year: Four-digit year.
        month: Two-digit month.
        table_id: ``id`` of the table wanted.

    Returns:
        The table, or ``None`` when the page carries no such element -- one
        month with no data, which the caller skips while keeping the others.

    Raises:
        requests.RequestException: The page could not be reached, or was
            answered with an error status (``requests.HTTPError``). Left
            uncaught on purpose: swallowing it would turn one unreachable host
            into a year of silently empty months (AU-013).
    """
    # fetch_text refuses an error status: an error page carries no table
    # either, so without that a refusal (a 403, a 429) reads exactly like a
    # month timeanddate has no data for (W-063).
    html = fetch_text(url, params={"year": year, "month": month})
    table = BeautifulSoup(html, "html.parser").find("table", {"id": table_id})
    if not isinstance(table, Tag):
        logger.warning("Skipping %s-%s: %s carries no table with id %r", year, month, url, table_id)
        return None
    return table


def _day_rows(table: Tag, year: str, month: str, minimum: int) -> list[tuple[str, list[Tag]]]:
    """Return ``(date, cells)`` for each day row of a monthly table.

    A day row is the one shape that carries exactly one ``<th>``: the day of the
    month. Header rows carry several and spacer rows none, so both fall out
    without having to be recognised individually.

    Args:
        table: The monthly table.
        year: Four-digit year.
        month: Two-digit month.
        minimum: Fewest cells a row must have to be read by position.

    Returns:
        One entry per readable day row, in page order. A row shorter than
        *minimum*, or whose header is not a day of that month, is logged and
        left out.
    """
    days_in_month = calendar.monthrange(int(year), int(month))[1]
    rows = []
    for tr in table.find_all("tr"):
        headers = tr.find_all("th")
        if len(headers) != 1:
            continue
        day = headers[0].get_text().strip()
        # One <th> is the shape of a day row, not proof of one: a footnote or
        # a "Note" row has it too, and would otherwise be published under the
        # date "2021-01-Note" (S-133). isascii() keeps out characters that
        # isdigit() accepts but int() refuses: "\u00b2" (superscript two) is a
        # digit to isdigit(), and int() raises ValueError on it, which nothing
        # between here and run() catches -- one such header would end the whole
        # collection rather than skip one row (S-141).
        if not (day.isascii() and day.isdigit() and 1 <= int(day) <= days_in_month):
            logger.warning(
                "Skipping a %s-%s row: its header %r is not a day of that month", year, month, day
            )
            continue
        date_str = f"{year}-{month}-{day.zfill(2)}"
        tds = cells(tr, minimum)
        if tds is None:
            logger.warning(
                "Skipping %s: the row carries %d of the %d cells it is read by position with",
                date_str,
                len(tr.find_all("td")),
                minimum,
            )
            continue
        rows.append((date_str, tds))
    return rows


def _time(cell: Tag, date_str: str, column: str) -> str:
    """Return the ``HH:MM`` a cell states, zero-padded.

    Args:
        cell: The cell to read.
        date_str: The day being read, for the log message.
        column: What the cell should have stated, for the log message.

    Returns:
        The time, or :data:`_MISSING_TIME` when the cell states none in a
        24-hour clock -- the rest of the day is still good, so the gap stays in
        this one column.
    """
    text = cell.get_text().strip()
    match = _TIME_RE.search(text)
    # Out of range is the same gap as no match: "25:99" is not a time that is
    # merely late, and passing it on would publish a value no clock shows.
    if match is not None and (
        int(match.group("hour")) >= _HOURS_PER_DAY
        or int(match.group("minute")) >= _MINUTES_PER_HOUR
    ):
        match = None
    if match is None:
        # The cell text goes in the message: the column name alone cannot say
        # whether the page changed clocks or dropped the reading, and those
        # need different fixes.
        logger.warning(
            "%s: %s cell %r states no 24-hour time, so that column is a gap",
            date_str,
            column,
            text,
        )
        return _MISSING_TIME
    return f"{int(match.group('hour')):02d}:{match.group('minute')}"


def _sun_day(tds: list[Tag], date_str: str) -> dict:
    """Read the four sun columns off one row of the monthly sun table."""
    day_length = _DAY_LENGTH_RE.search(tds[_SUN_COL_DAY_LENGTH].get_text())
    if day_length is None:
        logger.warning(
            "%s: daylength cell %r states no HH:MM:SS, so that column is a gap",
            date_str,
            tds[_SUN_COL_DAY_LENGTH].get_text().strip(),
        )
    return {
        "sunrise": _time(tds[_SUN_COL_SUNRISE], date_str, "sunrise"),
        "sunset": _time(tds[_SUN_COL_SUNSET], date_str, "sunset"),
        "solar_noon": _time(tds[_SUN_COL_SOLAR_NOON], date_str, "solar noon"),
        "day_length": _MISSING_TIME if day_length is None else day_length.group("value"),
    }


def _moon_events(tds: list[Tag], date_str: str) -> tuple[str | None, str | None, int] | None:
    """Walk the rise/set/rise slots at the front of a moon row.

    Args:
        tds: The row's cells.
        date_str: The day being read, for the log message.

    Returns:
        ``(moonrise, moonset, end)``, where either time is ``None`` on a day the
        event does not happen and *end* is the index of the first cell after
        the slots -- or ``None`` for the whole tuple when the row ends before
        the walk does, which means the layout is not the one read here and the
        cells after it cannot be trusted either.

    A second moonrise overwrites the first, which is what the legacy script did
    and therefore what the committed file holds. Keeping the earlier one would
    make a re-collection disagree with history on exactly the days the moon
    rises twice.
    """
    moonrise = moonset = None
    index = 0
    for slot in range(_MOON_EVENT_SLOTS):
        if index >= len(tds):
            logger.warning(
                "Skipping %s: the moon row ran out after %d of %d rise/set slots",
                date_str,
                slot,
                _MOON_EVENT_SLOTS,
            )
            return None
        cell = tds[index]
        if colspan(cell) == _MOON_EMPTY_SLOT_COLSPAN:
            index += 1
            continue
        if slot == _MOON_SLOT_INDEX_SET:
            moonset = _time(cell, date_str, "moonset")
        else:
            moonrise = _time(cell, date_str, "moonrise")
        index += _MOON_FILLED_SLOT_CELLS
    return moonrise, moonset, index


_TRAILING_MERGED = "merged"
"""Tail kind: the one merged full-moon cell (see :func:`_trailing_block_kind`)."""
_TRAILING_CELLS = "cells"
"""Tail kind: the four separate trailing cells (see :func:`_trailing_block_kind`)."""


def _trailing_block_kind(tds: list[Tag], end: int) -> str | None:
    """Say which trailing block follows the slot walk, if either.

    The tail is decided here and only here, from every cell after the walk: the
    caller branches on the answer and never looks at ``tds[-1]`` itself. Judging
    width and kind in two places is what let a four-cell tail ending in a merged
    cell read as a full moon with three cells never looked at (W-065).

    Args:
        tds: The row's cells.
        end: Index of the first cell after the rise/set slots.

    Returns:
        :data:`_TRAILING_MERGED` when exactly one cell remains and it spans the
        whole block; :data:`_TRAILING_CELLS` when exactly the block's width of
        cells remains and none of them spans more than one column; ``None`` for
        any other tail, where a read from the end would take a moonset or a
        stray column for the meridian passing (W-062).
    """
    tail = tds[end:]
    if len(tail) == 1 and colspan(tail[0]) == _MOON_MERGED_TRAILING_COLSPAN:
        return _TRAILING_MERGED
    if len(tail) == _MOON_TRAILING_CELLS and all(colspan(cell) == 1 for cell in tail):
        return _TRAILING_CELLS
    return None


def _moon_day(tds: list[Tag], date_str: str) -> dict | None:
    """Read the four moon columns off one row of the monthly moon table.

    Returns:
        The columns, or ``None`` when the slot walk ran off the end or the cells
        after it are neither trailing block -- the caller then drops that day
        rather than publishing it with the moon half quietly blank or read from
        the wrong cells.
    """
    events = _moon_events(tds, date_str)
    if events is None:
        return None
    moonrise, moonset, end = events
    kind = _trailing_block_kind(tds, end)
    if kind is None:
        logger.warning(
            "Skipping %s: the moon row ends in %d cells after its rise/set slots, not the %d "
            "single-column cells (or the one merged cell) its trailing block is read from",
            date_str,
            len(tds) - end,
            _MOON_TRAILING_CELLS,
        )
        return None

    if kind == _TRAILING_MERGED:
        return {
            "moonrise": moonrise,
            "moonset": moonset,
            "moon_transit": None,
            "moon_illumination_pct": _FULL_MOON_ILLUMINATION_PCT,
        }

    illumination = _ILLUMINATION_RE.search(tds[_MOON_COL_ILLUMINATION].get_text())
    if illumination is None:
        logger.warning(
            "%s: illumination cell %r states no percentage, so that column is a gap",
            date_str,
            tds[_MOON_COL_ILLUMINATION].get_text().strip(),
        )
    return {
        "moonrise": moonrise,
        "moonset": moonset,
        "moon_transit": _time(tds[_MOON_COL_TRANSIT], date_str, "meridian passing") or None,
        "moon_illumination_pct": (
            None if illumination is None else float(illumination.group("value"))
        ),
    }


def _sun_month(year: str, month: str) -> dict[str, dict]:
    """Return the sun columns for one month, keyed by date."""
    table = _table(_SUN_URL, year, month, _SUN_TABLE_ID)
    if table is None:
        return {}
    return {
        date_str: _sun_day(tds, date_str)
        for date_str, tds in _day_rows(table, year, month, _SUN_ROW_CELLS)
    }


def _moon_month(year: str, month: str) -> dict[str, dict]:
    """Return the moon columns for one month, keyed by date."""
    table = _table(_MOON_URL, year, month, _MOON_TABLE_ID)
    if table is None:
        return {}
    days = {}
    for date_str, tds in _day_rows(table, year, month, _MOON_TRAILING_CELLS):
        columns = _moon_day(tds, date_str)
        if columns is not None:
            days[date_str] = columns
    return days


def fetch_month(year: str, month: str) -> list[SunMoon]:
    """Fetch one month of sun and moon times.

    The two tables live on separate pages, so a day is published only when both
    were readable -- the same inner join the legacy script's ``pd.merge`` did.
    Half a record would be worse than none: the file it is written to is the
    only copy of these columns, so a day with a sunrise and blank moon fields
    would overwrite good history with a gap.

    Args:
        year: Four-digit year as a string, e.g. ``"2021"``.
        month: Two-digit month as a string, e.g. ``"01"``.

    Returns:
        One record per readable day, in date order.

    Raises:
        requests.RequestException: Either page could not be reached, or was
            answered with an error status.
    """
    sun = _sun_month(year, month)
    moon = _moon_month(year, month)
    return [
        SunMoon(date=date_str, **sun[date_str], **moon[date_str])
        for date_str in sorted(sun.keys() & moon.keys())
    ]


def fetch_year(year: str) -> list[SunMoon]:
    """Fetch a whole year of sun and moon times, one month at a time.

    Both pages are served per month. A month whose page carries no table is
    logged and skipped; a transport failure is raised, because a host that
    cannot be reached says nothing about that one month.

    Args:
        year: Four-digit year as a string, e.g. ``"2021"``.

    Returns:
        One record per readable day, in date order.

    Raises:
        requests.RequestException: A page could not be reached, or was answered
            with an error status.
        WeatherPageStructureError: Not one day of the year was readable. The
            caller writes what this returns over the only copy of these columns,
            so a year of empty months is a page that changed shape, not a year
            with no sunrise (W-063).
    """
    records: list[SunMoon] = []
    for month in range(_FIRST_MONTH, _LAST_MONTH + 1):
        records.extend(fetch_month(year, f"{month:02d}"))
    if not records:
        raise WeatherPageStructureError(
            f"no sun/moon day of {year} was readable: every month's pages lacked "
            f"the {_SUN_TABLE_ID!r} or {_MOON_TABLE_ID!r} table, or no day was in both"
        )
    return records
