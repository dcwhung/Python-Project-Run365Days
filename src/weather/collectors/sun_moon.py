"""Fetch sunrise, sunset and moon times from timeanddate.com.

The port of ``legacy/03_GetSunMoonRiseSetHistory.py`` (AU-037). Until it
existed, :class:`~run365days.weather.models.SunMoon` was the one weather
dataclass with no writer and ``config.SUN_MOON_JSON`` named a file nothing in
the package could produce, so re-collecting the weather could only ever shrink
what ``data/raw/weather/`` holds.

What the page layout below rests on
-----------------------------------
Two pieces of evidence, and it is worth being exact about which is which.

The **output contract** is settled: every column name, every value format, the
``"/"`` marker and the trailing ``%`` are fixed by the 365 committed rows in
``sun_moon_rise_set_history.json``, which that legacy script produced.
:class:`SunMoon`'s raw-row pair owns them and the tests compare against real
rows out of that file. This is the half CUI-0010 found broken in the daily
extract -- a collector filling a field name the model did not have -- and it is
the half that is checked against something outside this repository's guesses.

The **page layout** -- which table id, which cell index -- is read off the
legacy script and cannot be re-checked against timeanddate today. Two things
corroborate it beyond "the script says so": the sun table's solar-noon index of
10 matches the twelve columns timeanddate documents for that table, and the
moon table's merged-meridian branch is confirmed by the file itself, where all
twelve days with ``Moon Transit`` of ``"/"`` are full moons carrying exactly
``100.0%``. Everything else about the layout is unverified, which is why every
positional read here is guarded: a page that has moved reports a gap or drops a
row and says so, rather than returning a number from a neighbouring column the
way the legacy character slices did (CUI-0013, CUI-0018).
"""

import calendar
import logging
import re

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from run365days.common import config
from run365days.weather.collectors.html_reads import cells
from run365days.weather.models import SunMoon

logger = logging.getLogger(__name__)

_SUN_URL = "https://timeanddate.com/sun/hong-kong/hong-kong"
_MOON_URL = "https://timeanddate.com/moon/hong-kong/hong-kong"

_SUN_TABLE_ID = "as-monthsun"
_MOON_TABLE_ID = "tb-7dmn"

# Cell order of one row of the monthly sun table. Left to right timeanddate
# serves: sunrise, sunset, daylength, difference, astronomical twilight start
# and end, nautical start and end, civil start and end, solar noon, and the
# sun's distance. The four this collector keeps are named; the eight it skips
# are not, and their absence from this block is what says they are skipped on
# purpose.
_SUN_COL_SUNRISE = 0
_SUN_COL_SUNSET = 1
_SUN_COL_DAY_LENGTH = 2
_SUN_COL_SOLAR_NOON = 10

# Eleven cells, so the widest index read is the last one. A shorter row either
# raises or -- worse -- lands on a neighbouring column, so it is a data gap to
# report rather than a row to salvage.
_SUN_ROW_CELLS = 11

# The moon table opens with three rise/set slots: a moonrise, a moonset, and a
# second moonrise, because a day about fifty minutes longer than the moon's
# gives room for two. Each slot is either a time cell followed by a compass
# bearing, or a single cell spanning both when the event does not happen that
# day.
_MOON_EVENT_SLOTS = 3
_MOON_SLOT_INDEX_SET = 1
_MOON_FILLED_SLOT_CELLS = 2
_MOON_EMPTY_SLOT_COLSPAN = 2

# After the slots come four cells: meridian passing time, its altitude, the
# moon's distance, and the illuminated fraction. They are read from the end
# because the slots in front of them are not a fixed width.
_MOON_COL_TRANSIT = -4
_MOON_COL_ILLUMINATION = -1
_MOON_TRAILING_CELLS = 4

# On the day of a full moon the meridian passing falls outside the calendar day
# and timeanddate merges that whole trailing block into one cell. The legacy
# script answered a flat 100.0% there, and the committed file shows it was
# right to: all twelve rows whose Moon Transit is "/" are full moons, and all
# twelve carry exactly this figure.
_MOON_MERGED_TRAILING_COLSPAN = 4
_FULL_MOON_ILLUMINATION_PCT = 100.0

# A clock time, rejecting the two ways a character slice used to read one that
# was not there. The lookbehind stops a longer number ending in the pattern
# from matching; "(?!:)" keeps this from taking the HH:MM off the front of a
# daylength; and "(?![ap]m)" refuses a 12-hour clock outright, because
# "7:05 pm" sliced to five characters reads as 07:05 -- a real time, twelve
# hours out, with nothing to show it was ever wrong (CUI-0018).
_TIME_RE = re.compile(r"(?<!\d)(?P<hour>\d{1,2}):(?P<minute>\d{2})(?!:)(?!\s*[ap]m)", re.IGNORECASE)
_DAY_LENGTH_RE = re.compile(r"(?<!\d)(?P<value>\d{1,2}:\d{2}:\d{2})(?!\d)")
_ILLUMINATION_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*%")

_MISSING_TIME = ""
"""What a sun column answers when its cell states no time. The four sun fields
are a plain ``str`` on :class:`SunMoon`, so this is the same empty-string
default the rest of ``weather.models`` gives a missing string column."""


def _table(url: str, year: str, month: str, table_id: str) -> Tag | None:
    """Fetch one month's page and return the table it should carry.

    Args:
        url: Page to fetch.
        year: Four-digit year.
        month: Two-digit month.
        table_id: ``id`` of the table wanted.

    Returns:
        The table, or ``None`` when the page carries no such element -- one
        month with no data, which the caller skips while keeping the other
        eleven.

    Raises:
        requests.RequestException: The page could not be reached. Left uncaught
            on purpose: swallowing it would turn one unreachable host into
            twelve silently empty months (AU-013).
    """
    html = requests.get(
        url, params={"year": year, "month": month}, timeout=config.HTTP_TIMEOUT
    ).text
    table = BeautifulSoup(html, "html.parser").find("table", {"id": table_id})
    if table is None:
        logger.warning(
            "Skipping %s-%s: %s carries no table with id %r",
            year,
            month,
            url,
            table_id,
        )
        return None
    return table


def _day_rows(table: Tag, year: str, month: str, minimum: int) -> list[tuple[str, list[Tag]]]:
    """Return ``(date, cells)`` for each day row of a monthly table.

    A day row is the one shape that carries exactly one ``<th>``: the day of
    the month. Header rows carry several and spacer rows none, so both fall out
    without having to be recognised individually.

    Args:
        table: The monthly table.
        year: Four-digit year.
        month: Two-digit month.
        minimum: Fewest cells a row must have to be read by position.

    Returns:
        One entry per readable day row, in page order. A row shorter than
        *minimum* is logged and left out.
    """
    rows = []
    for tr in table.find_all("tr"):
        headers = tr.find_all("th")
        if len(headers) != 1:
            continue
        date_str = f"{year}-{month}-{headers[0].get_text().strip().zfill(2)}"
        tds = cells(tr, minimum)
        if tds is None:
            logger.warning(
                "Skipping %s: the row carries %d of the %d cells its readings "
                "are told apart by position with",
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
    if match is None:
        # The cell text goes in the message: naming only the column cannot say
        # whether the page changed clocks or dropped the reading altogether,
        # and those need different fixes.
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


def _is_empty_slot(cell: Tag) -> bool:
    """Say whether a rise/set slot is the merged cell standing for "not today"."""
    try:
        return int(cell.get("colspan", 1)) == _MOON_EMPTY_SLOT_COLSPAN
    except (TypeError, ValueError):
        return False


def _moon_events(tds: list[Tag], date_str: str) -> tuple[str | None, str | None] | None:
    """Walk the rise/set/rise slots at the front of a moon row.

    Args:
        tds: The row's cells.
        date_str: The day being read, for the log message.

    Returns:
        ``(moonrise, moonset)``, either of which is ``None`` on a day the event
        does not happen -- or ``None`` for the pair when the row ends before
        the walk does, which means the layout is not the one read here and the
        cells after it cannot be trusted either.

    A second moonrise overwrites the first, which is what the legacy script did
    and therefore what the committed file holds. Keeping the earlier one would
    make a re-collection disagree with history on exactly the days the moon
    rises twice, and there is no way to tell from the file which of the two any
    given row recorded.
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
        if _is_empty_slot(cell):
            index += 1
            continue
        if slot == _MOON_SLOT_INDEX_SET:
            moonset = _time(cell, date_str, "moonset")
        else:
            moonrise = _time(cell, date_str, "moonrise")
        index += _MOON_FILLED_SLOT_CELLS
    return moonrise, moonset


def _moon_day(tds: list[Tag], date_str: str) -> dict | None:
    """Read the four moon columns off one row of the monthly moon table.

    Returns:
        The columns, or ``None`` when the row's slot walk ran off the end --
        reported by the caller dropping that day rather than publishing it with
        the moon half quietly blank.
    """
    events = _moon_events(tds, date_str)
    if events is None:
        return None
    moonrise, moonset = events

    if int(tds[_MOON_COL_ILLUMINATION].get("colspan", 1)) == _MOON_MERGED_TRAILING_COLSPAN:
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
    Half a record would be worse than none here: the file it is written to is
    the only copy of these columns, so a day carrying a sunrise and four blank
    moon fields would overwrite good history with a gap.

    Args:
        year: Four-digit year as a string, e.g. ``"2021"``.
        month: Two-digit month as a string, e.g. ``"01"``.

    Returns:
        One record per readable day, in date order.

    Raises:
        requests.RequestException: Either page could not be reached.
    """
    sun = _sun_month(year, month)
    moon = _moon_month(year, month)
    return [
        SunMoon(date=date_str, **sun[date_str], **moon[date_str])
        for date_str in sorted(sun.keys() & moon.keys())
    ]


def fetch_year(year: str) -> list[SunMoon]:
    """Fetch a whole year of sun and moon times, a month at a time.

    Both pages are served per month, so a year is twenty-four requests. A month
    whose page carries no table is logged and skipped; a transport failure is
    raised, because a host that cannot be reached says nothing about that one
    month.

    Args:
        year: Four-digit year as a string, e.g. ``"2021"``.

    Returns:
        One record per readable day, in date order.

    Raises:
        requests.RequestException: A page could not be reached.
    """
    records: list[SunMoon] = []
    for month in range(1, len(calendar.month_name)):
        records.extend(fetch_month(year, f"{month:02d}"))
    return records
