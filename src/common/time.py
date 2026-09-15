"""Timestamp parsing and formatting helpers (Hong Kong local time)."""

import re
from datetime import datetime, timedelta
from datetime import timezone as dt_timezone
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import dateutil.parser

_ISO_OFFSET_PATTERN: Final[re.Pattern[str]] = re.compile(r"[+-]\d{2}:?\d{2}$")
"""Trailing UTC offset of an ISO 8601 timestamp: ``+08:00``, ``-05:00``, ``+0800``.

Anchored at the end of the string because that is the only place an offset can be
told apart from the rest of the timestamp: a plain ``"-" in rec_time`` test matches
the date separators of every input, which is why the condition this replaces could
only look for ``"+"`` and dropped negative offsets into the naive branch (CUI-0006).
"""

_UNIX_MS_DIGITS: Final[int] = 13
"""Digits an epoch-millisecond timestamp has, and the only length that path accepts.

Exact, not a minimum: it is what separates epoch milliseconds from the
10-digit epoch *seconds* and the float-formatted ``1634422256000.0`` that
Garmin also emits, both of which must be rejected rather than mis-parsed.
Good until 2286, when epoch milliseconds reach 14 digits.
"""

_MS_PER_SECOND: Final[int] = 1000
"""Milliseconds in a second, for the epoch-ms to epoch-seconds conversion."""

_MISSING_TZ_DATA_MESSAGE: Final[str] = (
    "No IANA time zone database entry for {timezone!r}. zoneinfo carries no data of "
    "its own: it reads the host database (usually /usr/share/zoneinfo) and falls back "
    "to the 'tzdata' PyPI package, and neither is available here. Install the "
    "declared dependency with `pip install tzdata`, or provide a host tz database."
)
"""Body of :class:`MissingTimeZoneDataError`, formatted with the zone that was asked for.

Names the remedy rather than the symptom: the failure is an environment one and
the reader needs to know that zoneinfo carries no data of its own.
"""


class MissingTimeZoneDataError(RuntimeError):
    """No IANA time zone database is reachable on this host.

    Deliberately not a :class:`LookupError` or :class:`ValueError` subclass, unlike
    the :class:`~zoneinfo.ZoneInfoNotFoundError` it replaces. A missing tz database
    is an environment fault that applies to every input, so it must escape the
    per-file ``except`` lists that exist to drop individual unreadable records
    (W-004) and stop the run outright.
    """


def _zone_info(timezone: str) -> ZoneInfo:
    """Return the :class:`ZoneInfo` for *timezone* with an actionable failure.

    Args:
        timezone: IANA zone name.

    Returns:
        The requested zone.

    Raises:
        MissingTimeZoneDataError: If no tz database is reachable on this host.
    """
    try:
        return ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise MissingTimeZoneDataError(_MISSING_TZ_DATA_MESSAGE.format(timezone=timezone)) from exc


def parse_datetime(rec_time: str, timezone: str = "Asia/Hong_Kong") -> datetime:
    """Parse any of the four Garmin / HKO timestamp formats.

    Supported inputs:

    - ``2021-10-16T22:10:56.000Z`` (UTC ISO with milliseconds)
    - ``2021-10-17T06:10:56+08:00`` (ISO with offset)
    - ``1634422256000`` (Unix timestamp in milliseconds, exactly 13 digits)
    - ``2021-10-17 06:10:56`` (naive local time)

    The first three all name an absolute instant and are converted into *timezone*.
    The naive one carries no instant of its own: it is read as a wall clock already
    in *timezone* and is labelled with it.

    Args:
        rec_time: The timestamp string.
        timezone: IANA zone name used for the result and for naive inputs.

    Returns:
        A timezone-aware ``datetime`` in *timezone*, for every supported form.

        For the three instant-carrying forms this is a conversion: the instant is
        preserved and only the wall clock and offset change, so
        ``parse_datetime("2021-10-17T06:10:56+09:00")`` returns
        ``2021-10-17T05:10:56+08:00`` -- the same moment, stated in Hong Kong.
        The offset an ISO string carries is therefore read and then discarded;
        it decides which instant was meant, never how the result is labelled.
        Any ``±HH:MM`` or ``±HHMM`` offset is accepted, negative ones included.

        One neighbouring shape is not supported and raises ``ValueError``
        through the naive branch: a ``Z`` string *without* milliseconds, since
        the UTC branch requires a ``"."``. It does not occur in the data this
        parses -- every ``Z`` timestamp in the GPX and TCX exports has them --
        and ``TestParseDateTimeIsoOffsetIsConvertedToTimezone`` pins it as a
        measured fact rather than leaving it fixed in passing.

    Raises:
        MissingTimeZoneDataError: If no tz database is reachable on this host.
    """
    tz = _zone_info(timezone)
    rec_time = rec_time.strip()

    if "T" in rec_time:
        if "." in rec_time and rec_time.endswith("Z"):
            return dateutil.parser.parse(rec_time).replace(tzinfo=dt_timezone.utc).astimezone(tz)
        elif _ISO_OFFSET_PATTERN.search(rec_time):
            # An offset names an instant just as surely as a Z does, so it is
            # converted into *timezone* rather than handed back as written
            # (CUI-0006). The three exports of one run disagree about format --
            # GPX and TCX write Z, KML writes the local offset -- and only a
            # conversion here keeps them on the same wall clock once that offset
            # is something other than the +08:00 every tracked file carries.
            return dateutil.parser.parse(rec_time).astimezone(tz)
    elif rec_time.isdigit() and len(rec_time) == _UNIX_MS_DIGITS:
        # Quantised to a tenth of a second, which is coarser than the input:
        # a millisecond residue is rounded rather than carried, so 1634422256092
        # comes back as .100000 -- 8ms this timestamp never had. Kept because it
        # predates AU-048 and every value that reaches it is a whole second
        # (residues across the 660 tracked activities are 659 x 0 and 1 x 92), so
        # changing it would alter no output while changing a shared parser. The
        # fabrication is pinned by test_a_millisecond_residue_is_rounded_to_a
        # _tenth_of_a_second so removing the round is a deliberate act, not a
        # silent one. `fromtimestamp` takes the unrounded float perfectly well.
        epoch_seconds = round(int(rec_time) / _MS_PER_SECOND, 1)
        # Epoch milliseconds name an absolute instant (Garmin's beginTimestamp, equal to
        # its startTimeGmt), so the instant is converted into *timezone* rather than
        # relabelled: the wall clock legitimately differs from the UTC one. Garmin's
        # startTimeLocal is that wall clock re-encoded as if it were UTC and must never
        # reach here -- feeding it in lands 8 hours late, which is the defect AU-003 left
        # in place and AU-048 fixed. See TestParseDateTimeAbsoluteInstantInputs and
        # TestParseDateTimeEpochMilliseconds in tests/test_common_time.py.
        return datetime.fromtimestamp(epoch_seconds, tz=tz)

    return datetime.strptime(rec_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)


def seconds_to_hhmmss(total_sec: float) -> str:
    """Format a duration in seconds as ``HH:MM:SS``."""
    return str(timedelta(seconds=int(total_sec))).split(".")[0]


def hhmmss_to_seconds(time_str: str) -> float:
    """Convert an ``HH:MM:SS`` string to total seconds."""
    return (datetime.strptime(time_str, "%H:%M:%S") - datetime(1900, 1, 1)).total_seconds()


def pace_str(total_sec: float, distance_km: float) -> str:
    """Format pace as ``M:SS`` per kilometre, or ``""`` if distance is zero."""
    if distance_km <= 0:
        return ""
    return str(timedelta(seconds=total_sec / distance_km)).split(".")[0]


def activity_time_range(start: datetime, end: datetime, period: int = 30):
    """Widen a start / end pair to the surrounding *period*-minute boundaries.

    Args:
        start: Activity start time.
        end: Activity end time.
        period: Boundary size in minutes (default 30).

    Returns:
        ``(floor, ceiling)`` where ``floor <= start`` and ``ceiling > end``.
    """
    floor = start.replace(minute=0, second=0) + timedelta(minutes=(start.minute // period) * period)
    ceiling = end.replace(minute=0, second=0) + timedelta(
        minutes=(end.minute // period + 1) * period
    )
    return floor, ceiling
