"""Timestamp parsing and formatting helpers (Hong Kong local time)."""

from datetime import datetime, timedelta
from datetime import timezone as dt_timezone
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import dateutil.parser

_UNIX_MS_DIGITS = 13
_MS_PER_SECOND = 1000
_MISSING_TZ_DATA_MESSAGE: Final[str] = (
    "No IANA time zone database entry for {timezone!r}. zoneinfo carries no data of "
    "its own: it reads the host database (usually /usr/share/zoneinfo) and falls back "
    "to the 'tzdata' PyPI package, and neither is available here. Install the "
    "declared dependency with `pip install tzdata`, or provide a host tz database."
)


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
    - ``1634422256000`` (Unix timestamp in milliseconds)
    - ``2021-10-17 06:10:56`` (naive local time)

    Args:
        rec_time: The timestamp string.
        timezone: IANA zone name used for the result and for naive inputs.

    Returns:
        A timezone-aware ``datetime`` in ``timezone``.

    Raises:
        MissingTimeZoneDataError: If no tz database is reachable on this host.
    """
    tz = _zone_info(timezone)
    rec_time = rec_time.strip()

    if "T" in rec_time:
        if "." in rec_time and rec_time.endswith("Z"):
            return dateutil.parser.parse(rec_time).replace(tzinfo=dt_timezone.utc).astimezone(tz)
        elif "+" in rec_time:
            return dateutil.parser.parse(rec_time)
    elif rec_time.isdigit() and len(rec_time) == _UNIX_MS_DIGITS:
        epoch_seconds = round(int(rec_time) / _MS_PER_SECOND, 1)
        # An epoch is an absolute instant, so it is read as UTC and converted; relabelling
        # it with *timezone* instead would shift the result by that zone's offset (AU-048).
        return datetime.fromtimestamp(epoch_seconds, tz=dt_timezone.utc).astimezone(tz)

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
