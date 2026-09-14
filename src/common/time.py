"""Timestamp parsing and formatting helpers (Hong Kong local time)."""

from datetime import datetime, timedelta
from datetime import timezone as dt_timezone
from zoneinfo import ZoneInfo

import dateutil.parser

_UNIX_MS_DIGITS = 13
_MS_PER_SECOND = 1000


def parse_datetime(rec_time: str, timezone: str = "Asia/Hong_Kong") -> datetime:
    """Parse any of the four Garmin / HKO timestamp formats.

    Supported inputs:

    - ``2021-10-16T22:10:56.000Z`` (UTC ISO with milliseconds)
    - ``2021-10-17T06:10:56+08:00`` (ISO with offset)
    - ``1634451056000`` (Unix timestamp in milliseconds)
    - ``2021-10-17 06:10:56`` (naive local time)

    Args:
        rec_time: The timestamp string.
        timezone: IANA zone name used for the result and for naive inputs.

    Returns:
        A timezone-aware ``datetime`` in ``timezone``.
    """
    tz = ZoneInfo(timezone)
    rec_time = rec_time.strip()

    if "T" in rec_time:
        if "." in rec_time and rec_time.endswith("Z"):
            return dateutil.parser.parse(rec_time).replace(tzinfo=dt_timezone.utc).astimezone(tz)
        elif "+" in rec_time:
            return dateutil.parser.parse(rec_time)
    elif rec_time.isdigit() and len(rec_time) == _UNIX_MS_DIGITS:
        epoch_seconds = round(int(rec_time) / _MS_PER_SECOND, 1)
        # KNOWN INCORRECT, kept deliberately. The epoch is read as UTC and then
        # relabelled -- not converted -- to *timezone*, so the result is 8 hours off for
        # Asia/Hong_Kong. AU-003 was scoped to the offset only and preserved this wall
        # clock; correcting the shift is AU-048. TestParseDateTimeWallClockUnchanged in
        # tests/test_common_time.py pins the current behaviour on purpose and must be
        # updated in the same change as this line.
        naive_utc = datetime.fromtimestamp(epoch_seconds, tz=dt_timezone.utc).replace(tzinfo=None)
        return naive_utc.replace(tzinfo=tz)

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
