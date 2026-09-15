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
    - ``1634422256000`` (Unix timestamp in milliseconds, exactly 13 digits)
    - ``2021-10-17 06:10:56`` (naive local time)

    The first and third are absolute instants and are converted into *timezone*.
    The naive one is read as a wall clock already in *timezone* and is labelled
    with it. The offset one is returned exactly as written -- see ``Returns``.

    Args:
        rec_time: The timestamp string.
        timezone: IANA zone name used for the result and for naive inputs.

    Returns:
        An aware ``datetime``, whose zone depends on which form came in:

        - UTC ISO, epoch milliseconds, naive local: a ``datetime`` in
          *timezone*.
        - ISO with an offset: the offset **as written**, as a fixed-offset
          zone. It is never converted to *timezone* and never checked against
          it, so ``parse_datetime("2021-10-17T06:10:56+09:00")`` returns
          ``+09:00``, not Hong Kong. The four supported inputs are all
          ``+08:00`` in practice, which is why the callers see no difference;
          that this is a property of the data and not of this function is what
          ``TestParseDateTimeOffsetIsReturnedAsWritten`` pins.

        Two neighbouring shapes are not supported at all and raise
        ``ValueError`` through the naive branch: a ``Z`` string without
        milliseconds, and a *negative* offset (the branch tests for ``"+"``).
        Neither occurs in the data this parses.

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
