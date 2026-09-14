"""Timestamp parsing and formatting helpers (Hong Kong local time)."""

from datetime import UTC, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dateutil.parser import isoparse

_UNIX_MS_DIGITS: Final[int] = 13
_MS_PER_SECOND: Final[int] = 1000
_UNPARSEABLE_MESSAGE: Final[str] = (
    "Unrecognised timestamp {rec_time!r}: expected ISO 8601 (with or without an offset) "
    "or a 13-digit Unix timestamp in milliseconds."
)
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
    """Parse a Garmin / HKO timestamp into a ``timezone``-aware ``datetime``.

    The string is handed to a real ISO 8601 reader rather than classified by surface
    features. Two bugs came out of the feature-sniffing it replaces: CUI-0006 (``"+" in
    rec_time`` rejected the negative offset ``-05:00``) and CUI-0016 (``"." in rec_time``
    demanded milliseconds, so plain ``...T06:10:56Z`` was not recognised as UTC). Both
    were the same mistake -- a proxy for "is this ISO?" instead of trying to read it --
    so the resolution order below states each shape completely:

    1. 13 ASCII digits -> Unix milliseconds, an absolute instant read as UTC.
    2. Anything ``dateutil.parser.isoparse`` accepts -> ISO 8601, in basic or extended
       form, with ``Z``, with an offset of either sign, or with none at all.
    3. Otherwise -> ``ValueError``.

    Epoch is settled first because ``isoparse`` also reads ISO basic format, under which a
    minority of 13-digit epochs are valid dates: ``1591101132141`` reads as year 1591.
    An offset-bearing input names an instant, so it is converted into *timezone*; a naive
    one carries no offset and is therefore taken as a wall clock already in *timezone*.

    Args:
        rec_time: The timestamp string.
        timezone: IANA zone name used for the result and for naive inputs.

    Returns:
        A timezone-aware ``datetime`` in ``timezone``.

    Raises:
        ValueError: If *rec_time* matches none of the supported shapes.
        MissingTimeZoneDataError: If no tz database is reachable on this host.
    """
    tz = _zone_info(timezone)
    rec_time = rec_time.strip()

    if rec_time.isdigit() and len(rec_time) == _UNIX_MS_DIGITS:
        epoch_seconds = round(int(rec_time) / _MS_PER_SECOND, 1)
        # An epoch is an absolute instant, so it is read as UTC and converted; relabelling
        # it with *timezone* instead would shift the result by that zone's offset (AU-048).
        return datetime.fromtimestamp(epoch_seconds, tz=UTC).astimezone(tz)

    try:
        parsed = isoparse(rec_time)
    except ValueError as exc:
        raise ValueError(_UNPARSEABLE_MESSAGE.format(rec_time=rec_time)) from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


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


def activity_time_range(
    start: datetime, end: datetime, period: int = 30
) -> tuple[datetime, datetime]:
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
