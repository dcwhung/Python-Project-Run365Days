from datetime import datetime, timedelta

import dateutil.parser
import pytz

_HK_TZ = pytz.timezone("Asia/Hong_Kong")


def parse_datetime(rec_time: str, timezone: str = "Asia/Hong_Kong") -> datetime:
    """Parse a datetime string in any of the four Garmin/HKO formats and
    return a timezone-aware datetime in the requested timezone.

    Supported formats
    -----------------
    - '2021-10-16T22:10:56.000Z'  (UTC ISO with milliseconds)
    - '2021-10-17T06:10:56+08:00' (ISO with offset)
    - '1634451056000'             (Unix ms timestamp)
    - '2021-10-17 06:10:56'       (local naive)
    """
    tz = pytz.timezone(timezone)
    rec_time = rec_time.strip()

    if "T" in rec_time:
        if "." in rec_time and rec_time.endswith("Z"):
            return dateutil.parser.parse(rec_time).replace(tzinfo=pytz.UTC).astimezone(tz)
        elif "+" in rec_time:
            return dateutil.parser.parse(rec_time)
    elif rec_time.isdigit() and len(rec_time) == 13:
        return datetime.utcfromtimestamp(round(int(rec_time) / 1000, 1)).replace(tzinfo=tz)

    return datetime.strptime(rec_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)


def seconds_to_hhmmss(total_sec: float) -> str:
    """Convert a total-seconds float to 'HH:MM:SS' string."""
    return str(timedelta(seconds=int(total_sec))).split(".")[0]


def hhmmss_to_seconds(time_str: str) -> float:
    """Convert 'HH:MM:SS' string to total seconds."""
    return (datetime.strptime(time_str, "%H:%M:%S") - datetime(1900, 1, 1)).total_seconds()


def pace_str(total_sec: float, distance_km: float) -> str:
    """Return 'MM:SS' per-km pace string."""
    if distance_km <= 0:
        return ""
    return str(timedelta(seconds=total_sec / distance_km)).split(".")[0]


def activity_time_range(start: datetime, end: datetime, period: int = 30):
    """Return (floor, ceiling) rounded to *period*-minute boundaries."""
    floor = start.replace(minute=0, second=0) + timedelta(minutes=(start.minute // period) * period)
    ceiling = end.replace(minute=0, second=0) + timedelta(
        minutes=(end.minute // period + 1) * period
    )
    return floor, ceiling
