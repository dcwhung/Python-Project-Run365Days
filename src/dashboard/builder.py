"""Pure helpers shared by the export pipeline and the API.

These started life as the payload builder for the v2 static dashboard. The
payload itself is gone (the React dashboard reads the API or the exported
JSON), but the calculations that shape a run are still done here so that
:mod:`run365days.export.records` and the GraphQL resolvers agree.
"""

import json
import math
from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path

from run365days.activities.models import Activity, TrackPoint

TRACK_POINT_LIMIT = 150
"""Default number of track points kept per run when downsampling."""


# ── helpers ────────────────────────────────────────────────────────────────
def _num(value, ndigits: int = 1):
    """Round a number for JSON, mapping None/NaN to None."""
    if value is None:
        return None
    try:
        if math.isnan(value):
            return None
    except TypeError:
        return None
    return round(value, ndigits)


def _to_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def downsample(points: Sequence, limit: int = TRACK_POINT_LIMIT) -> list:
    """Return at most *limit* evenly spaced items, always keeping first and last."""
    n = len(points)
    if n <= limit:
        return list(points)
    if limit < 2:
        return [points[-1]]
    step = (n - 1) / (limit - 1)
    return [points[round(i * step)] for i in range(limit)]


def total_ascent(elevations: Iterable[float | None], smooth: int = 4) -> float:
    """Sum of positive elevation gains after a moving-average smooth.

    Raw barometric altitude jitters by ±1 m between samples, which inflates
    ascent badly; smoothing over ``2*smooth+1`` samples matches Garmin's
    reported figure closely.
    """
    vals = [e for e in elevations if e is not None]
    if len(vals) < 2:
        return 0.0
    sm = []
    for i in range(len(vals)):
        lo, hi = max(0, i - smooth), min(len(vals), i + smooth + 1)
        sm.append(sum(vals[lo:hi]) / (hi - lo))
    return sum(max(0.0, b - a) for a, b in zip(sm, sm[1:], strict=False))


# ── track points ───────────────────────────────────────────────────────────
def merge_temperature(
    tcx_points: list[TrackPoint], gpx_points: list[TrackPoint] | None
) -> dict[str, float]:
    """Index GPX temperatures by timestamp so they can be joined to TCX points.

    Args:
        tcx_points: Unused; kept so the signature reads as a merge.
        gpx_points: Track from the GPX export, which carries temperature.

    Returns:
        ``{time: temperature_c}`` for every GPX point with a temperature.
    """
    if not gpx_points:
        return {}
    return {p.time: p.temperature for p in gpx_points if p.temperature is not None}


def track_rows(activity: Activity, temps: dict[str, float]) -> list[list]:
    """Flatten a track into compact rows for the dashboard.

    Args:
        activity: The activity whose track is exported.
        temps: ``{time: temperature_c}`` from :func:`merge_temperature`.

    Returns:
        One ``[sec, lat, lon, ele, dist_m, speed, cad, temp]`` list per
        point, with ``sec`` relative to the first point.
    """
    if not activity.track_points:
        return []
    t0 = datetime.strptime(activity.track_points[0].time, "%Y-%m-%d %H:%M:%S")
    rows = []
    for p in activity.track_points:
        sec = (datetime.strptime(p.time, "%Y-%m-%d %H:%M:%S") - t0).total_seconds()
        rows.append(
            [
                int(sec),
                _num(p.lat, 5),
                _num(p.lon, 5),
                _num(p.elevation, 1),
                _num(p.distance_m, 0),
                _num(p.speed, 2),
                p.cadence,
                _num(temps.get(p.time), 1),
            ]
        )
    return rows


# ── weather ────────────────────────────────────────────────────────────────
def load_jsonl(path: Path) -> list[dict]:
    """Read a JSON Lines file, returning ``[]`` if it does not exist."""
    if not path.exists():
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def hourly_at(rows: list[dict], date: str, time_hhmm: str) -> dict | None:
    """Pick the hourly observation closest to a time of day.

    Args:
        rows: Hourly weather rows as loaded from ``weather_history.json``.
        date: Day to search, ``YYYY-MM-DD``.
        time_hhmm: Target time ``HH:MM``.

    Returns:
        ``{desc, temp, hum, wind}`` for the nearest observation, or ``None``
        if the day has no rows.
    """
    target = int(time_hhmm[:2]) * 60 + int(time_hhmm[3:5])
    best, best_gap = None, 10**9
    for r in rows:
        if r.get("Date") != date:
            continue
        t = r.get("Time", "00:00")
        gap = abs(int(t[:2]) * 60 + int(t[3:5]) - target)
        if gap < best_gap:
            best, best_gap = r, gap
    if best is None:
        return None
    return {
        "desc": best.get("Description"),
        "temp": _to_float(best.get("Temperature (°C)")),
        "hum": _to_float(best.get("Humidity (%)")),
        "wind": _to_float(best.get("Wind (Km/h)")),
    }


def warnings_by_date(rows: list[dict]) -> dict[str, list[str]]:
    """Group warning rows into ``{date: [signal, ...]}`` without duplicates."""
    out: dict[str, list[str]] = {}
    for r in rows:
        sig = r.get("Warning_Signal")
        if sig:
            out.setdefault(r["Date"], [])
            if sig not in out[r["Date"]]:
                out[r["Date"]].append(sig)
    return out
