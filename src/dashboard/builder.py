"""Build the JSON payload consumed by the web dashboard.

The dashboard is a static HTML page. It reads one JavaScript file,
``data.js``, which assigns the payload to ``window.RUN365``. This module
turns parsed activities, weight records and weather history into that
payload. All functions here are pure so they can be unit tested.

Payload shape (all numbers rounded to keep the file small)::

    {
      "year": 2021,
      "generated": "2026-09-08T12:00:00",
      "activities": [ {id, date, time, doy, km, sec, pace, kcal, cad,
                       temp, eleMin, eleMax, ascent, gps, pts,
                       wx: {desc, temp, hum, wind} | null,
                       warn: [signal, ...]} ],
      "tracks": { id: [[sec, lat, lon, ele, dist_m, speed, cad, temp], ...] },
      "weight": [[date, lbs], ...],
      "weather": [ {date, max, avg, min, hum, rain, wind, sunrise, sunset} ]
    }
"""

import json
import math
from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path

from run365days.activities.models import Activity, TrackPoint

TRACK_POINT_LIMIT = 150


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


# ── activity summary ───────────────────────────────────────────────────────
def activity_summary(
    activity: Activity,
    gpx: Activity | None,
    hourly: dict | None,
    warnings: list[str],
) -> dict:
    """Build the per-run summary shown in lists, heatmaps and charts.

    Args:
        activity: The TCX activity (distance, calories, track).
        gpx: The matching GPX activity for temperature, if any.
        hourly: Nearest hourly weather from :func:`hourly_at`, if any.
        warnings: HKO warning signals active that day.

    Returns:
        A JSON-ready dict; see the module docstring for the key list.
    """
    start = datetime.strptime(activity.date, "%Y-%m-%d %H:%M:%S")
    pts = activity.track_points
    eles = [p.elevation for p in pts if p.elevation is not None]
    cads = [p.cadence * 2 for p in pts if p.cadence is not None and p.cadence >= 50]
    has_gps = any(p.lat is not None for p in pts)
    km = activity.distance_km or activity.distance_by_coord_km or 0.0
    return {
        "id": activity.activity_id,
        "date": start.strftime("%Y-%m-%d"),
        "time": start.strftime("%H:%M"),
        "doy": start.timetuple().tm_yday,
        "km": _num(km, 2),
        "sec": int(round(activity.total_sec)),
        "pace": int(round(activity.total_sec / km)) if km else None,
        "kcal": activity.calories,
        "cad": _num(sum(cads) / len(cads), 0) if cads else None,
        "temp": _num(gpx.avg_temp, 1) if gpx else None,
        "eleMin": _num(min(eles), 0) if eles else None,
        "eleMax": _num(max(eles), 0) if eles else None,
        "ascent": _num(total_ascent(eles), 0),
        "gps": has_gps,
        "pts": len(pts),
        "wx": hourly,
        "warn": warnings,
    }


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


def daily_weather(rows: list[dict]) -> list[dict]:
    """Convert HKO daily-extract rows to the compact dashboard shape."""
    return [
        {
            "date": r["Date"],
            "max": _to_float(r.get("Max. Temp")),
            "avg": _to_float(r.get("Avg. Temp")),
            "min": _to_float(r.get("Min. Temp")),
            "hum": _to_float(r.get("Humidity (%)")),
            "rain": _to_float(r.get("Total Rainfall (mm)")),
            "wind": _to_float(r.get("Avg. Wind Speed (km/h)")),
            "sunrise": r.get("Sunrise"),
            "sunset": r.get("Sunset"),
        }
        for r in rows
    ]


# ── payload ────────────────────────────────────────────────────────────────
def build_payload(
    year: int,
    tcx_activities: list[Activity],
    gpx_activities: list[Activity],
    weight_records,
    hko_rows: list[dict],
    hourly_rows: list[dict],
    warning_rows: list[dict],
    point_limit: int = TRACK_POINT_LIMIT,
) -> dict:
    """Assemble the complete dashboard payload.

    Args:
        year: Challenge year, echoed into the payload.
        tcx_activities: Primary activities (distance, calories, track).
        gpx_activities: Matching GPX activities, joined by ``activity_id``
            for per-point temperature.
        weight_records: Daily weigh-ins.
        hko_rows: HKO daily extract rows.
        hourly_rows: Hourly weather rows.
        warning_rows: HKO warning rows.
        point_limit: Maximum track points kept per activity.

    Returns:
        The payload described in the module docstring.
    """
    gpx_by_id = {a.activity_id: a for a in gpx_activities}
    warn_map = warnings_by_date(warning_rows)

    activities, tracks = [], {}
    for act in sorted(tcx_activities, key=lambda a: a.date):
        gpx = gpx_by_id.get(act.activity_id)
        date, hhmm = act.date[:10], act.date[11:16]
        summary = activity_summary(
            act, gpx, hourly_at(hourly_rows, date, hhmm), warn_map.get(date, [])
        )
        activities.append(summary)
        temps = merge_temperature(act.track_points, gpx.track_points if gpx else None)
        tracks[act.activity_id] = downsample(track_rows(act, temps), point_limit)

    return {
        "year": year,
        "generated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "activities": activities,
        "tracks": tracks,
        "weight": [[r.date, r.weight_lbs] for r in weight_records],
        "weather": daily_weather(hko_rows),
    }


def payload_to_js(payload: dict) -> str:
    """Serialise the payload as a ``window.RUN365 = {...}`` script body."""
    body = json.dumps(payload, separators=(",", ":"), allow_nan=False)
    return f"/* generated by run365-dashboard - do not edit */\nwindow.RUN365 = {body};\n"


def write_data_js(payload: dict, out_path: Path) -> None:
    """Write :func:`payload_to_js` output to *out_path*, creating parents."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(payload_to_js(payload))


def inline_data(html: str, payload: dict, data_src: str = "data.js") -> str:
    """Inline the payload into the dashboard HTML for a single-file build.

    Args:
        html: Contents of ``static/index.html``.
        payload: Output of :func:`build_payload`.
        data_src: The script ``src`` to replace.

    Returns:
        The HTML with the external script tag replaced by an inline script.

    Raises:
        ValueError: If the expected script tag is not present.
    """
    tag = f'<script src="{data_src}"></script>'
    if tag not in html:
        raise ValueError(f"{tag!r} not found in dashboard HTML")
    return html.replace(tag, "<script>\n" + payload_to_js(payload) + "</script>")
