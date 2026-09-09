"""Intermediate, source-agnostic records shared by the SQLite and JSON writers.

Everything here is plain Python (dicts, lists, dataclasses) so the writers
stay trivial and the shape can be unit-tested without touching disk.
"""

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime

from run365days.activities.models import Activity
from run365days.dashboard.builder import (
    TRACK_POINT_LIMIT,
    downsample,
    hourly_at,
    merge_temperature,
    total_ascent,
    track_rows,
    warnings_by_date,
)
from run365days.weight.models import WeightRecord

TRACK_COLUMNS = (
    "sec",
    "lat",
    "lon",
    "elevation_m",
    "distance_m",
    "speed_mps",
    "cadence",
    "temp_c",
)
"""Column order of every track row produced by :func:`build_records`."""

_CADENCE_FLOOR = 50
_STEPS_PER_CADENCE_SAMPLE = 2
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass
class ExportRecords:
    """Everything the writers need, already shaped for storage.

    Attributes:
        year: Challenge year.
        generated_at: ISO timestamp of the export run.
        activities: One dict per run, keyed as listed in :func:`activity_record`.
        tracks: ``{activity_id: [row, ...]}`` with rows in :data:`TRACK_COLUMNS` order.
        weight: One dict per weigh-in (``date``, ``weight_lbs``, ``weight_kg``, ``bmi``).
        daily_weather: One dict per day from the HKO daily extract.
        warnings: One dict per HKO warning (``date``, ``type``, ``signal``,
            ``start_time``, ``end_time``).
    """

    year: int
    generated_at: str
    activities: list[dict] = field(default_factory=list)
    tracks: dict[str, list[list]] = field(default_factory=dict)
    weight: list[dict] = field(default_factory=list)
    daily_weather: list[dict] = field(default_factory=list)
    warnings: list[dict] = field(default_factory=list)


def _round(value, ndigits: int = 1):
    if value is None:
        return None
    try:
        if value != value:  # NaN
            return None
    except TypeError:
        return None
    return round(float(value), ndigits)


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def activity_record(
    activity: Activity,
    gpx: Activity | None,
    hourly: dict | None,
    warnings: list[str],
) -> dict:
    """Shape one run for storage.

    Args:
        activity: The TCX activity (device distance, calories, full track).
        gpx: Matching GPX activity for ambient temperature, if any.
        hourly: Nearest hourly weather from :func:`hourly_at`, if any.
        warnings: HKO warning signals active that day.

    Returns:
        A dict with snake_case keys: ``id``, ``date``, ``start_time``,
        ``day_of_year``, ``distance_km``, ``duration_sec``, ``pace_sec_per_km``,
        ``calories``, ``avg_cadence``, ``avg_temp_c``, ``elevation_min_m``,
        ``elevation_max_m``, ``ascent_m``, ``has_gps``, ``num_points``,
        ``weather`` (nested dict or ``None``) and ``warnings`` (list).
    """
    start = datetime.strptime(activity.date, _TIMESTAMP_FORMAT)
    points = activity.track_points
    elevations = [p.elevation for p in points if p.elevation is not None]
    cadences = [
        p.cadence * _STEPS_PER_CADENCE_SAMPLE
        for p in points
        if p.cadence is not None and p.cadence >= _CADENCE_FLOOR
    ]
    km = activity.distance_km or activity.distance_by_coord_km or 0.0
    return {
        "id": activity.activity_id,
        "date": start.strftime("%Y-%m-%d"),
        "start_time": start.strftime("%H:%M"),
        "day_of_year": start.timetuple().tm_yday,
        "distance_km": _round(km, 2),
        "duration_sec": int(round(activity.total_sec)),
        "pace_sec_per_km": int(round(activity.total_sec / km)) if km else None,
        "calories": activity.calories,
        "avg_cadence": _round(sum(cadences) / len(cadences), 0) if cadences else None,
        "avg_temp_c": _round(gpx.avg_temp, 1) if gpx else None,
        "elevation_min_m": _round(min(elevations), 0) if elevations else None,
        "elevation_max_m": _round(max(elevations), 0) if elevations else None,
        "ascent_m": _round(total_ascent(elevations), 0),
        "has_gps": any(p.lat is not None for p in points),
        "num_points": len(points),
        "weather": _weather_record(hourly),
        "warnings": list(warnings),
    }


def _weather_record(hourly: dict | None) -> dict | None:
    if hourly is None:
        return None
    return {
        "description": hourly.get("desc"),
        "temp_c": hourly.get("temp"),
        "humidity_pct": hourly.get("hum"),
        "wind_kmh": hourly.get("wind"),
    }


def weight_record(record: WeightRecord) -> dict:
    """Shape one weigh-in for storage."""
    return {
        "date": record.date,
        "weight_lbs": record.weight_lbs,
        "weight_kg": record.weight_kg,
        "bmi": record.bmi,
    }


def daily_weather_record(row: dict) -> dict:
    """Shape one HKO daily-extract row (raw scraper column names) for storage."""
    return {
        "date": row["Date"],
        "max_temp_c": _to_float(row.get("Max. Temp")),
        "avg_temp_c": _to_float(row.get("Avg. Temp")),
        "min_temp_c": _to_float(row.get("Min. Temp")),
        "humidity_pct": _to_float(row.get("Humidity (%)")),
        "rainfall_mm": _to_float(row.get("Total Rainfall (mm)")),
        "wind_kmh": _to_float(row.get("Avg. Wind Speed (km/h)")),
        "sunrise": row.get("Sunrise"),
        "sunset": row.get("Sunset"),
    }


def warning_record(row: dict) -> dict:
    """Shape one HKO warning row (raw scraper column names) for storage."""
    return {
        "date": row["Date"],
        "type": row.get("Type"),
        "signal": row.get("Warning_Signal"),
        "start_time": row.get("Start_Time"),
        "end_time": row.get("End_Time"),
    }


def build_records(
    year: int,
    tcx_activities: Iterable[Activity],
    gpx_activities: Iterable[Activity],
    weight_records: Iterable[WeightRecord],
    hko_rows: list[dict],
    hourly_rows: list[dict],
    warning_rows: list[dict],
    point_limit: int = TRACK_POINT_LIMIT,
    generated_at: str | None = None,
) -> ExportRecords:
    """Turn parsed sources into :class:`ExportRecords`.

    Args:
        year: Challenge year.
        tcx_activities: Primary activities.
        gpx_activities: GPX activities joined by id for temperature.
        weight_records: Daily weigh-ins.
        hko_rows: HKO daily extract rows.
        hourly_rows: Hourly weather rows.
        warning_rows: HKO warning rows.
        point_limit: Maximum track points stored per activity.
        generated_at: Override the export timestamp (for tests).

    Returns:
        The shaped records, activities sorted by start time.
    """
    gpx_by_id = {a.activity_id: a for a in gpx_activities}
    warn_map = warnings_by_date(warning_rows)
    records = ExportRecords(
        year=year,
        generated_at=generated_at or datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    )

    for act in sorted(tcx_activities, key=lambda a: a.date):
        gpx = gpx_by_id.get(act.activity_id)
        date, hhmm = act.date[:10], act.date[11:16]
        records.activities.append(
            activity_record(act, gpx, hourly_at(hourly_rows, date, hhmm), warn_map.get(date, []))
        )
        temps = merge_temperature(act.track_points, gpx.track_points if gpx else None)
        records.tracks[act.activity_id] = downsample(track_rows(act, temps), point_limit)

    records.weight = [weight_record(r) for r in weight_records]
    records.daily_weather = [daily_weather_record(r) for r in hko_rows]
    records.warnings = [warning_record(r) for r in warning_rows if r.get("Warning_Signal")]
    return records
