"""Database queries returning plain dicts in the export record shape.

The GraphQL layer maps these dicts onto Strawberry types; keeping the
service free of Strawberry makes it trivial to test against a temporary
database built from :class:`~run365days.export.records.ExportRecords`.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from run365days.dashboard.builder import downsample
from run365days.export import models
from run365days.export.records import TRACK_COLUMNS

_ACTIVITY_COLUMNS = (
    "id",
    "date",
    "start_time",
    "day_of_year",
    "distance_km",
    "duration_sec",
    "pace_sec_per_km",
    "calories",
    "avg_cadence",
    "avg_temp_c",
    "elevation_min_m",
    "elevation_max_m",
    "ascent_m",
    "has_gps",
    "num_points",
)


def _activity_dict(row: models.Activity) -> dict:
    out = {col: getattr(row, col) for col in _ACTIVITY_COLUMNS}
    out["weather"] = (
        {
            "description": row.weather_description,
            "temp_c": row.weather_temp_c,
            "humidity_pct": row.weather_humidity_pct,
            "wind_kmh": row.weather_wind_kmh,
        }
        if row.weather_description is not None or row.weather_temp_c is not None
        else None
    )
    out["warnings"] = [w.signal for w in row.warnings]
    return out


def meta(session: Session) -> dict:
    """Return ``{year, generated_at}`` from the meta table."""
    rows = {m.key: m.value for m in session.scalars(select(models.Meta))}
    return {"year": int(rows["year"]), "generated_at": rows["generated_at"]}


def activities(
    session: Session,
    date_from: str | None = None,
    date_to: str | None = None,
    min_km: float | None = None,
    has_gps: bool | None = None,
) -> list[dict]:
    """List activities in start order with optional filters (inclusive dates)."""
    stmt = select(models.Activity).options(selectinload(models.Activity.warnings))
    if date_from:
        stmt = stmt.where(models.Activity.date >= date_from)
    if date_to:
        stmt = stmt.where(models.Activity.date <= date_to)
    if min_km is not None:
        stmt = stmt.where(models.Activity.distance_km >= min_km)
    if has_gps is not None:
        stmt = stmt.where(models.Activity.has_gps.is_(has_gps))
    stmt = stmt.order_by(models.Activity.date, models.Activity.start_time)
    return [_activity_dict(row) for row in session.scalars(stmt)]


def activity(session: Session, activity_id: str) -> dict | None:
    """Return one activity or ``None``."""
    row = session.get(
        models.Activity, activity_id, options=[selectinload(models.Activity.warnings)]
    )
    return _activity_dict(row) if row else None


def track(session: Session, activity_id: str, points: int | None = None) -> list[dict]:
    """Return the stored track for an activity, optionally downsampled to *points*."""
    stmt = (
        select(models.TrackPoint)
        .where(models.TrackPoint.activity_id == activity_id)
        .order_by(models.TrackPoint.seq)
    )
    rows = [{col: getattr(p, col) for col in TRACK_COLUMNS} for p in session.scalars(stmt)]
    return downsample(rows, points) if points else rows


def weight(
    session: Session, date_from: str | None = None, date_to: str | None = None
) -> list[dict]:
    """List weigh-ins in date order with optional inclusive date filters."""
    stmt = select(models.WeightEntry)
    if date_from:
        stmt = stmt.where(models.WeightEntry.date >= date_from)
    if date_to:
        stmt = stmt.where(models.WeightEntry.date <= date_to)
    return [
        {"date": w.date, "weight_lbs": w.weight_lbs, "weight_kg": w.weight_kg, "bmi": w.bmi}
        for w in session.scalars(stmt.order_by(models.WeightEntry.date))
    ]


def daily_weather(
    session: Session, date_from: str | None = None, date_to: str | None = None
) -> list[dict]:
    """List HKO daily rows in date order with optional inclusive date filters."""
    stmt = select(models.DailyWeather)
    if date_from:
        stmt = stmt.where(models.DailyWeather.date >= date_from)
    if date_to:
        stmt = stmt.where(models.DailyWeather.date <= date_to)
    cols = (
        "date",
        "max_temp_c",
        "avg_temp_c",
        "min_temp_c",
        "humidity_pct",
        "rainfall_mm",
        "wind_kmh",
        "sunrise",
        "sunset",
    )
    return [
        {c: getattr(d, c) for c in cols}
        for d in session.scalars(stmt.order_by(models.DailyWeather.date))
    ]


def warnings(
    session: Session, date_from: str | None = None, date_to: str | None = None
) -> list[dict]:
    """List HKO warnings in date order with optional inclusive date filters."""
    stmt = select(models.WeatherWarning)
    if date_from:
        stmt = stmt.where(models.WeatherWarning.date >= date_from)
    if date_to:
        stmt = stmt.where(models.WeatherWarning.date <= date_to)
    cols = ("date", "type", "signal", "start_time", "end_time")
    return [
        {c: getattr(w, c) for c in cols}
        for w in session.scalars(
            stmt.order_by(models.WeatherWarning.date, models.WeatherWarning.id)
        )
    ]
