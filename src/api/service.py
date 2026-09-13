"""Database queries returning plain dicts in the export record shape.

The GraphQL layer maps these dicts onto Strawberry types; keeping the
service free of Strawberry makes it trivial to test against a temporary
database built from :class:`~run365days.export.records.ExportRecords`.
"""

from sqlalchemy import ColumnElement, Select, func, or_, select
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


def _page(stmt: Select, limit: int | None, offset: int) -> Select:
    """Apply the page window in SQL so unwanted rows never leave the database."""
    if offset:
        stmt = stmt.offset(offset)
    return stmt.limit(limit) if limit is not None else stmt


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
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    """List activities in start order with optional filters (inclusive dates) and page window."""
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
    return [_activity_dict(row) for row in session.scalars(_page(stmt, limit, offset))]


def activity(session: Session, activity_id: str) -> dict | None:
    """Return one activity or ``None``."""
    row = session.get(
        models.Activity, activity_id, options=[selectinload(models.Activity.warnings)]
    )
    return _activity_dict(row) if row else None


def _stride_filter(session: Session, activity_id: str, points: int) -> ColumnElement[bool] | None:
    """Return a ``seq`` predicate keeping roughly *points* evenly spaced rows, or ``None``.

    Args:
        session: Open read-only session.
        activity_id: Track owner.
        points: How many samples the caller wants.

    Returns:
        A predicate selecting every ``total // points``-th row plus the final
        one, or ``None`` when the track already fits in *points* rows.
    """
    total, last_seq = session.execute(
        select(func.count(), func.max(models.TrackPoint.seq)).where(
            models.TrackPoint.activity_id == activity_id
        )
    ).one()
    if total <= points:
        return None
    stride = total // points
    # The modulo keeps the first sample; the last one is added back because a
    # route's end point is what makes the downsampled track look complete.
    return or_(models.TrackPoint.seq % stride == 0, models.TrackPoint.seq == last_seq)


def track(session: Session, activity_id: str, points: int | None = None) -> list[dict]:
    """Return the stored track for an activity, optionally downsampled to *points*.

    The thinning happens in SQL, so asking for a handful of samples never
    materialises the several hundred rows a run stores.
    """
    stmt = select(models.TrackPoint).where(models.TrackPoint.activity_id == activity_id)
    if points:
        stride = _stride_filter(session, activity_id, points)
        if stride is not None:
            stmt = stmt.where(stride)
    rows = [
        {col: getattr(p, col) for col in TRACK_COLUMNS}
        for p in session.scalars(stmt.order_by(models.TrackPoint.seq))
    ]
    return downsample(rows, points) if points else rows


def weight(
    session: Session,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    """List weigh-ins in date order with optional inclusive date filters and page window."""
    stmt = select(models.WeightEntry)
    if date_from:
        stmt = stmt.where(models.WeightEntry.date >= date_from)
    if date_to:
        stmt = stmt.where(models.WeightEntry.date <= date_to)
    stmt = _page(stmt.order_by(models.WeightEntry.date), limit, offset)
    return [
        {"date": w.date, "weight_lbs": w.weight_lbs, "weight_kg": w.weight_kg, "bmi": w.bmi}
        for w in session.scalars(stmt)
    ]


def daily_weather(
    session: Session,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    """List HKO daily rows in date order with optional date filters and page window."""
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
    stmt = _page(stmt.order_by(models.DailyWeather.date), limit, offset)
    return [{c: getattr(d, c) for c in cols} for d in session.scalars(stmt)]


def warnings(
    session: Session,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    """List HKO warnings in date order with optional date filters and page window."""
    stmt = select(models.WeatherWarning)
    if date_from:
        stmt = stmt.where(models.WeatherWarning.date >= date_from)
    if date_to:
        stmt = stmt.where(models.WeatherWarning.date <= date_to)
    cols = ("date", "type", "signal", "start_time", "end_time")
    stmt = _page(stmt.order_by(models.WeatherWarning.date, models.WeatherWarning.id), limit, offset)
    return [{c: getattr(w, c) for c in cols} for w in session.scalars(stmt)]
