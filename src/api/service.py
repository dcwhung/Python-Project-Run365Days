"""Database queries returning plain dicts in the export record shape.

The GraphQL layer maps these dicts onto Strawberry types; keeping the
service free of Strawberry makes it trivial to test against a temporary
database built from :class:`~run365days.export.records.ExportRecords`.
"""

from sqlalchemy import ColumnElement, Select, func, select
from sqlalchemy.orm import Session, selectinload

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


def _dated(stmt: Select, column, date_from: str | None, date_to: str | None) -> Select:
    """Apply an inclusive date window to *column*."""
    if date_from:
        stmt = stmt.where(column >= date_from)
    if date_to:
        stmt = stmt.where(column <= date_to)
    return stmt


def _activity_filters(
    stmt: Select,
    date_from: str | None,
    date_to: str | None,
    min_km: float | None,
    has_gps: bool | None,
) -> Select:
    """Apply the activity filters.

    Shared by the list and the count so the two can never disagree about what
    a filter means -- a count that answers a different question than the list
    it is compared against is worse than no count at all.
    """
    stmt = _dated(stmt, models.Activity.date, date_from, date_to)
    if min_km is not None:
        stmt = stmt.where(models.Activity.distance_km >= min_km)
    if has_gps is not None:
        stmt = stmt.where(models.Activity.has_gps.is_(has_gps))
    return stmt


def _count_dated(session: Session, model, date_from: str | None, date_to: str | None) -> int:
    """Count rows of a date-keyed table inside an inclusive date window."""
    stmt = _dated(select(func.count()).select_from(model), model.date, date_from, date_to)
    return session.scalar(stmt) or 0


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
    stmt = _activity_filters(stmt, date_from, date_to, min_km, has_gps)
    stmt = stmt.order_by(models.Activity.date, models.Activity.start_time)
    return [_activity_dict(row) for row in session.scalars(_page(stmt, limit, offset))]


def activities_count(
    session: Session,
    date_from: str | None = None,
    date_to: str | None = None,
    min_km: float | None = None,
    has_gps: bool | None = None,
) -> int:
    """Count activities matching the filters, ignoring any page window."""
    stmt = _activity_filters(
        select(func.count()).select_from(models.Activity), date_from, date_to, min_km, has_gps
    )
    return session.scalar(stmt) or 0


def activity(session: Session, activity_id: str) -> dict | None:
    """Return one activity or ``None``."""
    row = session.get(
        models.Activity, activity_id, options=[selectinload(models.Activity.warnings)]
    )
    return _activity_dict(row) if row else None


def _even_positions(total: int, points: int) -> list[int]:
    """Return *points* zero-based row positions spread evenly over *total* rows.

    Same arithmetic as :func:`run365days.dashboard.builder.downsample`, so the
    SQL-side thinning picks exactly the samples the in-Python one would.

    Args:
        total: Rows stored for the track.
        points: Samples wanted, assumed to be below *total*.

    Returns:
        Strictly increasing positions, first and last row included -- except in
        the degenerate case below.

    Note:
        ``points < 2`` leaves no room for both ends, and returns the final row
        alone rather than the first. That is not an oversight to tidy up:
        :func:`~run365days.dashboard.builder.downsample` resolves the same tie
        the same way, and this function exists to agree with it sample for
        sample. Changing it here alone would split the two data modes apart.
    """
    if points < 2:
        return [total - 1]
    step = (total - 1) / (points - 1)
    return [round(i * step) for i in range(points)]


def _even_sample_filter(session: Session, activity_id: str, points: int) -> ColumnElement | None:
    """Return a ``seq`` predicate keeping exactly *points* even samples, or ``None``.

    Args:
        session: Open read-only session.
        activity_id: Track owner.
        points: How many samples the caller wants.

    Returns:
        A predicate selecting the wanted rows, or ``None`` when the track
        already fits in *points* rows.
    """
    total = session.scalar(
        select(func.count())
        .select_from(models.TrackPoint)
        .where(models.TrackPoint.activity_id == activity_id)
    )
    if total <= points:
        return None
    # Positions, not seq arithmetic: row_number stays evenly spaced even if a
    # track is ever stored with gaps in seq. The IN list is bounded by
    # MAX_TRACK_POINTS, well under SQLite's bound-parameter ceiling.
    position = (func.row_number().over(order_by=models.TrackPoint.seq) - 1).label("position")
    numbered = (
        select(models.TrackPoint.seq.label("seq"), position)
        .where(models.TrackPoint.activity_id == activity_id)
        .subquery()
    )
    wanted = select(numbered.c.seq).where(numbered.c.position.in_(_even_positions(total, points)))
    return models.TrackPoint.seq.in_(wanted)


def track(session: Session, activity_id: str, points: int | None = None) -> list[dict]:
    """Return the stored track for an activity, optionally downsampled to *points*.

    The thinning happens in SQL and once only: an earlier version narrowed the
    rows with a floor stride and then downsampled the survivors again, and the
    two passes compounded into gaps that differed by a factor of two.
    """
    stmt = select(models.TrackPoint).where(models.TrackPoint.activity_id == activity_id)
    if points:
        sample = _even_sample_filter(session, activity_id, points)
        if sample is not None:
            stmt = stmt.where(sample)
    return [
        {col: getattr(p, col) for col in TRACK_COLUMNS}
        for p in session.scalars(stmt.order_by(models.TrackPoint.seq))
    ]


def weight(
    session: Session,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    """List weigh-ins in date order with optional inclusive date filters and page window."""
    stmt = _dated(select(models.WeightEntry), models.WeightEntry.date, date_from, date_to)
    stmt = _page(stmt.order_by(models.WeightEntry.date), limit, offset)
    return [
        {"date": w.date, "weight_lbs": w.weight_lbs, "weight_kg": w.weight_kg, "bmi": w.bmi}
        for w in session.scalars(stmt)
    ]


def weight_count(session: Session, date_from: str | None = None, date_to: str | None = None) -> int:
    """Count weigh-ins in the date window, ignoring any page window."""
    return _count_dated(session, models.WeightEntry, date_from, date_to)


def daily_weather(
    session: Session,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    """List HKO daily rows in date order with optional date filters and page window."""
    stmt = _dated(select(models.DailyWeather), models.DailyWeather.date, date_from, date_to)
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


def daily_weather_count(
    session: Session, date_from: str | None = None, date_to: str | None = None
) -> int:
    """Count HKO daily rows in the date window, ignoring any page window."""
    return _count_dated(session, models.DailyWeather, date_from, date_to)


def warnings(
    session: Session,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    """List HKO warnings in date order with optional date filters and page window."""
    stmt = _dated(select(models.WeatherWarning), models.WeatherWarning.date, date_from, date_to)
    cols = ("date", "type", "signal", "start_time", "end_time")
    stmt = _page(stmt.order_by(models.WeatherWarning.date, models.WeatherWarning.id), limit, offset)
    return [{c: getattr(w, c) for c in cols} for w in session.scalars(stmt)]


def warnings_count(
    session: Session, date_from: str | None = None, date_to: str | None = None
) -> int:
    """Count HKO warnings in the date window, ignoring any page window."""
    return _count_dated(session, models.WeatherWarning, date_from, date_to)
