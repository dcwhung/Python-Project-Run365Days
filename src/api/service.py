"""Database queries returning plain dicts in the export record shape.

The GraphQL layer maps these dicts onto Strawberry types; keeping the
service free of Strawberry makes it trivial to test against a temporary
database built from :class:`~run365days.export.records.ExportRecords`.
"""

from collections.abc import Iterable

from sqlalchemy import ColumnElement, Select, and_, func, or_, select
from sqlalchemy.orm import Session, aliased, selectinload

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

    Static mode thins the same tracks in the browser, so ``downsample`` in
    ``frontend/src/lib/downsample.ts`` is held to this arithmetic too --
    including the rounding. It rounds half to even by hand rather than calling
    ``Math.round``, which rounds half up and so would land one index away on
    any step that falls exactly on .5 (CUI-0021).

    Args:
        total: Rows stored for the track.
        points: Samples wanted, assumed to be below *total*.

    Returns:
        Strictly increasing positions. From ``points >= 2`` both the first and
        the last row are included. ``points == 1`` cannot hold both, and
        returns the last row alone rather than the first -- deliberately the
        same row :func:`~run365days.dashboard.builder.downsample` picks for
        ``limit < 2``, since the two samplers back the same field in the two
        deployment modes and have to agree on every input, this edge included.
    """
    if points < 2:
        return [total - 1]
    step = (total - 1) / (points - 1)
    return [round(i * step) for i in range(points)]


def _stored_counts(session: Session, activity_ids: list[str]) -> dict[str, int]:
    """Return ``{activity_id: rows stored}`` for the whole batch in one statement.

    Grouped rather than one COUNT per track, and read from the track table
    rather than from ``Activity.num_points``: that column counts the raw
    samples the source file held *before* the export downsampled them, so for
    a long run it reads higher than the rows actually stored. Sizing the
    sampler with it would put positions past the end of the track and thin the
    wrong rows -- silently, since a ``seq`` that matches nothing simply
    returns nothing.

    Args:
        session: Open read-only session.
        activity_ids: Tracks wanted, with no duplicates.

    Returns:
        A count per activity that stores at least one row; activities with no
        track are absent, not zero.
    """
    rows = session.execute(
        select(models.TrackPoint.activity_id, func.count())
        .where(models.TrackPoint.activity_id.in_(activity_ids))
        .group_by(models.TrackPoint.activity_id)
    )
    return dict(rows.all())


def _sample_filter(numbered, activity_ids: list[str], totals: dict[str, int], points: int):
    """Return the predicate keeping *points* even samples of every track in the batch.

    Each track gets its own positions, computed by :func:`_even_positions` in
    Python. That is the whole reason the counts are fetched first: the batch
    could have been one window-function statement with the arithmetic in SQL,
    but then SQLite's rounding would have to agree with Python's
    round-half-to-even on every track length, and ``downsample`` is the
    reference the samples are held against.

    Args:
        numbered: Subquery whose rows carry ``activity_id`` and ``position``.
        activity_ids: Tracks wanted, with no duplicates.
        totals: Rows stored per activity, as :func:`_stored_counts` returns.
        points: Samples wanted per track.

    Returns:
        A predicate over *numbered* selecting the wanted rows.
    """
    whole: list[str] = []
    sampled: list[ColumnElement] = []
    for activity_id in activity_ids:
        total = totals.get(activity_id, 0)
        if total <= points:
            # Nothing to thin: a track at or under the ask comes back entire,
            # which is also the trackless case and so is never an empty batch.
            whole.append(activity_id)
        else:
            sampled.append(
                and_(
                    numbered.c.activity_id == activity_id,
                    numbered.c.position.in_(_even_positions(total, points)),
                )
            )
    if whole:
        sampled.append(numbered.c.activity_id.in_(whole))
    return or_(*sampled)


def tracks(
    session: Session, activity_ids: Iterable[str], points: int | None = None
) -> dict[str, list[dict]]:
    """Return the stored track of every activity in *activity_ids*, in two statements.

    The cost is flat in the size of the batch: one grouped COUNT, then one
    SELECT over a ``row_number() OVER (PARTITION BY activity_id ORDER BY seq)``
    subquery. Before AU-050 every track paid those two statements on its own,
    so the count grew with the page;
    ``test_a_page_of_tracks_costs_the_same_statements_however_wide_it_is`` is
    what now holds it flat, and ``SQL_PER_TRACK_BATCH`` is the figure.

    What batching trades away is bound parameters: the position IN lists that
    used to be spread over 2N statements now arrive in one, and a statement
    over SQLite's ``SQLITE_MAX_VARIABLE_NUMBER`` raises rather than slows. The
    caller is what bounds them. This function will happily read a thousand
    tracks in one go; :mod:`run365days.api.schema` never asks for more than its
    per-request track budgets still cover, which is what keeps the widest legal
    batch inside the ceiling --
    ``test_the_widest_batch_stays_under_sqlites_bound_parameter_ceiling``.

    Args:
        session: Open read-only session.
        activity_ids: Tracks wanted. Duplicates are collapsed.
        points: Samples per track, or ``None``/``0`` for every stored row.

    Returns:
        ``{activity_id: rows}`` in :data:`TRACK_COLUMNS` shape, ordered by
        ``seq``, with an entry for every id asked for -- an empty list for an
        activity that stores no track.
    """
    wanted = list(dict.fromkeys(str(a) for a in activity_ids))
    found: dict[str, list[dict]] = {activity_id: [] for activity_id in wanted}
    if not wanted:
        return found
    if points:
        totals = _stored_counts(session, wanted)
        # Positions, not seq arithmetic: row_number stays evenly spaced even if
        # a track is ever stored with gaps in seq.
        position = (
            func.row_number().over(
                partition_by=models.TrackPoint.activity_id, order_by=models.TrackPoint.seq
            )
            - 1
        ).label("position")
        numbered = (
            select(models.TrackPoint, position)
            .where(models.TrackPoint.activity_id.in_(wanted))
            .subquery()
        )
        point = aliased(models.TrackPoint, numbered)
        stmt = select(point).where(_sample_filter(numbered, wanted, totals, points))
        order = (numbered.c.activity_id, numbered.c.seq)
    else:
        stmt = select(models.TrackPoint).where(models.TrackPoint.activity_id.in_(wanted))
        order = (models.TrackPoint.activity_id, models.TrackPoint.seq)
    for row in session.scalars(stmt.order_by(*order)):
        found[row.activity_id].append({col: getattr(row, col) for col in TRACK_COLUMNS})
    return found


def track(session: Session, activity_id: str, points: int | None = None) -> list[dict]:
    """Return the stored track for an activity, optionally downsampled to *points*.

    A batch of one, so the single-activity path and the fan-out path cannot
    drift apart in what they sample -- there is only one sampler.

    The thinning happens in SQL and once only: an earlier version narrowed the
    rows with a floor stride and then downsampled the survivors again, and the
    two passes compounded into gaps that differed by a factor of two.
    """
    return tracks(session, [activity_id], points)[str(activity_id)]


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
