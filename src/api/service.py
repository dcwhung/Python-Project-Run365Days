"""Database queries returning plain dicts in the export record shape.

The GraphQL layer maps these dicts onto Strawberry types; keeping the
service free of Strawberry makes it trivial to test against a temporary
database built from :class:`~run365days.export.records.ExportRecords`.
"""

from collections.abc import Iterable

from sqlalchemy import ColumnElement, Select, String, Subquery, cast, func, or_, select
from sqlalchemy.orm import Session, aliased, selectinload

from run365days.export import models
from run365days.export.records import TRACK_COLUMNS

MAX_TRACK_POINTS = 1000
"""Ceiling for a track, both for what may be asked for and for what comes back.

Deliberately above 600, the most track rows the export stores for one run under
its default ``run365-export --points`` (``DEFAULT_POINT_LIMIT``), so a client
asking for the maximum always gets the whole stored track back.

Defined here rather than in :mod:`run365days.api.schema`, which re-exports it,
because it has to bound :func:`tracks` -- the layer below GraphQL -- and the
service may not import Strawberry. One definition, not two: the number is
already written down a second time in TypeScript for static mode, and
``frontend/schema.graphql`` is what holds those two equal (CUI-0034). A third
copy inside Python would be outside that gate entirely.

Not to be read as a bound on ``Activity.num_points``: that field counts the raw
samples the source file held *before* the export downsampled them, so it runs
past 600 for the occasional long run and the two numbers do diverge.
"""

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


SAMPLE_KEY_SEPARATOR = ":"
"""Separator joining a position to an activity id in the sample key.

What makes the key injective is that one of its two halves cannot contain this
character: a position is a run of decimal digits, so whichever end of the key
it sits at is unambiguous, and the whole of the other half is the activity id
however many separators that id itself holds. Nothing here ever parses a key
back apart -- the ``IN`` compares them whole -- so injectivity is the only
property needed.

Position first is therefore an arbitrary choice, kept only because it reads
well. An earlier revision of this docstring, of ``f57caa8``'s commit message
and of CUI-0019 all said instead that id-first would let two rows share a key,
and that is wrong: id-first is injective for exactly the same reason, off the
*last* separator rather than the first. Both orders were brute-forced over ids
holding separators, empty ids, leading zeros and non-ASCII, at zero collisions
either way. The order is not the load-bearing half of this; the one thing the
key does rest on is that a position never renders with this character in it
(:func:`_sample_key`). Let that change and both orders break together, and the
key stops matching silently rather than raising.

Two tests in ``tests/test_api.py`` hold that last sentence, and until CUI-0028
nothing did -- this constant could be set to a decimal digit, or dropped
entirely, with the whole suite still green.
``test_the_sample_key_separator_cannot_occur_in_a_position`` asserts the
property itself over the positions ``COLLIDING_TRACK_LENGTHS`` and
``VARIED_TRACK_LENGTHS`` render -- not every track length in the suite, which
is what an earlier revision of this sentence claimed. It does not need them:
the sweep is held to rendering all ten decimal digits, so a separator set to a
digit the narrower set never produced cannot slip through. That is what makes
it the test that says which way this constant may not be changed.
``test_a_batch_of_variable_length_ids_thins_each_track_independently`` proves
the consequence on the only fixture shape that can show it: activity ids that
are pure decimal and not all one width (``COLLIDING_IDS``). Every other
fixture, and today's production export, carries either a non-decimal prefix or
a fixed width, and either one makes the key injective on its own -- which is
why the suite stayed green for as long as it did.
"""


def _sample_key(numbered: Subquery) -> ColumnElement:
    """Return the expression naming a row of *numbered* as ``position:activity_id``.

    One value per row, so the whole batch can be selected by a single ``IN``
    against the keys :func:`_sample_filter` builds in Python, which is what
    keeps the predicate the same size at any batch width (CUI-0019).

    The cast is explicitness, not necessity, and an earlier revision of this
    docstring had both of its reasons wrong. ``numbered.c.position`` is not an
    integer column: it is a label over ``row_number() OVER (...) - 1``, which
    SQLAlchemy cannot type and leaves as ``NullType``. And ``concat`` does not
    render as arithmetic addition on an integer -- ``+`` does, but ``concat``
    is the explicit ``concat_op`` and renders ``||`` whatever the operand type,
    a real ``Integer`` column included. SQLite then coerces an integer to text
    across ``||`` by itself, so dropping the cast changes neither the value nor
    any test. What it changes is the type: without it the expression the ``IN``
    binds against is ``NullType`` rather than ``String``, and neither the SQL
    nor the SQLAlchemy expression says that this is text concatenation. The
    cast is what puts that in writing, which is why it stays.

    What the keys do rest on is that both sides render a position identically:
    Python's ``str(int)`` and SQLite's ``CAST(... AS VARCHAR)`` agree on every
    integer, which is why :func:`_even_positions` must keep returning ``int``.
    A float would build ``2.0`` on the Python side against ``2`` on the SQL
    side and match nothing -- thinning the wrong rows silently rather than
    raising.
    """
    return (
        cast(numbered.c.position, String)
        .concat(SAMPLE_KEY_SEPARATOR)
        .concat(numbered.c.activity_id)
    )


def _sample_filter(
    numbered: Subquery, activity_ids: list[str], totals: dict[str, int], points: int
) -> ColumnElement:
    """Return the predicate keeping *points* even samples of every track in the batch.

    Each track gets its own positions, computed by :func:`_even_positions` in
    Python. That is the whole reason the counts are fetched first: the batch
    could have been one window-function statement with the arithmetic in SQL,
    but then SQLite's rounding would have to agree with Python's
    round-half-to-even on every track length, and ``downsample`` is the
    reference the samples are held against.

    Those positions reach SQL as one ``IN`` over :func:`_sample_key` rather
    than as one ``activity_id = ? AND position IN (...)`` arm per track. The
    arms were what AU-050 shipped, and they made this predicate as wide as the
    batch: SQLite evaluates the whole disjunction against every row the
    numbered subquery scans, so the work grew with batch width times batch
    rows -- 8.9x the pre-AU-050 cost at 365 tracks, where it should have been
    flat (CUI-0019). A single ``IN`` over a list of constants is one ephemeral
    index and a lookup per row, so the predicate now costs the same whether
    the batch holds one track or all of them.

    Args:
        numbered: Subquery whose rows carry ``activity_id`` and ``position``.
        activity_ids: Tracks wanted, with no duplicates.
        totals: Rows stored per activity, as :func:`_stored_counts` returns.
        points: Samples wanted per track.

    Returns:
        A predicate over *numbered* selecting the wanted rows: at most two
        arms, and never zero for a non-empty batch.
    """
    whole: list[str] = []
    keys: list[str] = []
    for activity_id in activity_ids:
        total = totals.get(activity_id, 0)
        if total <= points:
            # Nothing to thin: a track at or under the ask comes back entire,
            # which is also the trackless case and so is never an empty batch.
            whole.append(activity_id)
        else:
            keys.extend(
                f"{position}{SAMPLE_KEY_SEPARATOR}{activity_id}"
                for position in _even_positions(total, points)
            )
    arms: list[ColumnElement] = []
    if keys:
        arms.append(_sample_key(numbered).in_(keys))
    if whole:
        arms.append(numbered.c.activity_id.in_(whole))
    return or_(*arms)


def tracks(
    session: Session, activity_ids: Iterable[str], points: int | None = None
) -> dict[str, list[dict]]:
    """Return the stored track of every activity in *activity_ids*, in two statements.

    The statement count is flat in the size of the batch: one grouped COUNT,
    then one SELECT over a
    ``row_number() OVER (PARTITION BY activity_id ORDER BY seq)`` subquery.
    Before AU-050 every track paid those two statements on its own, so the
    count grew with the page;
    ``test_a_page_of_tracks_costs_the_same_statements_however_wide_it_is`` is
    what now holds it flat, and ``SQL_PER_TRACK_BATCH`` is the figure.

    Two statements are not by themselves two statements' worth of work, and for
    a while here they were not: AU-050 bought the flat count with a predicate
    that carried an arm per track, so the SELECT's own cost grew with the width
    of the batch even though its count did not (CUI-0019). It is
    :func:`_sample_filter` that keeps the predicate a fixed size, and
    ``test_the_batch_predicate_does_not_widen_with_the_batch`` that holds it
    there, since a statement count alone cannot see that kind of regression.

    What batching trades away is bound parameters: the samples that used to be
    named across 2N statements are now named in one, and a statement over
    SQLite's ``SQLITE_MAX_VARIABLE_NUMBER`` raises rather than slows. The
    caller is what bounds them. This function will happily read a thousand
    tracks in one go; :mod:`run365days.api.schema` never asks for more than its
    per-request track budgets still cover, which is what keeps the widest legal
    batch inside the ceiling --
    ``test_the_widest_batch_stays_under_sqlites_bound_parameter_ceiling``.

    Args:
        session: Open read-only session.
        activity_ids: Tracks wanted. Duplicates are collapsed.
        points: Samples per track, or ``None``/``0`` for every stored row up to
            :data:`MAX_TRACK_POINTS`. A negative count is refused, not clamped;
            see Raises.

    Returns:
        ``{activity_id: rows}`` in :data:`TRACK_COLUMNS` shape, ordered by
        ``seq``, with an entry for every id asked for -- an empty list for an
        activity that stores no track, and never more than
        :data:`MAX_TRACK_POINTS` rows for one.

    Raises:
        ValueError: If *points* is negative.

    Refused rather than clamped, which is the half of CUI-0040 that was a
    choice rather than a bug. ``min(points, MAX_TRACK_POINTS)`` passed a
    negative straight through, and :func:`_even_positions` turns anything under
    2 into the last row alone: measured against a 1250-row track, ``-1``,
    ``-5`` and ``-1000`` each came back as exactly one row, which is also what
    ``points=1`` returns.

    Clamping is the smaller change and was the ticket's own first suggestion,
    but neither direction survives. Clamping *down* into the legal range lands
    on ``1`` -- which is precisely what a negative already returned above, so
    it would preserve the bug it was meant to fix while looking deliberate.
    That leaves clamping *upward*, to :data:`MAX_TRACK_POINTS`, because that is
    already what falsy means here.
    That hands the largest answer this function can give to the most obviously
    broken question, and it erases the distinction CUI-0025 was argued over:
    ``0`` means "I did not ask", while ``-5`` means "I asked for something that
    cannot exist". Those deserve different answers, and only one of them can be
    silent.

    The wording is deliberately not :func:`run365days.api.schema._track_points`'s
    sentence, near as it is. That one says "between 1 and MAX_TRACK_POINTS",
    which would be false here, where ``None`` and ``0`` are both legal. The two
    layers refuse different sets, so they say different things -- and no client
    ever reads this one: ``_track_points`` and ``checkPoints`` in
    ``frontend/src/data/static/source.ts`` both refuse a negative first, in the
    single sentence CUI-0025 bought for both deployment modes.
    """
    if points is not None and points < 0:
        raise ValueError(f"points must not be negative, got {points}")
    wanted = list(dict.fromkeys(str(a) for a in activity_ids))
    found: dict[str, list[dict]] = {activity_id: [] for activity_id in wanted}
    if not wanted:
        return found
    # CUI-0033 (a). Omitting `points` still means "the whole stored track", and
    # capping it does not change that for any track the export writes today:
    # the longest is 600 rows under the default --points and every one of them
    # comes back entire. What it changes is the claim MAX_TRACK_POINTS makes.
    # It used to bound only what a client could *ask* for, so the same module
    # refused `points: 1250` and then served 1250 rows to a caller that named
    # no number -- and `--points 1250` is all it takes to put such a track in
    # the database. The ceiling now bounds the output too, which is the reading
    # every caller of this module already had of it.
    #
    # There is no longer a branch that reads every stored row: `points` falsy
    # means MAX_TRACK_POINTS, not "unbounded", so the one-statement path it
    # used to take would be unreachable. The sampler returns a track at or
    # under the ask entire (see :func:`_sample_filter`), so what that branch
    # used to do is what this one does, one grouped COUNT more expensively.
    sample = min(points, MAX_TRACK_POINTS) if points else MAX_TRACK_POINTS
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
    stmt = select(point).where(_sample_filter(numbered, wanted, totals, sample))
    order = (numbered.c.activity_id, numbered.c.seq)
    for row in session.scalars(stmt.order_by(*order)):
        found[row.activity_id].append({col: getattr(row, col) for col in TRACK_COLUMNS})
    return found


def track(session: Session, activity_id: str, points: int | None = None) -> list[dict]:
    """Return the stored track for an activity, downsampled to *points*.

    Omitting *points* asks for the whole stored track, which
    :func:`tracks` bounds at :data:`MAX_TRACK_POINTS` rows like any other ask.

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
