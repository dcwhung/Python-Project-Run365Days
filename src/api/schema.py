"""Strawberry GraphQL schema.

Types mirror the export record shape (snake_case in Python, camelCase in
GraphQL via Strawberry's auto conversion). Resolvers pull a SQLAlchemy
session from ``info.context["session"]`` and delegate to
:mod:`run365days.api.service` and :mod:`run365days.dashboard.stats`.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, MutableMapping
from datetime import date as date_type

import strawberry
from strawberry.extensions import (
    DisableIntrospection,
    MaxTokensLimiter,
    QueryDepthLimiter,
    SchemaExtension,
)
from strawberry.types import Info

from run365days.api import service
from run365days.dashboard import stats
from run365days.export.records import TRACK_COLUMNS

DEFAULT_TRACK_POINTS = 150
"""Track points returned per activity unless the query asks for more."""

MAX_TRACK_POINTS = 1000
"""Ceiling for ``track(points:)``.

Deliberately above 600, the most track rows the export stores for one run
(``run365-export --points``, whose default is ``DEFAULT_POINT_LIMIT``), so a
client asking for the maximum always gets the whole stored track back.

Not to be read as a bound on ``Activity.num_points``: that field counts the
raw samples the source file held *before* the export downsampled them, so it
runs past 600 for the occasional long run and the two numbers do diverge.
"""

MAX_TRACK_POINTS_PER_REQUEST = 10000
"""Track points one operation may ask for, summed over every ``track`` field in it.

``MAX_TRACK_POINTS`` bounds a single track; nothing bounded the product of
``activities x points``, so one legal document could ask for 365 x 1000 =
365,000 points. Measured at ~27,000 rows/s that is 8.3 s of row
materialisation -- most of a 15 s Vercel function, and growing with the export.

10,000 is a round number chosen as a ceiling, *not* a figure derived from what
any client needs. The largest track a real client asks for is 600 points on a
single activity: ``TrackQuery`` is the only document in the front end that
selects ``track`` (``frontend/src/data/api/queries.ts``), it takes one
activity, and both call sites pass 600 (``TRACK_POINTS`` in
``frontend/src/views/activity/ActivityView.tsx``, ``DEFAULT_TRACK_POINTS`` in
``frontend/src/data/api/source.ts``). ``YearQuery``'s ``personalBests`` spend
nothing at all: each of its five fields only spreads the ``ActivityFields``
fragment, which carries no ``track``. So this sits roughly 16x above real
demand, and exists to keep the worst case finite rather than to fit a caller.

It bounds rows materialised, and nothing else. Round trips are bounded
separately by :data:`MAX_TRACK_FIELDS_PER_REQUEST`, which is where the cost of
a wide fan-out actually lands.
"""

MAX_TRACK_FIELDS_PER_REQUEST = 64
"""``track`` fields one operation may resolve, however few points each asks for.

Every ``track`` field costs a fixed round trip, and that cost does not move
with ``points``. Since AU-050 a field either joins a batch a sibling already
opened, for nothing, or opens one itself -- a grouped COUNT and a SELECT, see
:func:`run365days.api.service.tracks` -- and the worst case is still one batch
per field. So the points budget cannot bound it:
``track(points: 1)`` buys the most expensive thing in the request (two
statements) for the cheapest charge the budget can levy (one point). That is
AU-047 C-001, found as 27 aliases of ``activities(limit: 365)`` each taking
``track(points: 1)``: 9,855 points, inside every other limit, 19,764
statements and 13 s.

64 is about where the points budget already put the ceiling at the default
``points`` of :data:`DEFAULT_TRACK_POINTS`: ``10000 // 150`` is 66, and 64 is
that figure rounded down to the power of two below it. Stating the cap
outright costs legal traffic close to nothing -- the flood worked only by
pushing ``points`` down to 1 to slip out from under that implicit ceiling.

What this bounds is ``track`` statements, and only those: at most 64 batches
of 2 = 128 of them, reached only when no two fields share a batch, however the
fields are spread over the document. It is not a bound on the statements a
request issues, because every parent field carrying a ``track`` costs about two
statements of its own first -- a list plus its warnings for ``activities``, a
get plus its warnings for ``activity(id:)`` -- and it pays them whether the
``track`` under it is served or refused. That half of the cost is bounded by
:data:`MAX_QUERY_TOKENS` alone, and
``test_a_flood_of_aliased_parents_issues_more_statements_than_the_field_cap_bounds``
holds the two apart: the widest flood of parents the token limit admits issues
208 statements where this cap on its own would account for 128.

Measured through the Flask client against the ``year_db`` fixture in
``tests/test_api.py``: 365 activities, of which one carries a full 600-point
track -- 600 track rows in the database, not 365 x 600. Statement counts do
not depend on that; the wall-clock readings below do, and are one machine's
reading rather than a bound. The real export holds 134,041 track rows, 223x
what this fixture holds, and since AU-050 the batch read is the one path here
whose cost follows that total -- so a fixture reading of a *wide* batch does
not carry to production, while a reading of anything else roughly does. QA
measured both scales on 2026-09-15 (CUI-0020) and every figure below names the
scale it came from; none of them is asserted, so none of them goes red on
its own. They are quoted to one significant figure on purpose: an earlier
revision wrote two of them as two-figure intervals (``0.07-0.08``,
``0.17-0.21``) and both were narrower than the same machine produced on a
re-run, which reads as a measured bound when it is not one (S-033). If a
figure here ever needs to be tight enough to matter, it needs an assertion,
and a wall time in CI buys a flaky test rather than a guarantee:

* The shortest way to saturate this cap is one list field -- not the only
  way. ``activities(limit: 64) { track(points: 1) }`` is 19 tokens and issues
  4 statements (2 for the list, 2 for the one batch its 64 tracks share) in
  ~0.01 s on the fixture but ~0.2 s on the export (10.4 ms and 165.6 ms) --
  the 16x above, and the only reading here that moves with the export's size
  at all, since this is the one shape whose batch is wide. ``limit: 65``
  issues the same 4 before refusing the 65th -- the cap holds on a refused
  request, which is the point of charging before the SQL -- in ~0.01 s on the
  fixture, unmeasured on the export and bounded there by the served case
  beside it. Aliasing 64 ``track`` fields of one activity under a single
  parent reaches the same 4 for 714 tokens in ~0.02 s at either scale
  (20.8 ms fixture, 21.7 ms export: that batch reads one activity's track, so
  what it scans is that track's rows and not the export's), and its 65th field
  is refused by this cap too: that route is longer in tokens, not out of
  reach.
* Spending the document on *parents* instead reaches further. Of the two
  shapes that carry one track each, ``activity(id:)`` and
  ``activities(limit: 1)``, the token limit admits 52 -- 990 of
  :data:`MAX_QUERY_TOKENS`, where 53 lexes to 1009 and no longer parses. Both
  are legal and fully served. What they cost turns on whether their tracks can
  share a batch: 52 parents naming *one* activity issue 106 statements
  (52 x 2 + 2) in ~0.07 s on the fixture and ~0.08 s on the export (68.7 ms
  and 78.2 ms), while 52 parents naming 52 *different* activities cannot
  share and issue 208 (52 x 2 + 52 x 2) in ~0.1 s on the fixture and ~0.2 s
  on the export (141.4 ms and 194.8 ms) -- more than the cap alone would
  suggest. Neither moved by as much as 1.4x between the two scales, because
  every batch these open is a single-track batch and scans one track whatever
  else the export holds. The Vercel headroom is therefore read off the export
  rather than off any fixture figure: the worst legal shape QA measured there
  on 2026-09-15 came to 390 ms, some 38x inside the 15 s function, so the
  headroom is tens of times over and not the sixty an earlier revision
  inferred from fixture readings. AU-050 flattened the fan-out under one
  parent; it does not flatten a document that spends itself on parents.

A document that takes no ``track`` at all spends neither budget and is not
bounded here at all: 166 aliased ``activities`` fields fit the token limit at
998 tokens and are served, issuing 332 statements in ~2 s on the fixture. That
is the one reading here with no export figure beside it: QA has not measured
this shape at production scale. What it would cost there follows the activity
and warning rows a list field returns rather than the track rows the export is
large in, and the export holds the same 365 activities this fixture does, so
the gap is expected to be small -- expected, not measured. That cost belongs to
the list fan-out, and wants its own answer; it is not what this cap is for.

Every token and statement count above is asserted in ``tests/test_api.py``: the
served worst cases by
``test_the_documented_worst_cases_still_measure_as_documented``, and the alias
route and the 52/53 boundary by the tests beside it. A change to either limit
turns those red rather than leaving this prose quietly wrong. The wall-clock
figures are the exception: they are one machine's reading, not a bound,
because asserting a wall time in CI buys a flaky test rather than a guarantee.

AU-050 has since made a page of tracks cost two statements however wide it is.
An earlier revision of this docstring read that as weakening the round-trip
argument for keeping this number where it is. Measurement says the opposite.
The two statements are bought with a predicate carrying one OR arm per track
in the batch (:func:`run365days.api.service._sample_filter`), and the whole
disjunction is evaluated against every row the numbered subquery scans, so the
work grows with the *square* of the batch width where the pre-AU-050 shape --
one index-bounded statement per track -- grew with the width. QA measured it
on 2026-09-15 at the service layer against the real export (134,041 track
rows, ``points: 1``, median of 3), as a multiple of what the same read cost
before AU-050: 16 tracks 0.9x, 32 tracks 1.5x, 64 -- this cap -- 2.4x, 128
3.9x, 365 8.9x, the last being 3.8 s. Statement count fell, wall clock rose,
and how far it rose is a function of how wide this cap lets a batch get.

So raising this number is the direction this implementation is worst in, not
one batching has made safer. Those figures are a reading rather than a bound
and nothing asserts them; what is asserted is the shape they come from, by
``test_the_batch_predicate_costs_more_per_track_as_the_batch_widens``, which
holds the per-track cost of a 64-track batch above that of an 8-track one and
would go red if the predicate ever stopped being the thing that grows. The
superlinearity itself is CUI-0019 and belongs to
:func:`run365days.api.service.tracks`, not to this cap.

What survives from before: the worst case above is unshared batches, still two
statements per field; raising this number stays a separate decision with its
own measurement, since it is the points budget and :data:`MAX_QUERY_TOKENS`
that would then be doing the bounding; and a cap at or above
:data:`MAX_PAGE_SIZE` breaks the fan-out the tests reach it with.
"""

TRACK_BUDGET_KEY = "track_points_remaining"
"""``info.context`` key holding what is left of this request's track budget."""

TRACK_FIELDS_KEY = "track_fields_remaining"
"""``info.context`` key holding how many ``track`` fields this request may still resolve."""

TRACK_ROWS_KEY = "track_rows_read"
"""``info.context`` key holding the track rows this request has already read.

Keyed by ``(activity_id, points)`` -- both halves, because two fields asking
different ``points`` of one activity are two different samples of it.

Seeded lazily by :func:`_track_rows` rather than by :class:`_TrackBudget`,
since a request that reads no ``track`` should carry no cache. Its size is
bounded by the same budgets that bound the reads which fill it.
"""

DEFAULT_PAGE_SIZE = 500
"""Rows a list field returns when the client asks for no window.

Deliberately above the 365 rows a full year holds, so clients that read a
whole year in one request (the dashboard does) need no paging.
"""

MAX_PAGE_SIZE = 1000
"""Ceiling for ``limit``: the largest single-request page the API will serve."""

MAX_QUERY_DEPTH = 5
"""Deepest operation the API accepts.

The deepest document the dashboard sends is ``YearQuery`` at depth 4
(year -> personalBests -> longest -> weather -> leaf), which is also the
deepest the acyclic type graph allows today. So 5 is one level of headroom and
nothing else: **this limiter cannot fire against the schema as it stands**, and
that is the choice, not an oversight (CUI-0003). A document deep enough to
refuse cannot be written, so nothing a client sends today is stopped here.

What actually answers the abuse this was reached for is
:data:`MAX_QUERY_TOKENS`. An alias flood is wide rather than deep -- it repeats
a depth-2 field hundreds of times -- so the depth limiter would let every one
of them through whatever it were set to, and the token limiter is what refuses
them. Read "we have a depth limit" as covering that and the cover is imaginary.

Keeping the headroom rather than tightening to 4 costs nothing while the graph
stays this shape, and means a new nested field is a schema change rather than
also a limit change. The trade is that the limit then stops being unreachable
without anyone noticing, so it is a test that notices:
``test_the_type_graph_stays_one_level_below_the_depth_limit`` walks the type
graph and goes red the moment a field makes it deeper, and
``test_the_depth_limiter_refuses_one_level_below_the_deepest_document`` holds
the limiter either side of that edge, since ``build_schema`` reports full
coverage whether the extension is wired up or not.
"""

MAX_QUERY_TOKENS = 1000
"""Largest document the parser accepts.

The biggest document the dashboard sends lexes to 113 tokens and
introspection to 163, so this only ever stops alias-flooded documents.
"""

GRAPHIQL_ENV = "RUN365_GRAPHIQL"
"""Environment variable that opens the endpoint up for local development.

One variable, not two: a browser IDE is useless without introspection, so the
flag that serves GraphiQL is the same flag that lets ``__schema`` through.
"""

GRAPHIQL_ON = frozenset({"1", "true", "yes", "on"})
"""Values that count as on. An allowlist, so ``RUN365_GRAPHIQL=0`` stays off."""


def graphiql_enabled() -> bool:
    """Report whether the environment asks for the IDE and its introspection."""
    return os.environ.get(GRAPHIQL_ENV, "").strip().lower() in GRAPHIQL_ON


def _introspection_gate() -> SchemaExtension:
    """Reject ``__schema`` / ``__type`` documents unless the dev flag is on.

    Returns:
        A per-request extension that either blocks introspection or does
        nothing, according to :func:`graphiql_enabled`.
    """
    # The flag is read here, per request, rather than once at import: the
    # module-level ``schema`` below is a singleton, so a start-up read would
    # freeze whatever the environment happened to hold for the first importer.
    return SchemaExtension() if graphiql_enabled() else DisableIntrospection()


class _TrackBudget(SchemaExtension):
    """Give every operation its own track budgets, in points and in fields.

    "Per operation" and "per request" are the same thing here only because
    Strawberry's operation batching is off (``schema.config.batching_config``
    is ``None``), so one HTTP request carries exactly one operation. Switching
    batching on would let a client spend both budgets once per operation in
    the batch, which is a decision to weigh against these limits, not a free
    transport setting.
    """

    def on_operation(self) -> Iterator[None]:
        """Seed both budgets before any resolver runs, then let the operation go."""
        # Seeded here rather than in the transport's ``get_context`` so that
        # the bound belongs to the schema: every caller gets it, including
        # ``schema.execute_sync(..., context_value=...)`` in a test.
        context = self.execution_context.context
        if isinstance(context, MutableMapping):
            context[TRACK_BUDGET_KEY] = MAX_TRACK_POINTS_PER_REQUEST
            context[TRACK_FIELDS_KEY] = MAX_TRACK_FIELDS_PER_REQUEST
        # A context that is not a mutable mapping is left as it is, and that is
        # safe for a less obvious reason than "track is then unreachable". A
        # read-only ``Mapping`` -- ``MappingProxyType``, say -- is not a
        # ``MutableMapping``, so nothing is seeded here, yet
        # ``info.context["session"]`` still reads and ``track`` is reachable.
        # What actually holds the line is ``_charge_track_field``, which reads
        # a missing budget as a refusal rather than as permission.
        yield


def _charge_track_field(info: Info, points: int) -> int:
    """Deduct one ``track`` field, and its points, from this request's budgets.

    Two counters because they bound two different costs. Points bound the rows
    an operation materialises; fields bound the round trips it makes, which
    points cannot see -- a ``track`` field issues the same two statements
    whether it asks for 1 point or 1000.

    Points are charged on what the client asked for, not on the rows the track
    turns out to hold. That is the whole point: the request has to be refused
    *before* the SQL goes out, and the row count is only known after. The
    trade-off is that an honest client asking 1000 points of a 4-point track
    still pays 1000, which keeps the bound conservative rather than exact.

    Both charges land before the resolver touches its session, so a field that
    overruns either budget is refused having issued no SQL at all. Neither
    counter is written back on a refusal -- the raises below both come before
    the two assignments -- and that single fact reads differently for the two
    budgets.

    The field budget is sticky. Nothing is stored below 0: the count stops
    there and stays, and it is the arithmetic that latches, not the counter,
    since every later ``track`` field still costs 1, computes -1 locally, and
    is refused in turn. Exhausting it therefore refuses the whole rest of the
    operation, which ``test_the_track_field_cap_is_the_boundary`` pins.

    The points budget is not sticky: it keeps whatever was left and refuses
    each later field on that field's own cost. Measured: after a field asking
    1000 points is refused with 500 left, a following ``track(points: 500)``
    is still served. Leaving the remainder intact is deliberate; charging a
    refused field would make an over-large field poison the cheap ones behind
    it.

    Args:
        info: Resolver info carrying this request's context.
        points: Already bounds-checked sample count for one track field.

    Returns:
        ``points``, so the caller can spend and pass it in one expression.

    Raises:
        ValueError: If this operation has already spent either budget.
        RuntimeError: If the budgets were never seeded. Two ways in: the schema
            was built without :class:`_TrackBudget`, or the context is a
            mapping that extension could not write to (a read-only ``Mapping``
            is not a ``MutableMapping``). Falling back to an unbounded request
            would defeat both limits, so it fails loudly instead.
    """
    try:
        fields_left = info.context[TRACK_FIELDS_KEY] - 1
        points_left = info.context[TRACK_BUDGET_KEY] - points
    except KeyError as exc:
        # Deliberately not surfacing the key that was missing: it is an
        # internal detail of the extension, not something a client can act on.
        raise RuntimeError(
            "track budget was not seeded for this request, so `track` cannot be served"
        ) from exc
    if fields_left < 0:
        raise ValueError(
            "track field budget exhausted: one request may read at most "
            f"{MAX_TRACK_FIELDS_PER_REQUEST} tracks"
        )
    if points_left < 0:
        raise ValueError(
            "track points budget exhausted: one request may return at most "
            f"{MAX_TRACK_POINTS_PER_REQUEST} track points"
        )
    info.context[TRACK_FIELDS_KEY] = fields_left
    info.context[TRACK_BUDGET_KEY] = points_left
    return points


def _batch_ids(
    context: MutableMapping, activity_id: str, page: tuple[str, ...], points: int, read: dict
) -> list[str]:
    """Return the ids to read together: this activity, plus the siblings still afforded.

    A batch is a prefetch. It reads tracks for fields that have not been
    charged yet -- they are charged as they resolve, afterwards -- so reading
    the whole page would materialise exactly the rows the budgets exist to
    refuse, and undo AU-047's "refuse before the SQL goes out". Instead the
    batch stops where the budgets do: what is left of them after this field's
    own charge is what it may read ahead into, so the rows a batch fetches
    never exceed the rows the budgets were going to allow anyway.

    Siblings already in *read* are skipped rather than counted, so a second
    pass over the same page widens the batch instead of re-reading rows.

    Args:
        context: This request's context, read after this field has been charged.
        activity_id: The activity whose ``track`` field is resolving.
        page: Ids the parent list field put on the page, ``activity_id`` among
            them. Empty for an activity that arrived on its own.
        points: Samples per track, at least 1.
        read: The request's track cache, keyed by ``(activity_id, points)``.

    Returns:
        ``activity_id`` first, then the affordable siblings in page order.
    """
    spare = min(context[TRACK_FIELDS_KEY], context[TRACK_BUDGET_KEY] // points)
    ids = [activity_id]
    for sibling in page:
        if len(ids) > spare:
            break
        if sibling != activity_id and (sibling, points) not in read:
            ids.append(sibling)
    return ids


def _track_rows(info: Info, activity_id: str, page: tuple[str, ...], points: int) -> list[dict]:
    """Return one activity's sampled track, reading its whole page in one batch.

    Args:
        info: Resolver info, whose context carries the session and the budgets.
        activity_id: The activity whose ``track`` field is resolving.
        page: Ids the parent list field put on the page.
        points: Already charged sample count.

    Returns:
        Rows in :data:`run365days.export.records.TRACK_COLUMNS` shape.
    """
    # Reached only after _charge_track_field has read both budget keys, so the
    # context is a mapping that was seeded and can be written to.
    read = info.context.setdefault(TRACK_ROWS_KEY, {})
    key = (activity_id, points)
    if key not in read:
        ids = _batch_ids(info.context, activity_id, page, points, read)
        for batched_id, rows in service.tracks(info.context["session"], ids, points).items():
            read[(batched_id, points)] = rows
    return read[key]


TRACK_DESCRIPTION = (
    "GPS track, evenly downsampled to at most `points` samples. "
    f"One request may read at most {MAX_TRACK_FIELDS_PER_REQUEST} `track` fields, "
    f"totalling {MAX_TRACK_POINTS_PER_REQUEST} points. Both are counted across every "
    "`track` field in the request; points are charged on `points` as asked for, not "
    "on the rows a track turns out to hold. The field limit applies however small "
    "`points` is, because each `track` field costs the same query either way."
)
"""Description for ``Activity.track``.

Both budgets are part of the field's contract, so a client reading the SDL can
see why a wide fan-out is refused without having to trigger the error first.
"""


COUNT_DESCRIPTION = (
    "Rows matching the same filters, ignoring `limit` and `offset`. "
    "A list shorter than this count was cut off by the page window."
)
"""Description shared by the four count fields.

A bare list gives a client no way to tell "these are all the rows" from
"these are the first `limit` rows"; comparing its length against the count
is that signal.
"""


def _iso(d: date_type | None) -> str | None:
    return d.isoformat() if d else None


def _page(limit: int, offset: int) -> tuple[int, int]:
    """Return the requested page window after bounds-checking it.

    Args:
        limit: Rows the client asked for.
        offset: Rows to skip.

    Returns:
        The validated ``(limit, offset)`` pair.

    Raises:
        ValueError: If the window is empty, negative, or over MAX_PAGE_SIZE.
    """
    if not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}, got {limit}")
    if offset < 0:
        raise ValueError(f"offset must not be negative, got {offset}")
    return limit, offset


def _track_points(points: int) -> int:
    """Return the requested track sample count after bounds-checking it.

    Args:
        points: Samples the client asked for.

    Returns:
        The validated sample count.

    Raises:
        ValueError: If the count is below 1 or over MAX_TRACK_POINTS.
    """
    if not 1 <= points <= MAX_TRACK_POINTS:
        raise ValueError(f"points must be between 1 and {MAX_TRACK_POINTS}, got {points}")
    return points


# ── leaf types ─────────────────────────────────────────────────────────────
@strawberry.type(description="Hourly weather observation closest to the run's start time.")
class RunWeather:
    description: str | None
    temp_c: float | None
    humidity_pct: float | None
    wind_kmh: float | None


@strawberry.type(description="One stored sample of a run's track.")
class TrackPoint:
    sec: int
    lat: float | None
    lon: float | None
    elevation_m: float | None
    distance_m: float | None
    speed_mps: float | None
    cadence: int | None
    temp_c: float | None


@strawberry.type(description="One run parsed from the Garmin TCX export.")
class Activity:
    id: strawberry.ID
    date: str
    start_time: str
    day_of_year: int
    distance_km: float
    duration_sec: int
    pace_sec_per_km: int | None
    calories: int | None
    avg_cadence: float | None
    avg_temp_c: float | None
    elevation_min_m: float | None
    elevation_max_m: float | None
    ascent_m: float | None
    has_gps: bool
    num_points: int
    weather: RunWeather | None
    warnings: list[str]
    page: strawberry.Private[tuple[str, ...]] = ()
    """Ids this activity shares a list field with, so ``track`` can read them together.

    ``Private``, so it stays out of the SDL: it is how the resolver found the
    activity, not a fact about the run. Empty for an activity that arrived on
    its own, which makes that a batch of one rather than a separate path.
    """

    @strawberry.field(description=TRACK_DESCRIPTION)
    def track(self, info: Info, points: int = DEFAULT_TRACK_POINTS) -> list[TrackPoint]:
        # Spend first: the budgets exist to stop the query being issued at all.
        # The batch below reads no further than what is left of them, so
        # charging before reading still means refusing before the SQL goes out.
        wanted = _charge_track_field(info, _track_points(points))
        rows = _track_rows(info, str(self.id), self.page, wanted)
        return [TrackPoint(**row) for row in rows]


@strawberry.type
class WeightEntry:
    date: str
    weight_lbs: float
    weight_kg: float
    bmi: float


@strawberry.type(description="One day of the Hong Kong Observatory daily extract.")
class DailyWeather:
    date: str
    max_temp_c: float | None
    avg_temp_c: float | None
    min_temp_c: float | None
    humidity_pct: float | None
    rainfall_mm: float | None
    wind_kmh: float | None
    sunrise: str | None
    sunset: str | None


@strawberry.type(description="One HKO warning or tropical cyclone signal.")
class WeatherWarning:
    date: str
    type: str | None
    signal: str
    start_time: str | None
    end_time: str | None


# ── aggregates ─────────────────────────────────────────────────────────────
@strawberry.type
class Totals:
    runs: int
    days: int
    active_days: int
    distance_km: float
    duration_sec: int
    calories: int
    avg_pace_sec_per_km: int | None
    avg_cadence: float | None
    avg_distance_km: float


@strawberry.type
class MonthSummary:
    month: int
    runs: int
    distance_km: float
    duration_sec: int
    calories: int
    avg_pace_sec_per_km: int | None
    avg_cadence: float | None
    best_pace_sec_per_km: int | None
    best_pace_activity_id: strawberry.ID | None


@strawberry.type
class WeekSummary:
    week: int
    week_start: str
    runs: int
    distance_km: float
    duration_sec: int
    avg_pace_sec_per_km: int | None
    longest_km: float
    activity_ids: list[strawberry.ID]


@strawberry.type
class DayDistance:
    date: str
    distance_km: float
    activity_id: strawberry.ID | None


@strawberry.type(
    description="Exponentially weighted load: fitness (CTL), fatigue (ATL), form (TSB)."
)
class TrainingLoadPoint:
    date: str
    ctl: float
    atl: float
    tsb: float


@strawberry.type
class PersonalBests:
    longest: Activity | None
    fastest: Activity | None
    longest_time: Activity | None
    most_calories: Activity | None
    top_cadence: Activity | None


@strawberry.type(description="Everything the overview and analytics views need for one year.")
class YearSummary:
    year: int
    totals: Totals
    monthly: list[MonthSummary]
    weekly: list[WeekSummary]
    daily_distance: list[DayDistance]
    training_load: list[TrainingLoadPoint]
    personal_bests: PersonalBests


@strawberry.type
class Meta:
    year: int
    generated_at: str
    track_columns: list[str]


# ── helpers ────────────────────────────────────────────────────────────────
def _activity(rec: dict | None) -> Activity | None:
    if rec is None:
        return None
    weather = rec.get("weather")
    return Activity(
        **{k: v for k, v in rec.items() if k not in ("weather", "warnings")},
        weather=RunWeather(**weather) if weather else None,
        warnings=list(rec.get("warnings", [])),
    )


def _page_of(recs: list[dict]) -> list[Activity]:
    """Build a list field's activities, each knowing the page it came on.

    One shared tuple rather than a copy per row: it is the same page, and
    ``track`` reads it to batch its siblings' tracks into one statement.
    """
    activities = [_activity(rec) for rec in recs]
    ids = tuple(str(a.id) for a in activities)
    for activity in activities:
        activity.page = ids
    return activities


# ── root ───────────────────────────────────────────────────────────────────
@strawberry.type
class Query:
    @strawberry.field(description="Export metadata.")
    def meta(self, info: Info) -> Meta:
        m = service.meta(info.context["session"])
        return Meta(
            year=m["year"], generated_at=m["generated_at"], track_columns=list(TRACK_COLUMNS)
        )

    @strawberry.field(description="Runs in start order, optionally filtered (dates inclusive).")
    def activities(
        self,
        info: Info,
        from_date: date_type | None = None,
        to_date: date_type | None = None,
        min_km: float | None = None,
        has_gps: bool | None = None,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> list[Activity]:
        limit, offset = _page(limit, offset)
        rows = service.activities(
            info.context["session"], _iso(from_date), _iso(to_date), min_km, has_gps, limit, offset
        )
        return _page_of(rows)

    @strawberry.field(description=COUNT_DESCRIPTION)
    def activities_count(
        self,
        info: Info,
        from_date: date_type | None = None,
        to_date: date_type | None = None,
        min_km: float | None = None,
        has_gps: bool | None = None,
    ) -> int:
        return service.activities_count(
            info.context["session"], _iso(from_date), _iso(to_date), min_km, has_gps
        )

    @strawberry.field(description="One run by Garmin activity id.")
    def activity(self, info: Info, id: strawberry.ID) -> Activity | None:
        return _activity(service.activity(info.context["session"], str(id)))

    @strawberry.field(description="Daily weigh-ins (dates inclusive).")
    def weight(
        self,
        info: Info,
        from_date: date_type | None = None,
        to_date: date_type | None = None,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> list[WeightEntry]:
        limit, offset = _page(limit, offset)
        rows = service.weight(
            info.context["session"], _iso(from_date), _iso(to_date), limit, offset
        )
        return [WeightEntry(**r) for r in rows]

    @strawberry.field(description=COUNT_DESCRIPTION)
    def weight_count(
        self, info: Info, from_date: date_type | None = None, to_date: date_type | None = None
    ) -> int:
        return service.weight_count(info.context["session"], _iso(from_date), _iso(to_date))

    @strawberry.field(description="HKO daily weather (dates inclusive).")
    def weather(
        self,
        info: Info,
        from_date: date_type | None = None,
        to_date: date_type | None = None,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> list[DailyWeather]:
        limit, offset = _page(limit, offset)
        rows = service.daily_weather(
            info.context["session"], _iso(from_date), _iso(to_date), limit, offset
        )
        return [DailyWeather(**r) for r in rows]

    @strawberry.field(description=COUNT_DESCRIPTION)
    def weather_count(
        self, info: Info, from_date: date_type | None = None, to_date: date_type | None = None
    ) -> int:
        return service.daily_weather_count(info.context["session"], _iso(from_date), _iso(to_date))

    @strawberry.field(description="HKO warnings and signals (dates inclusive).")
    def warnings(
        self,
        info: Info,
        from_date: date_type | None = None,
        to_date: date_type | None = None,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> list[WeatherWarning]:
        limit, offset = _page(limit, offset)
        rows = service.warnings(
            info.context["session"], _iso(from_date), _iso(to_date), limit, offset
        )
        return [WeatherWarning(**r) for r in rows]

    @strawberry.field(description=COUNT_DESCRIPTION)
    def warnings_count(
        self, info: Info, from_date: date_type | None = None, to_date: date_type | None = None
    ) -> int:
        return service.warnings_count(info.context["session"], _iso(from_date), _iso(to_date))

    @strawberry.field(description="Aggregates for the exported year (or a given year).")
    def year(self, info: Info, year: int | None = None) -> YearSummary:
        session = info.context["session"]
        y = year or service.meta(session)["year"]
        # No page window: these rows are folded into aggregates server-side, so
        # the response size is fixed by the calendar, not by what the client asks for.
        acts = service.activities(session, f"{y}-01-01", f"{y}-12-31")
        daily = stats.daily_distance(acts, y)
        pbs = stats.personal_bests(acts)
        return YearSummary(
            year=y,
            totals=Totals(**stats.totals(acts, y)),
            monthly=[MonthSummary(**m) for m in stats.monthly(acts)],
            weekly=[WeekSummary(**w) for w in stats.weekly(acts, y)],
            daily_distance=[DayDistance(**d) for d in daily],
            training_load=[TrainingLoadPoint(**t) for t in stats.training_load(daily)],
            personal_bests=PersonalBests(**{k: _activity(v) for k, v in pbs.items()}),
        )


def build_schema(
    max_depth: int = MAX_QUERY_DEPTH, max_tokens: int = MAX_QUERY_TOKENS
) -> strawberry.Schema:
    """Return the schema with its query-cost limits applied.

    Args:
        max_depth: Deepest nesting a single operation may reach.
        max_tokens: Most tokens the parser accepts in one document.

    Returns:
        A schema that rejects over-deep or over-large documents during
        parsing and validation, before any resolver opens a query.
    """
    return strawberry.Schema(
        query=Query,
        extensions=[
            # Factories, not instances: Strawberry builds a fresh extension per
            # request. A lambda is a factory; so is a class, which is why the
            # limiters are wrapped and the last two are passed bare.
            lambda: QueryDepthLimiter(max_depth=max_depth),
            lambda: MaxTokensLimiter(max_token_count=max_tokens),
            _introspection_gate,
            _TrackBudget,
        ],
    )


schema = build_schema()
