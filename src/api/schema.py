"""Strawberry GraphQL schema.

Types mirror the export record shape (snake_case in Python, camelCase in
GraphQL via Strawberry's auto conversion). Resolvers pull a SQLAlchemy
session from ``info.context["session"]`` and delegate to
:mod:`run365days.api.service` and :mod:`run365days.dashboard.stats`.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping
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
materialisation -- most of a 15 s Vercel function, and growing with the
export. The cost is building the rows, not the per-activity round trips, so
batching the queries would not have bought the headroom back.

10,000 is twice the most expensive document the dashboard can send: the five
activities of ``YearQuery``'s ``personalBests``, each carrying a full
``MAX_TRACK_POINTS`` track, comes to 5,000. 10,000 rows costs about 0.37 s.
"""

TRACK_BUDGET_KEY = "track_points_remaining"
"""``info.context`` key holding what is left of this request's track budget."""

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
deepest the acyclic type graph allows today; 5 leaves one level of headroom
for a new nested field without reopening the limit question.
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
    """Give every operation its own :data:`MAX_TRACK_POINTS_PER_REQUEST`."""

    def on_operation(self):
        """Seed the budget before any resolver runs, then let the operation go."""
        # Seeded here rather than in the transport's ``get_context`` so that
        # the bound belongs to the schema: every caller gets it, including
        # ``schema.execute_sync(..., context_value=...)`` in a test.
        context = self.execution_context.context
        if isinstance(context, MutableMapping):
            context[TRACK_BUDGET_KEY] = MAX_TRACK_POINTS_PER_REQUEST
        # An operation run with no context mapping at all cannot reach
        # ``track`` anyway -- that resolver reads its session out of the same
        # mapping -- so there is nothing to seed and nothing left unbounded.
        yield


def _charge_track_points(info: Info, points: int) -> int:
    """Deduct one ``track`` field's cost from this request's budget.

    Charged on the points the client asked for, not on the rows the track
    turns out to hold. That is the whole point: the request has to be refused
    *before* the SQL goes out, and the row count is only known after. The
    trade-off is that an honest client asking 1000 points of a 4-point track
    still pays 1000, which keeps the bound conservative rather than exact.

    Args:
        info: Resolver info carrying this request's context.
        points: Already bounds-checked sample count for one track field.

    Returns:
        ``points``, so the caller can spend and pass it in one expression.

    Raises:
        ValueError: If this operation has already spent its budget.
        KeyError: If the budget was never seeded, which means the schema was
            built without :class:`_TrackBudget`. Falling back to an unbounded
            request would defeat the limit, so it fails loudly instead.
    """
    remaining = info.context[TRACK_BUDGET_KEY] - points
    if remaining < 0:
        raise ValueError(
            "track points budget exhausted: one request may return at most "
            f"{MAX_TRACK_POINTS_PER_REQUEST} track points"
        )
    info.context[TRACK_BUDGET_KEY] = remaining
    return points


TRACK_DESCRIPTION = (
    "GPS track, evenly downsampled to at most `points` samples. "
    f"One request may ask for {MAX_TRACK_POINTS_PER_REQUEST} track points in total, "
    "counted across every `track` field in it and charged on `points` as asked for, "
    "not on the rows a track turns out to hold."
)
"""Description for ``Activity.track``.

The budget is part of the field's contract, so a client reading the SDL can
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

    @strawberry.field(description=TRACK_DESCRIPTION)
    def track(self, info: Info, points: int = DEFAULT_TRACK_POINTS) -> list[TrackPoint]:
        # Spend first: the budget exists to stop the query being issued at all.
        wanted = _charge_track_points(info, _track_points(points))
        rows = service.track(info.context["session"], str(self.id), wanted)
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
        return [_activity(r) for r in rows]

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
            # Factories, not instances: Strawberry builds a fresh extension per request.
            lambda: QueryDepthLimiter(max_depth=max_depth),
            lambda: MaxTokensLimiter(max_token_count=max_tokens),
            _introspection_gate,
            _TrackBudget,
        ],
    )


schema = build_schema()
