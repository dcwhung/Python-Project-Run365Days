import importlib.util
from datetime import date, timedelta
from pathlib import Path
from types import MappingProxyType

import pytest
import strawberry
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from graphql import GraphQLSyntaxError, parse
from run365days.api import db, service
from run365days.api.app import GRAPHQL_PATH, HEALTH_PATH, create_app
from run365days.api.schema import (
    MAX_PAGE_SIZE,
    MAX_QUERY_TOKENS,
    MAX_TRACK_FIELDS_PER_REQUEST,
    MAX_TRACK_POINTS,
    MAX_TRACK_POINTS_PER_REQUEST,
    Query,
    build_schema,
    schema,
)
from run365days.dashboard.builder import downsample
from run365days.export import models
from run365days.export.sqlite import sqlite_url, write_sqlite

YEAR_DAYS = 365
STORED_TRACK_POINTS = 600
TRACKED_ACTIVITY_ID = "r0"
GRAPHIQL_ENV = "RUN365_GRAPHIQL"
VERCEL_ENTRY = Path(__file__).resolve().parents[1] / "api" / "graphql.py"

# The deepest document the front end sends (queries.ts YearQuery): year ->
# personalBests -> longest -> weather -> leaf.
DEEPEST_CLIENT_QUERY = """
    { year { personalBests { longest { weather { description } track { sec } } } } }
"""


@pytest.fixture
def client(sample_records, tmp_path):
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    app = create_app(path, graphiql=False)
    app.testing = True
    return app.test_client()


def _write_year_db(path: Path) -> None:
    """A full year of rows, with one activity carrying a real-sized track."""
    engine = create_engine(sqlite_url(path))
    models.Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                models.Meta(key="year", value="2021"),
                models.Meta(key="generated_at", value="2026-01-01T00:00:00"),
            ]
        )
        for i in range(YEAR_DAYS):
            day = (date(2021, 1, 1) + timedelta(days=i)).isoformat()
            session.add(
                models.Activity(
                    id=f"r{i}",
                    date=day,
                    start_time=f"{day} 06:00:00",
                    day_of_year=i + 1,
                    distance_km=5.0,
                    duration_sec=1800,
                    pace_sec_per_km=360,
                    calories=300,
                    avg_cadence=83.0,
                    avg_temp_c=None,
                    elevation_min_m=None,
                    elevation_max_m=None,
                    ascent_m=None,
                    has_gps=True,
                    num_points=STORED_TRACK_POINTS if i == 0 else 0,
                    weather_description=None,
                    weather_temp_c=None,
                    weather_humidity_pct=None,
                    weather_wind_kmh=None,
                    track_points=[
                        models.TrackPoint(seq=s, sec=s, lat=22.3, lon=114.2)
                        for s in range(STORED_TRACK_POINTS if i == 0 else 0)
                    ],
                )
            )
            session.add(models.WeightEntry(date=day, weight_lbs=154.0, weight_kg=70.0, bmi=24.0))
            session.add(models.DailyWeather(date=day, max_temp_c=21.0))
            session.add(models.WeatherWarning(date=day, type="Fire Danger", signal="RED"))
        session.commit()
    engine.dispose()


@pytest.fixture
def year_db(tmp_path):
    path = tmp_path / "year.db"
    _write_year_db(path)
    return path


@pytest.fixture
def year_client(year_db):
    app = create_app(year_db, graphiql=False)
    app.testing = True
    return app.test_client()


@pytest.fixture
def year_session(year_db):
    with db.session_scope(db.make_engine(year_db)) as session:
        yield session


def gql(client, query, variables=None):
    r = client.post(GRAPHQL_PATH, json={"query": query, "variables": variables or {}})
    assert r.status_code == 200, r.data
    body = r.get_json()
    assert "errors" not in body, body["errors"]
    return body["data"]


def gql_errors(client, query, variables=None):
    r = client.post(GRAPHQL_PATH, json={"query": query, "variables": variables or {}})
    body = r.get_json()
    assert "errors" in body, body
    return " ".join(e["message"] for e in body["errors"])


def gql_partial(client, query, variables=None):
    """Return the whole body, for the documents that are half served and half refused.

    ``gql`` and ``gql_errors`` each assert one of the two away, which is no use
    when what is under test is exactly which fields survived.
    """
    r = client.post(GRAPHQL_PATH, json={"query": query, "variables": variables or {}})
    assert r.status_code == 200, r.data
    body = r.get_json()
    assert "errors" in body, body
    return body


def test_health(client):
    r = client.get(HEALTH_PATH)
    assert r.get_json()["status"] == "ok"


def test_meta(client):
    d = gql(client, "{ meta { year generatedAt trackColumns } }")
    assert d["meta"]["year"] == 2021
    assert d["meta"]["generatedAt"] == "2026-01-01T00:00:00"
    assert d["meta"]["trackColumns"][0] == "sec"


def test_activities_list_and_filters(client):
    d = gql(client, "{ activities { id date startTime distanceKm hasGps warnings } }")
    assert [a["id"] for a in d["activities"]] == ["a", "b"]
    assert d["activities"][0]["warnings"] == ["RED FIRE DANGER WARNING"]

    d = gql(client, '{ activities(fromDate: "2021-01-09") { id } }')
    assert [a["id"] for a in d["activities"]] == ["b"]
    d = gql(client, '{ activities(toDate: "2021-01-08") { id } }')
    assert [a["id"] for a in d["activities"]] == ["a"]
    d = gql(client, "{ activities(hasGps: false) { id } }")
    assert [a["id"] for a in d["activities"]] == ["b"]
    d = gql(client, "{ activities(minKm: 6) { id } }")
    assert d["activities"] == []


def test_activity_with_nested_weather_and_track(client):
    d = gql(
        client,
        """{ activity(id: "a") {
            id paceSecPerKm avgTempC
            weather { description tempC humidityPct windKmh }
            track(points: 2) { sec lat elevationM tempC }
        } }""",
    )
    a = d["activity"]
    assert a["paceSecPerKm"] == 360
    assert a["avgTempC"] == 18.4
    assert a["weather"] == {
        "description": "Clear weather",
        "tempC": 19.0,
        "humidityPct": 65.0,
        "windKmh": 10.0,
    }
    assert [p["sec"] for p in a["track"]] == [0, 54]  # first and last of 4 stored points
    assert a["track"][0]["tempC"] == 18.0


def test_track_default_returns_all_stored_points(client):
    d = gql(client, '{ activity(id: "a") { track { sec } } }')
    assert len(d["activity"]["track"]) == 4


def test_unknown_activity_is_null(client):
    d = gql(client, '{ activity(id: "nope") { id } }')
    assert d["activity"] is None


def test_activity_without_weather_is_null(client):
    d = gql(client, '{ activity(id: "b") { weather { description } calories } }')
    assert d["activity"]["weather"] is None
    assert d["activity"]["calories"] is None


def test_year_summary(client):
    d = gql(
        client,
        """{ year {
            year
            totals { runs activeDays distanceKm avgPaceSecPerKm days }
            monthly { month runs bestPaceActivityId }
            weekly { week weekStart runs activityIds }
            dailyDistance { date distanceKm }
            trainingLoad { date ctl }
            personalBests { longest { id } fastest { id } mostCalories { id } }
        } }""",
    )
    y = d["year"]
    assert y["year"] == 2021
    assert y["totals"]["runs"] == 2 and y["totals"]["activeDays"] == 2
    assert y["totals"]["distanceKm"] == 10.0 and y["totals"]["days"] == 365
    assert y["monthly"][0]["runs"] == 2 and y["monthly"][0]["bestPaceActivityId"] == "a"
    assert len(y["weekly"]) == 53
    assert y["weekly"][1]["activityIds"] == ["a", "b"]  # Jan 8-9 fall in week 1
    assert len(y["dailyDistance"]) == 365 and y["dailyDistance"][7]["distanceKm"] == 5.0
    assert y["trainingLoad"][7]["ctl"] > 0
    assert y["personalBests"]["mostCalories"]["id"] == "a"


def test_year_argument_outside_data_is_empty(client):
    d = gql(client, "{ year(year: 2020) { totals { runs days } } }")
    assert d["year"]["totals"] == {"runs": 0, "days": 366}


def test_weight_weather_warnings_ranges(client):
    d = gql(
        client,
        """{
            weight(fromDate: "2021-01-09") { date weightLbs bmi }
            weather { date maxTempC rainfallMm sunrise }
            warnings(toDate: "2021-01-08") { date signal type }
        }""",
    )
    assert d["weight"] == [{"date": "2021-01-09", "weightLbs": 154.2, "bmi": 24.2}]
    assert d["weather"][0]["maxTempC"] == 21.0 and d["weather"][0]["rainfallMm"] is None
    assert len(d["warnings"]) == 2 and d["warnings"][0]["signal"] == "RED FIRE DANGER WARNING"


def test_graphql_errors_are_reported(client):
    r = client.post(GRAPHQL_PATH, json={"query": "{ nope }"})
    assert "errors" in r.get_json()


def test_missing_database_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        create_app(tmp_path / "missing.db")


def test_db_path_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv(db.DB_PATH_ENV, str(tmp_path / "env.db"))
    assert db.resolve_db_path() == tmp_path / "env.db"
    assert db.resolve_db_path(tmp_path / "explicit.db") == tmp_path / "explicit.db"


# ── AU-001: pagination ─────────────────────────────────────────────────────
def test_default_limit_returns_a_full_year_of_activities(year_client):
    d = gql(year_client, '{ activities(fromDate: "2021-01-01", toDate: "2021-12-31") { id } }')
    assert len(d["activities"]) == YEAR_DAYS


def test_default_limit_returns_a_full_year_of_weight_weather_and_warnings(year_client):
    d = gql(year_client, "{ weight { date } weather { date } warnings { date } }")
    assert len(d["weight"]) == YEAR_DAYS
    assert len(d["weather"]) == YEAR_DAYS
    assert len(d["warnings"]) == YEAR_DAYS


def test_activities_limit_and_offset_page_the_result(client):
    assert [a["id"] for a in gql(client, "{ activities(limit: 1) { id } }")["activities"]] == ["a"]
    d = gql(client, "{ activities(limit: 1, offset: 1) { id } }")
    assert [a["id"] for a in d["activities"]] == ["b"]
    assert gql(client, "{ activities(offset: 2) { id } }")["activities"] == []


def test_weight_weather_and_warnings_accept_limit_and_offset(year_client):
    d = gql(
        year_client,
        """{
            weight(limit: 2, offset: 1) { date }
            weather(limit: 3) { date }
            warnings(limit: 1, offset: 2) { date }
        }""",
    )
    assert [w["date"] for w in d["weight"]] == ["2021-01-02", "2021-01-03"]
    assert len(d["weather"]) == 3
    assert [w["date"] for w in d["warnings"]] == ["2021-01-03"]


def test_limit_above_max_page_size_is_rejected(client):
    message = gql_errors(client, f"{{ activities(limit: {MAX_PAGE_SIZE + 1}) {{ id }} }}")
    assert str(MAX_PAGE_SIZE) in message


@pytest.mark.parametrize(
    "query",
    [
        "{ activities(limit: 0) { id } }",
        "{ activities(offset: -1) { id } }",
        "{ weight(limit: -5) { date } }",
    ],
)
def test_out_of_range_page_arguments_are_rejected(client, query):
    assert gql_errors(client, query)


# ── AU-001: track downsampling happens in SQL ──────────────────────────────
def test_track_returns_at_most_the_requested_points(year_client):
    d = gql(year_client, '{ activity(id: "r0") { track(points: 10) { sec } } }')
    track = d["activity"]["track"]
    assert len(track) == 10
    assert track[0]["sec"] == 0 and track[-1]["sec"] == STORED_TRACK_POINTS - 1


def test_track_does_not_materialise_every_stored_point(year_session):
    points = 5
    loaded: list[object] = []
    event.listen(year_session, "loaded_as_persistent", lambda _s, obj: loaded.append(obj))

    rows = service.track(year_session, TRACKED_ACTIVITY_ID, points)

    assert len(rows) == points
    assert rows[0]["sec"] == 0 and rows[-1]["sec"] == STORED_TRACK_POINTS - 1
    assert len(loaded) <= 2 * points, f"loaded {len(loaded)} of {STORED_TRACK_POINTS} points"


def test_track_returns_every_stored_point_when_more_are_requested(year_session):
    rows = service.track(year_session, TRACKED_ACTIVITY_ID, MAX_TRACK_POINTS)
    assert len(rows) == STORED_TRACK_POINTS


def test_track_points_above_the_maximum_are_rejected(year_client):
    query = f'{{ activity(id: "r0") {{ track(points: {MAX_TRACK_POINTS + 1}) {{ sec }} }} }}'
    assert str(MAX_TRACK_POINTS) in gql_errors(year_client, query)


# ── AU-001: query depth and token limits ───────────────────────────────────
def test_deepest_client_query_is_within_the_depth_limit(client):
    assert gql(client, DEEPEST_CLIENT_QUERY)["year"] is not None


def test_query_deeper_than_the_configured_limit_is_rejected():
    shallow_schema = build_schema(max_depth=2)
    result = shallow_schema.execute_sync(DEEPEST_CLIENT_QUERY)
    assert result.errors
    assert "depth" in str(result.errors[0]).lower()


def test_document_with_too_many_tokens_is_rejected(client):
    flood = " ".join(f"a{i}: meta {{ year }}" for i in range(500))
    assert "token" in gql_errors(client, f"{{ {flood} }}").lower()


# ── AU-001: GraphiQL is off unless the environment asks for it ─────────────
def _load_vercel_entry(monkeypatch, db_path, graphiql_env=None):
    monkeypatch.setenv(db.DB_PATH_ENV, str(db_path))
    if graphiql_env is None:
        monkeypatch.delenv(GRAPHIQL_ENV, raising=False)
    else:
        monkeypatch.setenv(GRAPHIQL_ENV, graphiql_env)
    spec = importlib.util.spec_from_file_location("run365_vercel_entry", VERCEL_ENTRY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_graphiql_is_disabled_by_default(monkeypatch, sample_records, tmp_path):
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    module = _load_vercel_entry(monkeypatch, path)
    r = module.app.test_client().get(GRAPHQL_PATH, headers={"Accept": "text/html"})
    assert b"graphiql" not in r.data.lower()


def test_graphiql_is_served_when_the_env_var_is_on(monkeypatch, sample_records, tmp_path):
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    module = _load_vercel_entry(monkeypatch, path, graphiql_env="1")
    r = module.app.test_client().get(GRAPHQL_PATH, headers={"Accept": "text/html"})
    assert r.status_code == 200
    assert b"graphiql" in r.data.lower()


# ── W-008: introspection follows the same flag as GraphiQL ─────────────────
INTROSPECTION_QUERY = "{ __schema { queryType { name } } }"


def test_introspection_is_rejected_when_the_env_var_is_off(monkeypatch, client):
    monkeypatch.delenv(GRAPHIQL_ENV, raising=False)
    assert "introspection" in gql_errors(client, INTROSPECTION_QUERY).lower()


def test_introspection_is_served_when_the_env_var_is_on(monkeypatch, client):
    monkeypatch.setenv(GRAPHIQL_ENV, "1")
    assert gql(client, INTROSPECTION_QUERY)["__schema"]["queryType"]["name"] == "Query"


def test_typename_still_resolves_when_introspection_is_off(monkeypatch, client):
    monkeypatch.delenv(GRAPHIQL_ENV, raising=False)
    assert gql(client, "{ meta { __typename year } }")["meta"]["__typename"] == "Meta"


# ── W-009: counts let a client see rows the page window cut off ────────────
def test_activities_count_reports_rows_beyond_the_page(year_client):
    d = gql(year_client, "{ activities(limit: 10) { id } activitiesCount }")
    assert len(d["activities"]) == 10
    assert d["activitiesCount"] == YEAR_DAYS


def test_activities_count_applies_the_same_filters_as_the_list(client):
    d = gql(client, '{ activitiesCount(fromDate: "2021-01-09") }')
    assert d["activitiesCount"] == 1
    assert gql(client, "{ activitiesCount(minKm: 6) }")["activitiesCount"] == 0
    assert gql(client, "{ activitiesCount(hasGps: false) }")["activitiesCount"] == 1


def test_activities_count_ignores_limit_and_offset(year_client):
    d = gql(year_client, '{ activitiesCount(fromDate: "2021-01-01", toDate: "2021-01-31") }')
    assert d["activitiesCount"] == 31


def test_weight_weather_and_warnings_expose_counts(year_client):
    d = gql(
        year_client,
        """{
            weight(limit: 5) { date }
            weightCount
            weather(limit: 5) { date }
            weatherCount
            warnings(limit: 5) { date }
            warningsCount
        }""",
    )
    assert len(d["weight"]) == 5 and d["weightCount"] == YEAR_DAYS
    assert len(d["weather"]) == 5 and d["weatherCount"] == YEAR_DAYS
    assert len(d["warnings"]) == 5 and d["warningsCount"] == YEAR_DAYS


def test_dated_counts_honour_the_date_window(year_client):
    d = gql(
        year_client,
        """{
            weightCount(fromDate: "2021-01-01", toDate: "2021-01-10")
            weatherCount(toDate: "2021-01-05")
            warningsCount(fromDate: "2021-12-31")
        }""",
    )
    assert d["weightCount"] == 10
    assert d["weatherCount"] == 5
    assert d["warningsCount"] == 1


# ── W-010: "evenly downsampled" has to mean evenly ─────────────────────────
def _gaps(rows) -> list[int]:
    secs = [r["sec"] for r in rows]
    return [b - a for a, b in zip(secs, secs[1:], strict=False)]


@pytest.mark.parametrize("points", [3, 5, 7, 10, 37, 150, 599])
def test_track_samples_are_evenly_spaced(year_session, points):
    rows = service.track(year_session, TRACKED_ACTIVITY_ID, points)

    gaps = _gaps(rows)
    assert len(rows) == points
    assert rows[0]["sec"] == 0 and rows[-1]["sec"] == STORED_TRACK_POINTS - 1
    # Integer positions cannot divide evenly in general, so one sample of
    # slack is the best any sampler can do; two would mean a doubled gap.
    assert max(gaps) - min(gaps) <= 1, f"points={points} gaps={gaps}"


def test_track_samples_match_the_reference_downsampler(year_session):
    points = 7
    rows = service.track(year_session, TRACKED_ACTIVITY_ID, points)
    expected = downsample(list(range(STORED_TRACK_POINTS)), points)
    assert [r["sec"] for r in rows] == expected


# ── AU-047: a per-request budget on track points ───────────────────────────
PERSONAL_BEST_FIELDS = ("longest", "fastest", "longestTime", "mostCalories", "topCadence")
ACTIVITIES_THAT_FIT = MAX_TRACK_POINTS_PER_REQUEST // MAX_TRACK_POINTS
"""Activities that can each carry a full track before the budget runs out."""


def _fan_out_query(activities: int, points: int) -> str:
    return f"{{ activities(limit: {activities}) {{ track(points: {points}) {{ sec }} }} }}"


def test_a_whole_year_of_full_tracks_is_rejected(year_client):
    message = gql_errors(year_client, _fan_out_query(YEAR_DAYS, MAX_TRACK_POINTS))
    assert "budget" in message.lower()
    assert str(MAX_TRACK_POINTS_PER_REQUEST) in message


def test_the_budget_counts_requested_points_not_rows_returned(year_client):
    # Only r0 stores a track, so this query would return 600 rows -- far under
    # the budget. It is still refused, because the count is charged before the
    # SQL goes out: that is what makes the bound a bound and not a post-mortem.
    message = gql_errors(year_client, _fan_out_query(ACTIVITIES_THAT_FIT + 1, MAX_TRACK_POINTS))
    # Named, not just truthy: any other failure would satisfy a bare assert and
    # leave this test green while the budget did nothing.
    assert "budget" in message.lower()
    assert str(MAX_TRACK_POINTS_PER_REQUEST) in message


def test_a_request_that_exactly_spends_the_budget_is_served(year_client):
    data = gql(year_client, _fan_out_query(ACTIVITIES_THAT_FIT, MAX_TRACK_POINTS))
    assert len(data["activities"]) == ACTIVITIES_THAT_FIT
    assert len(data["activities"][0]["track"]) == STORED_TRACK_POINTS


def test_the_budget_is_per_request_and_does_not_leak_across_requests(year_client):
    # Two back-to-back requests that each spend the whole budget. Module-level
    # mutable state would starve the second one.
    query = _fan_out_query(ACTIVITIES_THAT_FIT, MAX_TRACK_POINTS)
    for _ in range(2):
        assert len(gql(year_client, query)["activities"]) == ACTIVITIES_THAT_FIT


def test_a_rejected_request_does_not_starve_the_next_one(year_client):
    assert gql_errors(year_client, _fan_out_query(YEAR_DAYS, MAX_TRACK_POINTS))
    data = gql(year_client, f'{{ activity(id: "{TRACKED_ACTIVITY_ID}") {{ track {{ sec }} }} }}')
    assert data["activity"]["track"]


def test_a_single_activity_track_still_gets_every_stored_point(year_client):
    query = (
        f'{{ activity(id: "{TRACKED_ACTIVITY_ID}") '
        f"{{ track(points: {MAX_TRACK_POINTS}) {{ sec }} }} }}"
    )
    assert len(gql(year_client, query)["activity"]["track"]) == STORED_TRACK_POINTS


def test_the_deepest_client_query_stays_within_the_budget(year_client):
    assert gql(year_client, DEEPEST_CLIENT_QUERY)["year"] is not None


def test_every_personal_best_may_carry_a_full_track(year_client):
    # The most expensive shape the schema allows a client to ask for: five
    # activities, each with the largest track the API serves.
    bests = " ".join(
        f"{field} {{ track(points: {MAX_TRACK_POINTS}) {{ sec }} }}"
        for field in PERSONAL_BEST_FIELDS
    )
    data = gql(year_client, f"{{ year {{ personalBests {{ {bests} }} }} }}")
    assert data["year"]["personalBests"]["longest"]["track"] is not None


def test_the_schema_does_not_batch_operations():
    # "Per request" and "per operation" are only the same thing while batching
    # is off. If this ever flips on, both budgets are spent once per operation
    # in the batch and the per-request bound they document stops being true.
    assert schema.config.batching_config is None


# ── AU-047 C-001: a per-request bound on track round trips ─────────────────
SQL_PER_TRACK_FIELD = 2
"""Statements one ``track`` field issues: a COUNT then a SELECT.

See ``service._even_sample_filter``. The number is fixed -- it does not move
with ``points`` -- which is exactly why the points budget cannot bound it.
"""

SQL_PER_LIST_FIELD = 2
"""Statements one ``activities`` field issues: the list, then its warnings."""

SQL_PER_ACTIVITY_FIELD = 2
"""Statements one ``activity(id:)`` field issues: the get, then its warnings."""

CHEAP_TRACK = "track(points: 1) { sec }"
"""The cheapest ``track`` field there is: one point, so it spends fields not points."""

ALIAS_FLOOD_ALIASES = 27
"""Aliases in the flood, as C-001 was reported.

The widest the *points* budget lets through: 27 x 365 = 9,855 points, where 28
would charge 10,220 and be refused for the wrong reason. Not a token-limit
figure -- this document lexes to 515 of MAX_QUERY_TOKENS.
"""

ALIAS_FLOOD_MAX_SQL = (
    MAX_TRACK_FIELDS_PER_REQUEST * SQL_PER_TRACK_FIELD + ALIAS_FLOOD_ALIASES * SQL_PER_LIST_FIELD
)
"""Statements a flood of this width may issue, whether it is served or refused.

Every ``track`` field the cap allows, plus one list query and one warnings
query for each of the ALIAS_FLOOD_ALIASES parents.

Deliberately *not* named as a per-request ceiling, because it is not one: the
cap bounds ``track`` statements only, and each parent field carrying a track
pays for itself on top. A document that spends its tokens on more parents than
this flood does goes higher -- 52 aliased parents each taking one track are
legal, served, and issue 208 statements. See the docstring of
``MAX_TRACK_FIELDS_PER_REQUEST`` for what the cap does and does not bound.
"""


def _document(*fields: str) -> str:
    """Wrap root *fields* in one anonymous query."""
    return "{ " + " ".join(fields) + " }"


def _page_field(activities: int, selection: str) -> str:
    """One list field of *activities* rows, each taking *selection*.

    One list field rather than a fan-out of aliases because the page window is
    the *shortest* way to put many tracks in one operation, not the only one.
    Measured: ``_cheap_tracks`` builds a 19-token document whatever
    *activities* says, while 64 ``track`` fields aliased under a single parent
    is 714 -- some 11 tokens per track, so the alias route buys tracks more
    cheaply than this one and sits well inside MAX_QUERY_TOKENS.

    That alias route reaches the cap, and the cap is what stops it: 64 aliased
    tracks are served, and 65 (725 tokens) are refused by the field cap, not by
    the parser. 1218 tokens is a different shape -- 64 aliased *parents* each
    carrying one track, which the parser does refuse. An earlier version of
    this docstring hung that figure on aliased ``track`` fields and concluded
    the alias route could not reach the cap at all; it can, and
    ``test_aliased_track_fields_under_one_parent_reach_the_field_cap`` covers
    it, since nothing did while that claim stood.

    So what the page window buys here is brevity, not reach -- and it couples
    every caller to MAX_PAGE_SIZE, hence the assertion.
    """
    # AU-050 raising the field cap is already written down as expected, and a
    # cap at or above MAX_PAGE_SIZE would turn these tests red on a `limit`
    # error that has nothing to say about the track budget. Fail on the
    # coupling instead, where the message points at the real cause.
    assert activities <= MAX_PAGE_SIZE, (
        f"this fan-out needs {activities} rows from one page, over "
        f"MAX_PAGE_SIZE={MAX_PAGE_SIZE}; it has to be rebuilt before the field "
        f"cap can go that high"
    )
    return f"activities(limit: {activities}) {{ {selection} }}"


def _one_page(activities: int, selection: str) -> str:
    """A whole document whose only root field is one page of *activities*."""
    return _document(_page_field(activities, selection))


def _cheap_tracks(activities: int) -> str:
    """One list field taking the cheapest track it can, to spend fields not points."""
    return _one_page(activities, CHEAP_TRACK)


def _aliased_tracks(tracks: int, activity_id: str = TRACKED_ACTIVITY_ID) -> str:
    """*tracks* aliased ``track`` fields under one parent: the fan-out route.

    The shape C-001 was filed as, reduced to one parent. It spends no page
    window at all, so it is the route ``_page_field`` does not take.
    """
    fields = " ".join(f"t{n}: {CHEAP_TRACK}" for n in range(tracks))
    return _document(f'activity(id: "{activity_id}") {{ {fields} }}')


def _parent_flood(parents: int, activity_id: str = TRACKED_ACTIVITY_ID) -> str:
    """*parents* aliased ``activity(id:)`` fields, one cheap ``track`` each."""
    fields = " ".join(
        f'a{n}: activity(id: "{activity_id}") {{ {CHEAP_TRACK} }}' for n in range(parents)
    )
    return _document(fields)


def _list_flood(fields: int) -> str:
    """*fields* aliased ``activities`` fields taking no ``track`` at all.

    Spends neither budget, so nothing but MAX_QUERY_TOKENS bounds it.
    """
    return _document(" ".join(f"a{n}: activities {{ id }}" for n in range(fields)))


def _parses_within(document: str, max_tokens: int) -> bool:
    """Whether graphql-core will parse *document* under a *max_tokens* ceiling."""
    try:
        parse(document, max_tokens=max_tokens)
    except GraphQLSyntaxError:
        return False
    return True


def _token_count(document: str) -> int:
    """Tokens MaxTokensLimiter charges *document*.

    The smallest ceiling graphql-core will parse it under, which is by
    definition the number the limiter compares against MAX_QUERY_TOKENS --
    rather than a re-implementation of the lexer that could drift from it.
    """
    high = 2
    while not _parses_within(document, high):
        high *= 2
    low = high // 2
    while low < high:
        mid = (low + high) // 2
        if _parses_within(document, mid):
            high = mid
        else:
            low = mid + 1
    return low


def _alias_flood(aliases: int = ALIAS_FLOOD_ALIASES, points: int = 1) -> str:
    """Build the request C-001 was found with: cheap in points, dear in queries."""
    fields = " ".join(
        f"a{n}: activities(limit: {YEAR_DAYS}) {{ track(points: {points}) {{ sec }} }}"
        for n in range(aliases)
    )
    return f"{{ {fields} }}"


@pytest.fixture
def sql_count():
    """Count statements every engine issues while the fixture is alive."""
    counter = [0]

    def tally(*_args, **_kwargs):
        counter[0] += 1

    # Listening on the Engine class, not one instance: create_app builds its
    # own engine, so a test client gives no engine to attach to.
    event.listen(Engine, "before_cursor_execute", tally)
    try:
        yield counter
    finally:
        event.remove(Engine, "before_cursor_execute", tally)


def test_an_alias_flood_under_the_points_budget_is_still_rejected(year_client):
    # The whole of C-001: this asks for one point per track, so its points
    # charge fits the budget with room to spare, yet it is the single most
    # expensive request the schema could serve.
    charged = ALIAS_FLOOD_ALIASES * YEAR_DAYS
    assert charged < MAX_TRACK_POINTS_PER_REQUEST, "shape must pass the points budget"

    message = gql_errors(year_client, _alias_flood())

    assert "field" in message.lower()
    assert str(MAX_TRACK_FIELDS_PER_REQUEST) in message


def test_the_track_field_cap_is_the_boundary(year_client):
    fits = gql(year_client, _cheap_tracks(MAX_TRACK_FIELDS_PER_REQUEST))
    assert len(fits["activities"]) == MAX_TRACK_FIELDS_PER_REQUEST

    over = gql_errors(year_client, _cheap_tracks(MAX_TRACK_FIELDS_PER_REQUEST + 1))
    assert str(MAX_TRACK_FIELDS_PER_REQUEST) in over


DOCUMENTED_WORST_CASES = (
    # Every shape MAX_TRACK_FIELDS_PER_REQUEST's docstring quotes a number for,
    # with the tokens it lexes to and the statements it issues. Read the label
    # as the sentence in that docstring this row is holding to its word.
    (
        "saturating the cap takes one list field",
        _cheap_tracks(MAX_TRACK_FIELDS_PER_REQUEST),
        19,
        130,
    ),
    ("spending the document on parents reaches further", _parent_flood(52), 990, 208),
    ("a document that takes no track is not bounded here", _list_flood(166), 998, 332),
)
"""(label, document, tokens, statements) for each worst case the docstrings cite.

These numbers existed only as prose until AU-047 W-017. Prose does not go red:
four rounds of this ticket published a measured figure with no gate under it,
and the fifth found one of them wrong. A number worth writing down is worth
asserting, so anything quoted up there is quoted here too.
"""


@pytest.mark.parametrize(
    ("label", "document", "tokens", "statements"),
    DOCUMENTED_WORST_CASES,
    # Named cases: the default id would print a 166-alias document per row.
    ids=[case[0] for case in DOCUMENTED_WORST_CASES],
)
def test_the_documented_worst_cases_still_measure_as_documented(
    label, document, tokens, statements, year_client, sql_count
):
    # Each row is served, so what is pinned is the real cost of a request the
    # API accepts -- not the cost of one it turns away. Lower MAX_QUERY_TOKENS
    # or the field cap, or give a list field a third query, and this is what
    # tells you the docstrings have gone stale.
    assert _token_count(document) == tokens, f"token cost drifted: {label}"

    assert gql(year_client, document), f"documented as served: {label}"
    assert sql_count[0] == statements, f"statement cost drifted: {label}"


def test_aliased_track_fields_under_one_parent_reach_the_field_cap(year_client, sql_count):
    # The route _page_field does not take, and the one C-001 was filed as.
    # It had no test while _page_field's docstring said it could not exist:
    # 64 aliased tracks lex to 714 tokens, not the 1218 that docstring quoted,
    # so the parser lets them through and the field cap is what answers.
    fits = _aliased_tracks(MAX_TRACK_FIELDS_PER_REQUEST)
    assert _token_count(fits) == 714 < MAX_QUERY_TOKENS

    served = gql(year_client, fits)
    assert len(served["activity"]) == MAX_TRACK_FIELDS_PER_REQUEST
    assert sql_count[0] == (
        SQL_PER_ACTIVITY_FIELD + MAX_TRACK_FIELDS_PER_REQUEST * SQL_PER_TRACK_FIELD
    )

    over = _aliased_tracks(MAX_TRACK_FIELDS_PER_REQUEST + 1)
    assert _token_count(over) == 725 < MAX_QUERY_TOKENS, "the parser must not be the one refusing"
    assert str(MAX_TRACK_FIELDS_PER_REQUEST) in gql_errors(year_client, over)


def test_the_parser_never_gets_to_refuse_an_aliased_track_flood(year_client):
    # The cap has to hold across the whole token budget, not just at 65:
    # aliases are cheap enough that the widest fan-out MAX_QUERY_TOKENS admits
    # is far past the cap, so the parser refuses none of the documents the cap
    # is there for. Found by growing the shape rather than hard-coding the
    # width, so this stays true if either limit moves.
    widest = MAX_TRACK_FIELDS_PER_REQUEST
    while _token_count(_aliased_tracks(widest + 1)) <= MAX_QUERY_TOKENS:
        widest += 1

    assert widest > MAX_TRACK_FIELDS_PER_REQUEST, "the token limit must not pre-empt the cap"
    assert str(MAX_TRACK_FIELDS_PER_REQUEST) in gql_errors(year_client, _aliased_tracks(widest))


def test_the_document_that_does_out_token_the_parser_aliases_parents(year_client):
    # Where 1218 actually comes from: one parent per track, not one parent
    # carrying many. Each parent repeats the whole `activity(id: "r0")` head,
    # so a track costs ~19 tokens here against ~11 as a bare alias.
    flood = _parent_flood(MAX_TRACK_FIELDS_PER_REQUEST)

    assert _token_count(flood) == 1218 > MAX_QUERY_TOKENS
    assert "token" in gql_errors(year_client, flood).lower()


def test_the_alias_flood_issues_no_more_statements_than_the_field_cap_allows(
    year_client, sql_count
):
    # Both documents here are at most ALIAS_FLOOD_ALIASES parents wide, so the
    # bound is theirs, not every request's -- a wider document issues more.
    # What it pins is that the cap holds for a *refused* request too: an error
    # does not unwind the queries already sent, and before the cap existed a
    # request that ended in an error had still issued some 20,000 statements
    # getting there.
    gql(year_client, _cheap_tracks(MAX_TRACK_FIELDS_PER_REQUEST))
    assert 0 < sql_count[0] <= ALIAS_FLOOD_MAX_SQL

    sql_count[0] = 0
    gql_errors(year_client, _alias_flood())
    assert sql_count[0] <= ALIAS_FLOOD_MAX_SQL


def test_the_field_cap_is_per_request_and_does_not_leak_across_requests(year_client):
    query = _cheap_tracks(MAX_TRACK_FIELDS_PER_REQUEST)
    for _ in range(2):
        assert len(gql(year_client, query)["activities"]) == MAX_TRACK_FIELDS_PER_REQUEST


# ── AU-047: the two budgets have to hold each other's line ─────────────────
CHEAP_ALIASES = 20
"""Cheap ``track`` fields hung off the back of a document, to see them refused.

Well under MAX_TRACK_FIELDS_PER_REQUEST once the fields ahead of them are
counted, so nothing here can be refused by the field budget by accident: if
these are refused, the points budget is what refused them.
"""


def _cheap_alias(name: str, activity: str) -> str:
    """One aliased activity taking the cheapest track there is."""
    return f'{name}: activity(id: "{activity}") {{ {CHEAP_TRACK} }}'


def test_a_cheap_track_is_refused_once_the_points_budget_is_spent(year_client):
    # The budget is spent exactly, not overrun, and then 20 of the cheapest
    # field the schema has ask for one point each. Every one of them has to be
    # refused: a budget that only stops the field that overruns it, and lets
    # the traffic behind it through, is not a budget.
    query = _document(
        _page_field(ACTIVITIES_THAT_FIT, f"track(points: {MAX_TRACK_POINTS}) {{ sec }}"),
        *(_cheap_alias(f"c{n}", f"r{n}") for n in range(CHEAP_ALIASES)),
    )

    body = gql_partial(year_client, query)
    data = body["data"]

    assert len(data["activities"]) == ACTIVITIES_THAT_FIT, "the spend itself must be served"
    assert [n for n in range(CHEAP_ALIASES) if data[f"c{n}"] is not None] == []
    messages = [e["message"] for e in body["errors"]]
    assert len(messages) == CHEAP_ALIASES
    # Named, not just counted: the field budget would refuse these too, and a
    # bare count cannot tell the two budgets apart.
    assert all(str(MAX_TRACK_POINTS_PER_REQUEST) in m for m in messages), messages


def test_a_field_that_overruns_the_points_budget_leaves_it_for_the_next_one(year_client):
    # The charge is all-or-nothing: a refused field writes nothing back, so
    # what is left stays available to a field that fits in it. The budget
    # refuses each field on that field's own cost, and does not latch.
    half = MAX_TRACK_POINTS // 2
    spend = (MAX_TRACK_POINTS_PER_REQUEST - half) // half
    track_of = f'activity(id: "{TRACKED_ACTIVITY_ID}") {{ track(points: %d) {{ sec }} }}'
    query = _document(
        _page_field(spend, f"track(points: {half}) {{ sec }}"),
        f"over: {track_of % MAX_TRACK_POINTS}",
        f"after: {track_of % half}",
    )

    body = gql_partial(year_client, query)
    data = body["data"]

    assert len(data["activities"]) == spend
    assert data["over"] is None, f"{MAX_TRACK_POINTS} points must not fit in the {half} left"
    assert len(data["after"]["track"]) == half, "the refusal must not have spent the remainder"


# ── AU-047: the charge follows the resolved field, not the syntax ──────────
CHEAP_TRACK_FRAGMENT = f"fragment CheapTrack on Activity {{ {CHEAP_TRACK} }}"


@pytest.mark.parametrize(
    "selection,suffix",
    [
        ("...CheapTrack", f" {CHEAP_TRACK_FRAGMENT}"),
        (f"... on Activity {{ {CHEAP_TRACK} }}", ""),
    ],
    ids=["named-fragment", "inline-fragment"],
)
def test_a_track_reached_through_a_fragment_is_charged(year_client, selection, suffix):
    # A fragment resolves the same field by another spelling. A cap that
    # counted selections in the document rather than resolver calls would let
    # either of these spellings past it.
    fits = _one_page(MAX_TRACK_FIELDS_PER_REQUEST, selection) + suffix
    assert len(gql(year_client, fits)["activities"]) == MAX_TRACK_FIELDS_PER_REQUEST

    over = _one_page(MAX_TRACK_FIELDS_PER_REQUEST + 1, selection) + suffix
    assert str(MAX_TRACK_FIELDS_PER_REQUEST) in gql_errors(year_client, over)


def test_a_skipped_track_is_not_charged(year_client):
    # Two track fields per activity, one of them skipped: 2 x 64 fields if
    # @skip were charged, which the cap would refuse. Only the resolved one
    # may count, because only the resolved one costs a query.
    query = _one_page(
        MAX_TRACK_FIELDS_PER_REQUEST,
        f"skipped: track(points: 1) @skip(if: true) {{ sec }} charged: {CHEAP_TRACK}",
    )

    rows = gql(year_client, query)["activities"]

    assert len(rows) == MAX_TRACK_FIELDS_PER_REQUEST
    assert "skipped" not in rows[0], "the skipped field must not be resolved at all"
    assert len(rows[0]["charged"]) == 1


# ── AU-047 W-014: the budgets fail closed when they were never seeded ──────
TRACK_QUERY = f'{{ activity(id: "{TRACKED_ACTIVITY_ID}") {{ track(points: 10) {{ sec }} }} }}'


def _track_rows(result) -> list:
    activity = (result.data or {}).get("activity") or {}
    return activity.get("track") or []


NOT_SEEDED = "not seeded"
"""Words from the unseeded-budget refusal, asserted rather than left to truthiness.

A bare ``assert result.errors`` cannot tell a budget refusal from an error that
has nothing to do with the budget. Measured against a mutant that reads the
budget as ``.get(KEY, MAX_...)`` instead: the frozen context below still comes
back with an error, but it is ``'mappingproxy' object does not support item
assignment``, raised where the charge writes the counter back. The refusal is
incidental to that context and does not generalise -- the same mutant, given a
plain ``dict``, serves the track unbounded. Naming the message is what tells
those two apart.
"""


def test_track_fails_closed_when_the_schema_has_no_budget_extension(year_session):
    # A schema built without _TrackBudget seeds nothing. Serving the track
    # anyway would mean an unbounded request, so it must refuse instead.
    unbounded = strawberry.Schema(query=Query, extensions=[])

    result = unbounded.execute_sync(TRACK_QUERY, context_value={"session": year_session})

    assert result.errors
    assert NOT_SEEDED in result.errors[0].message
    assert _track_rows(result) == []


def test_track_fails_closed_when_the_context_cannot_be_seeded(year_session):
    # A read-only Mapping is not a MutableMapping, so _TrackBudget skips it --
    # but info.context["session"] still reads, so track is reachable. The
    # invariant is that the *missing budget* is what refuses the field. This
    # context happens to reject the write back as well, so it would error
    # either way; NOT_SEEDED is what pins the reason rather than the symptom.
    frozen = MappingProxyType({"session": year_session})

    result = schema.execute_sync(TRACK_QUERY, context_value=frozen)

    assert result.errors
    assert NOT_SEEDED in result.errors[0].message
    assert _track_rows(result) == []
