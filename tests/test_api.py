import importlib.util
import logging
import re
from contextlib import ExitStack
from datetime import date, timedelta
from pathlib import Path
from types import MappingProxyType

import pytest
import strawberry
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from graphql import (
    GraphQLInterfaceType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSyntaxError,
    GraphQLUnionType,
    parse,
)
from graphql import build_schema as build_schema_from_sdl
from run365days.api import db, service
from run365days.api.app import GRAPHQL_PATH, HEALTH_PATH, create_app
from run365days.api.schema import (
    DEFAULT_PAGE_SIZE,
    MAX_LIST_ROWS_PER_REQUEST,
    MAX_PAGE_SIZE,
    MAX_QUERY_DEPTH,
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


def _activity_row(index: int, stored: int, prefix: str = "r", has_gps: bool = True):
    """One activity carrying *stored* track rows, numbered ``seq``/``sec`` 0..stored-1.

    ``sec`` is the row's own position, which is what lets every sampling test
    read a returned ``sec`` as "this is the row the sampler picked".
    """
    day = (date(2021, 1, 1) + timedelta(days=index)).isoformat()
    return models.Activity(
        id=f"{prefix}{index}",
        date=day,
        start_time=f"{day} 06:00:00",
        day_of_year=index + 1,
        distance_km=5.0,
        duration_sec=1800,
        pace_sec_per_km=360,
        calories=300,
        avg_cadence=83.0,
        avg_temp_c=None,
        elevation_min_m=None,
        elevation_max_m=None,
        ascent_m=None,
        has_gps=has_gps,
        num_points=stored,
        weather_description=None,
        weather_temp_c=None,
        weather_humidity_pct=None,
        weather_wind_kmh=None,
        track_points=[models.TrackPoint(seq=s, sec=s, lat=22.3, lon=114.2) for s in range(stored)],
    )


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
            session.add(_activity_row(i, STORED_TRACK_POINTS if i == 0 else 0))
            session.add(models.WeightEntry(date=day, weight_lbs=154.0, weight_kg=70.0, bmi=24.0))
            session.add(models.DailyWeather(date=day, max_temp_c=21.0))
            session.add(models.WeatherWarning(date=day, type="Fire Danger", signal="RED"))
        session.commit()
    engine.dispose()


def _write_track_db(path: Path, lengths, prefix: str) -> None:
    """A database whose only interesting axis is how long each activity's track is."""
    engine = create_engine(sqlite_url(path))
    models.Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                models.Meta(key="year", value="2021"),
                models.Meta(key="generated_at", value="2026-01-01T00:00:00"),
            ]
        )
        for i, stored in enumerate(lengths):
            session.add(_activity_row(i, stored, prefix=prefix, has_gps=stored > 0))
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


VARIED_TRACK_LENGTHS = (0, 1, 2, 4, 250, 347, 600, 601, 1250)
"""Stored track lengths for the batching fixture: every activity a different one.

A batch reads many tracks in one statement, so a sampler that derived one
stride for the whole batch -- or that rounded in SQL rather than in Python --
would still look right on a fixture where every track is the same length. The
lengths straddle the 600-row export limit and include the degenerate 0, 1 and 2
that the position arithmetic has to survive.
"""

VARIED_IDS = tuple(f"v{i}" for i in range(len(VARIED_TRACK_LENGTHS)))
"""Activity ids of the varied fixture, in the order ``activities`` returns them."""


@pytest.fixture
def varied_db(tmp_path):
    path = tmp_path / "varied.db"
    _write_track_db(path, VARIED_TRACK_LENGTHS, prefix="v")
    return path


@pytest.fixture
def varied_session(varied_db):
    with db.session_scope(db.make_engine(varied_db)) as session:
        yield session


@pytest.fixture
def varied_client(varied_db):
    app = create_app(varied_db, graphiql=False)
    app.testing = True
    return app.test_client()


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


def test_track_points_of_zero_is_refused_rather_than_read_as_no_limit(year_client):
    # 0 is the one value the two deployment modes used to disagree on. Before
    # AU-001 this side read `if points:`, so 0 meant "no limit" -- the DoS
    # vector AU-001 closed. Static mode kept that reading until CUI-0025, and
    # `it("refuses points: 0 the way api mode does")` in
    # frontend/src/data/static/source.test.ts is the other half of this pair.
    # The message is asserted whole because both sides now raise it verbatim,
    # so a client writes one error path rather than one per mode.
    query = '{ activity(id: "r0") { track(points: 0) { sec } } }'
    assert f"points must be between 1 and {MAX_TRACK_POINTS}, got 0" in gql_errors(
        year_client, query
    )


# ── CUI-0025: the bounds a client can only find by tripping over them ──────
def _field_descriptions() -> dict[str, str]:
    """Return ``{root field name: SDL description}`` for every field on Query."""
    sdl = build_schema_from_sdl(schema.as_str())
    return {name: (f.description or "") for name, f in sdl.query_type.fields.items()}


LIST_FIELDS = ("activities", "weight", "weather", "warnings")
"""The four root fields that take a ``limit``/``offset`` page window."""


def test_the_sdl_states_the_range_track_points_must_fall_in():
    sdl = build_schema_from_sdl(schema.as_str())
    track = sdl.type_map["Activity"].fields["track"]

    # The bare number will not do: MAX_TRACK_POINTS_PER_REQUEST is 10000, whose
    # digits contain MAX_TRACK_POINTS's, so `"1000" in description` is already
    # true of a description that never states this range at all.
    assert f"(1-{MAX_TRACK_POINTS})" in (track.description or ""), (
        "MAX_TRACK_POINTS lives in a Python docstring and a runtime error; a client "
        "reading the SDL cannot see the ceiling until it trips over it"
    )


@pytest.mark.parametrize("field", LIST_FIELDS)
def test_the_sdl_states_the_range_limit_must_fall_in(field):
    descriptions = _field_descriptions()

    assert f"(1-{MAX_PAGE_SIZE})" in descriptions[field], (
        f"`{field}` takes a limit bounded by MAX_PAGE_SIZE and says so nowhere in the SDL"
    )


def test_the_year_description_does_not_mention_a_limit_it_has_no_argument_for():
    # S-053. `year` spends the same row budget as the list fields, so it carries
    # the budget sentence -- but it takes no `limit`, and a sentence saying the
    # charge is "on `limit` as asked for" sends its reader looking for an
    # argument that is not there.
    sdl = build_schema_from_sdl(schema.as_str())
    year = sdl.query_type.fields["year"]
    assert "limit" not in year.args, "this test is stale: `year` grew a limit argument"

    assert "`limit`" not in (year.description or ""), (
        "`year` has no `limit` argument, so its description must not explain a charge "
        "in terms of one"
    )


@pytest.mark.parametrize("field", LIST_FIELDS + ("year",))
def test_every_field_that_spends_the_row_budget_says_so(field):
    # The half of S-053 that must survive splitting the note in two: `year`
    # loses the `limit` sentence but keeps the shared budget one.
    assert str(MAX_LIST_ROWS_PER_REQUEST) in _field_descriptions()[field]


# ── AU-001: query depth and token limits ───────────────────────────────────
def test_deepest_client_query_is_within_the_depth_limit(client):
    assert gql(client, DEEPEST_CLIENT_QUERY)["year"] is not None


def test_query_deeper_than_the_configured_limit_is_rejected():
    shallow_schema = build_schema(max_depth=2)
    result = shallow_schema.execute_sync(DEEPEST_CLIENT_QUERY)
    assert result.errors
    assert "depth" in str(result.errors[0]).lower()


DEEPEST_REACHABLE_DEPTH = 4
"""The smallest ``max_depth`` that admits the deepest document this schema can build.

One *below* MAX_QUERY_DEPTH, which is the whole of CUI-0003: the depth limiter
cannot fire against the schema as it stands, and MAX_QUERY_TOKENS is what
actually answers an alias flood.
"""


def _unwrap(gql_type):
    """Strip ``!`` and ``[]`` off *gql_type* to get at the named type inside."""
    while isinstance(gql_type, GraphQLList | GraphQLNonNull):
        gql_type = gql_type.of_type
    return gql_type


def _deepest_selection(gql_type, seen=()):
    """Field levels the deepest document rooted at *gql_type* can nest.

    Walks the type graph rather than any particular document, so it answers
    "how deep can a client go", not "how deep does the dashboard go".

    Counts the leaf field as a level, which ``QueryDepthLimiter`` does not, so
    the ``max_depth`` the same document needs is one less than this returns.
    ``seen`` carries the types already on this path: the graph is acyclic
    today, and this is what keeps the walk finite on the day it stops being.

    Only object types are walked. An interface or a union would be counted as
    a leaf, so anything nested under one would be missed and the depth
    *under-reported* -- which is why the caller asserts the schema has none
    before trusting what this returns.
    """
    if not isinstance(gql_type, GraphQLObjectType) or gql_type.name in seen:
        return 0
    seen = (*seen, gql_type.name)
    return max(
        (
            1 + _deepest_selection(_unwrap(field.type), seen)
            for name, field in gql_type.fields.items()
            if not name.startswith("__")
        ),
        default=0,
    )


def test_the_depth_limiter_refuses_one_level_below_the_deepest_document(year_session):
    # CUI-0003. The existing max_depth=2 test above shows the limiter refusing
    # something; it does not show where the edge is. These two calls put the
    # deepest document the schema can express on either side of it, so the
    # limiter is pinned as connected rather than merely present -- the schema
    # reports 100% coverage on build_schema either way.
    deep_enough = build_schema(max_depth=DEEPEST_REACHABLE_DEPTH)
    served = deep_enough.execute_sync(DEEPEST_CLIENT_QUERY, context_value={"session": year_session})
    assert not served.errors, served.errors
    assert served.data["year"] is not None

    one_short = build_schema(max_depth=DEEPEST_REACHABLE_DEPTH - 1)
    refused = one_short.execute_sync(DEEPEST_CLIENT_QUERY, context_value={"session": year_session})
    assert refused.errors
    assert "depth" in str(refused.errors[0]).lower()


def test_the_type_graph_stays_one_level_below_the_depth_limit():
    # What this test is for is the person who adds a nested field. MAX_QUERY_DEPTH
    # is set one level above anything the schema can express, so today the
    # limiter never fires -- deliberately, as headroom. Deepen the type graph by
    # one and that silently becomes "fires on the deepest legal document", which
    # is a change worth noticing rather than discovering from a client. This is
    # what notices it.
    # Both readings below are questions about this one SDL, not two schemas.
    sdl = build_schema_from_sdl(schema.as_str())

    sdl_types = sdl.type_map.values()
    assert not [
        t.name for t in sdl_types if isinstance(t, GraphQLInterfaceType | GraphQLUnionType)
    ], (
        "the SDL grew an abstract type; _deepest_selection only walks object types and "
        "counts an interface or union as a leaf, so it now under-reports depth and this "
        "guard would stay green while MAX_QUERY_DEPTH quietly becomes able to fire. "
        "Teach _deepest_selection to walk an interface's fields and a union's possible "
        "types before trusting the number below"
    )

    root = sdl.query_type
    reachable = _deepest_selection(root) - 1

    assert reachable == DEEPEST_REACHABLE_DEPTH, (
        "the type graph changed depth; re-read MAX_QUERY_DEPTH's docstring, which "
        "says the limiter cannot fire, and the test above, which says where it would"
    )
    assert reachable < MAX_QUERY_DEPTH, "the limiter can now refuse a document the schema allows"


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


# ── CUI-0004: what one sample means ────────────────────────────────────────
def test_a_single_sample_is_the_last_row_not_the_first(year_session):
    # "Evenly spaced" has no meaning for one sample, so the field has to pick a
    # row, and it picks the last -- the same row ``downsample`` picks for
    # ``limit < 2``. Nothing about the arithmetic forces that choice; the two
    # implementations agree only because both were written to. This pins the
    # choice so that changing it on one side shows up as a failure here rather
    # than as api mode and static mode drawing different tracks.
    rows = service.track(year_session, TRACKED_ACTIVITY_ID, 1)

    assert len(rows) == 1
    assert rows[0]["sec"] == STORED_TRACK_POINTS - 1
    assert [r["sec"] for r in rows] == downsample(list(range(STORED_TRACK_POINTS)), 1)


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
SQL_PER_TRACK_BATCH = 2
"""Statements one batched ``track`` read issues: a grouped COUNT, then one SELECT.

The COUNT is grouped by ``activity_id`` and the SELECT numbers rows with
``row_number() OVER (PARTITION BY activity_id ORDER BY seq)``, so one of each
covers the whole batch. See ``service.tracks``.

This is the number AU-050 moved. It is charged per *batch*, not per ``track``
field: before the ticket every field paid two statements of its own, which is
why the first DOCUMENTED_WORST_CASES row -- a page of 64 tracks -- asserted 130
where it now asserts 4. It is still fixed, and still does not move with
``points``, which is why the points budget cannot bound the round trips and
MAX_TRACK_FIELDS_PER_REQUEST has to.
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
    MAX_TRACK_FIELDS_PER_REQUEST * SQL_PER_TRACK_BATCH + ALIAS_FLOOD_ALIASES * SQL_PER_LIST_FIELD
)
"""Statements a flood of this width may issue, whether it is served or refused.

Every ``track`` field the cap allows opening a batch of its own -- the worst
case, which batching makes an over-estimate rather than a reading -- plus one
list query and one warnings query for each of the ALIAS_FLOOD_ALIASES parents.

Deliberately *not* named as a per-request ceiling, because it is not one: the
cap bounds ``track`` statements only, and each parent field carrying a track
pays for itself on top. A document that spends its tokens on more parents than
this flood does goes higher -- 52 aliased parents each taking one track of a
*different* activity are legal, served, and issue 208 statements. See the
docstring of ``MAX_TRACK_FIELDS_PER_REQUEST`` for what the cap does and does
not bound.
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
    """*parents* aliased ``activity(id:)`` fields, one cheap ``track`` each.

    All of them name the same activity, so their tracks are one batch.
    """
    fields = " ".join(
        f'a{n}: activity(id: "{activity_id}") {{ {CHEAP_TRACK} }}' for n in range(parents)
    )
    return _document(fields)


def _distinct_parent_flood(parents: int, points: int = 1) -> str:
    """*parents* aliased ``activity(id:)`` fields, each naming a different activity.

    The same shape as ``_parent_flood`` and the same token cost -- an id is one
    STRING token whatever it spells -- but no two of these tracks can share a
    batch, so this is the shape that still pays two statements per track.

    *points* does not move the token cost either (an INT is one token however
    many digits it spells), so raising it turns the same document from one the
    points budget serves whole into one it refuses most of, without letting the
    parser answer instead.
    """
    track = f"track(points: {points}) {{ sec }}"
    fields = " ".join(f'a{n}: activity(id: "r{n}") {{ {track} }}' for n in range(parents))
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
def sql_count(year_client):
    """Count statements every engine issues while the fixture is alive.

    Takes ``year_client`` for the ordering rather than for the value: building
    that fixture issues ~393 statements of its own, so a test that starts
    counting first reads the setup instead of its request. Depending on it here
    makes pytest build the client first whatever order a test lists its
    arguments in, which is the difference between the invariant being stated and
    the invariant holding -- every consumer of this fixture uses that client
    anyway. A second kind of client would want a ``sql_count_for(client)``
    factory rather than a loosening of this.
    """
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
        4,
    ),
    ("52 parents of one activity share one batch", _parent_flood(52), 990, 106),
    ("52 parents of different activities share none", _distinct_parent_flood(52), 990, 208),
    # The fourth row used to be `_list_flood(166)`, served for 332 statements
    # under the sentence "no budget here bounds it". CUI-0027 gave it a budget,
    # so it is no longer a *served* worst case and no longer belongs in a table
    # of them; `test_the_widest_list_fan_out_is_refused_after_the_budget` is
    # where it is pinned now.
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
    # One activity, one `points`, so all 64 fields are one batch: the parent's
    # two statements plus the batch's two, not two per field as before AU-050.
    assert sql_count[0] == SQL_PER_ACTIVITY_FIELD + SQL_PER_TRACK_BATCH

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

    # The boundary the MAX_TRACK_FIELDS_PER_REQUEST docstring quotes: 52 of
    # these parse (and are served -- see DOCUMENTED_WORST_CASES), 53 do not.
    assert _token_count(_parent_flood(53)) == 1009 > MAX_QUERY_TOKENS


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


PARENT_FLOOD_WIDTH = 52
"""Aliased ``activity(id:)`` parents MAX_QUERY_TOKENS admits, one cheap track each.

990 of the 1000 tokens; 53 lexes to 1009 and no longer parses. Written down so
the ceiling below can be read, but derived rather than trusted -- see
``_widest_parent_flood``.
"""


def _widest_parent_flood() -> int:
    """The most aliased ``activity(id:)`` parents MAX_QUERY_TOKENS lets through."""
    parents = 1
    while _token_count(_distinct_parent_flood(parents + 1)) <= MAX_QUERY_TOKENS:
        parents += 1
    return parents


def test_a_flood_of_aliased_parents_issues_more_statements_than_the_field_cap_bounds(
    year_client, sql_count
):
    # CUI-0017, and the companion to the test above: the same question asked of
    # the shape that one does not reach -- aliased `activity(id:)` parents
    # rather than aliased `activities`. What it pins is the sentence in
    # MAX_TRACK_FIELDS_PER_REQUEST's docstring that says the cap is not a bound
    # on the statements a request issues. A parent pays about two statements
    # for itself before its track is weighed at all, and nothing but
    # MAX_QUERY_TOKENS says how many parents a document may carry, so the
    # request-level ceiling is the two limits together and is the larger number.
    parents = _widest_parent_flood()
    assert parents == PARENT_FLOOD_WIDTH, "the token limit moved; so does the ceiling below"

    track_statements = MAX_TRACK_FIELDS_PER_REQUEST * SQL_PER_TRACK_BATCH
    ceiling = parents * SQL_PER_ACTIVITY_FIELD + track_statements

    # Reset before the first half as well as the second, so both halves read
    # the same way: each counts one request rather than a request plus whatever
    # came before it. What keeps the fixtures' own ~393 statements out of the
    # first half is `sql_count` depending on `year_client`, which builds the
    # client before the listener attaches whatever order these arguments are
    # in; this line no longer carries that on its own.
    sql_count[0] = 0
    assert gql(year_client, _distinct_parent_flood(parents)), "the widest flood is served whole"
    assert sql_count[0] > track_statements, "the cap alone would under-count this request"
    assert sql_count[0] <= ceiling

    # The parents are paid for whether their tracks are served or refused. At
    # MAX_TRACK_POINTS each the points budget turns away all but the first few
    # -- the document is the same width and the same token cost, so nothing
    # else can be doing the refusing -- and the parent half is charged anyway.
    sql_count[0] = 0
    body = gql_partial(year_client, _distinct_parent_flood(parents, MAX_TRACK_POINTS))
    messages = [e["message"] for e in body["errors"]]
    assert all(str(MAX_TRACK_POINTS_PER_REQUEST) in m for m in messages), messages
    assert sql_count[0] >= parents * SQL_PER_ACTIVITY_FIELD
    assert sql_count[0] <= ceiling


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


def _track_field(points: int) -> str:
    """One ``track`` field asking for *points* samples."""
    return f"track(points: {points}) {{ sec }}"


def test_a_field_refused_on_points_leaves_the_field_budget_untouched_too(year_client):
    # The other half of "neither counter is written back on a refusal": the
    # test above pins the points counter, and until now nothing pinned the
    # field one -- moving ``info.context[TRACK_FIELDS_KEY] = fields_left``
    # above the points check left the whole file green (W-021). So a field
    # refused on points must not have spent a field slot either: every slot
    # the served fields left over has to still be serviceable behind it.
    half = MAX_TRACK_POINTS // 2
    spend = (MAX_TRACK_POINTS_PER_REQUEST - half) // half
    slots_left = MAX_TRACK_FIELDS_PER_REQUEST - spend
    assert slots_left > 0, "the points spend must not exhaust the field budget by itself"
    # `track` is non-null, so a refused child nulls its whole parent. Keeping
    # the slots under one parent makes that all-or-nothing, and leaving the
    # boundary probe a separate root field makes it null only itself.
    cheap_tracks = " ".join(f"t{n}: {CHEAP_TRACK}" for n in range(slots_left))
    fields = (
        _page_field(spend, _track_field(half)),
        f'over: activity(id: "{TRACKED_ACTIVITY_ID}") {{ {_track_field(MAX_TRACK_POINTS)} }}',
        f'rest: activity(id: "{TRACKED_ACTIVITY_ID}") {{ {cheap_tracks} }}',
    )

    body = gql_partial(year_client, _document(*fields))
    data = body["data"]

    assert len(data["activities"]) == spend
    assert data["over"] is None, f"{MAX_TRACK_POINTS} points must not fit in the {half} left"
    # The gate: had the refusal charged a field, the last of these would be
    # refused and null its parent.
    assert data["rest"] is not None, f"the refusal must leave all {slots_left} field slots"
    assert [n for n in range(slots_left) if data["rest"][f"t{n}"] is None] == []
    messages = [e["message"] for e in body["errors"]]
    # Named, not counted: only the points budget may have refused anything
    # here, and a bare count cannot tell the two budgets apart.
    assert all(str(MAX_TRACK_POINTS_PER_REQUEST) in m for m in messages), messages

    # The boundary is exact, so the assertion above is not slack: one slot past
    # what the refusal left over is refused, and by the *field* budget.
    past = gql_partial(
        year_client,
        _document(*fields, f'past: activity(id: "{TRACKED_ACTIVITY_ID}") {{ {CHEAP_TRACK} }}'),
    )
    assert past["data"]["rest"] is not None, "the slots before the boundary are still served"
    assert past["data"]["past"] is None
    assert str(MAX_TRACK_FIELDS_PER_REQUEST) in " ".join(e["message"] for e in past["errors"])


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


# ── AU-047 W-014 / CUI-0027 W-027: every budget fails closed when never seeded ──
TRACK_QUERY = f'{{ activity(id: "{TRACKED_ACTIVITY_ID}") {{ track(points: 10) {{ sec }} }} }}'

UNSEEDED_DOCUMENTS = [
    pytest.param(TRACK_QUERY, ("activity", "track"), id="track"),
    pytest.param("{ activities { id } }", ("activities",), id="activities"),
    pytest.param("{ year { year } }", ("year",), id="year"),
]
"""One document per charge site _RequestBudgets seeds a budget for, and where its rows land.

``track`` reaches :func:`_charge_track_field`; ``activities`` and ``year`` reach
``_charge_list_rows``, which the extension's own docstring says "reads it the
same way". Until CUI-0027 W-027 only the track half had an assertion behind
that sentence, so a change to the seeding would turn ``track`` red while the
list half went quietly unbounded -- the exact state CUI-0027 fixed, restored
with nobody watching.

``year`` is listed beside ``activities`` because it reaches the same charge
from its own call site, on :data:`DEFAULT_PAGE_SIZE` rather than on a client
``limit``, so ``activities`` alone would not cover it.
"""

NOT_SEEDED = "not seeded"
"""Words from the unseeded-budget refusal, asserted rather than left to truthiness.

A bare ``assert result.errors`` cannot tell a budget refusal from an error that
has nothing to do with the budget. Measured against a mutant that reads the
budget as ``.get(KEY, MAX_...)`` instead: the frozen context below still comes
back with an error, but it is ``'mappingproxy' object does not support item
assignment``, raised where the charge writes the counter back. The refusal is
incidental to that context and does not generalise -- the same mutant, given a
plain ``dict``, serves the field unbounded. Naming the message is what tells
those two apart, on every document above.
"""


def _rows_at(result, path) -> list:
    """Rows *result* served under *path*, or ``[]`` where the refusal nulled it away."""
    node = result.data or {}
    for key in path:
        node = (node or {}).get(key) or {}
    return node or []


@pytest.mark.parametrize(("document", "path"), UNSEEDED_DOCUMENTS)
def test_a_budget_fails_closed_when_the_schema_has_no_budget_extension(
    year_session, document, path
):
    # A schema built without _RequestBudgets seeds nothing. Serving the field
    # anyway would mean an unbounded request, so it must refuse instead.
    unbounded = strawberry.Schema(query=Query, extensions=[])

    result = unbounded.execute_sync(document, context_value={"session": year_session})

    assert result.errors
    assert NOT_SEEDED in result.errors[0].message
    assert _rows_at(result, path) == []


@pytest.mark.parametrize(("document", "path"), UNSEEDED_DOCUMENTS)
def test_a_budget_fails_closed_when_the_context_cannot_be_seeded(year_session, document, path):
    # A read-only Mapping is not a MutableMapping, so _RequestBudgets skips it
    # -- but info.context["session"] still reads, so the field is reachable. The
    # invariant is that the *missing budget* is what refuses it. This context
    # happens to reject the write back as well, so it would error either way;
    # NOT_SEEDED is what pins the reason rather than the symptom.
    frozen = MappingProxyType({"session": year_session})

    result = schema.execute_sync(document, context_value=frozen)

    assert result.errors
    assert NOT_SEEDED in result.errors[0].message
    assert _rows_at(result, path) == []


# ── CUI-0029: a refusal the client earned is not logged as a server fault ──
REFUSED_DOCUMENTS = (
    pytest.param(
        "{{ {} }}".format(
            " ".join(f"{alias}: activities(limit: {MAX_PAGE_SIZE}) {{ id }}" for alias in "abcde")
        ),
        id="list-rows",
    ),
    pytest.param(
        _fan_out_query(YEAR_DAYS, MAX_TRACK_POINTS),
        id="track-points",
    ),
)
"""One document per charge site that a client can drive past its budget.

Both, not one: ``_charge_list_rows`` and ``_charge_track_field`` raise from two
different call sites, and CUI-0029 measured nine absolute-path frames on each.
A fix applied to only one of them would leave the other amplifying log volume
exactly as before, with the suite still green.
"""


def _strawberry_records(caplog):
    """The records Strawberry's execution logger emitted, refusals included."""
    return [record for record in caplog.records if record.name == "strawberry.execution"]


@pytest.mark.parametrize("document", REFUSED_DOCUMENTS)
def test_a_budget_refusal_is_logged_without_a_traceback(year_client, caplog, document):
    # The refusal is what the budget exists to produce, so it is not a fault.
    # Measured before the fix: one ERROR record per refused request carrying
    # exc_info, which renders as nine frames naming absolute source paths and
    # the interpreter's site-packages directory -- on a public, unauthenticated
    # endpoint where one refusal costs the client 998 tokens.
    #
    # exc_info, not the rendered text: `logging` only formats a traceback when
    # a handler asks it to, so asserting on formatted output would pass or fail
    # according to the handler pytest happens to install rather than according
    # to what the record carries into a deployment's own handlers.
    with caplog.at_level(logging.ERROR, logger="strawberry.execution"):
        assert gql_errors(year_client, document)

    records = _strawberry_records(caplog)
    assert records, "the refusal must still be logged -- silence is not the fix here"
    assert [record.exc_info for record in records] == [None] * len(records)


@pytest.mark.parametrize(("document", "path"), UNSEEDED_DOCUMENTS)
def test_an_unseeded_budget_still_logs_its_traceback(year_session, caplog, document, path):
    # The other half of the same line, and the reason the fix cannot simply
    # stop Strawberry logging exc_info. An unseeded budget means the schema was
    # built wrong or the context could not be written to: nobody's request
    # caused it and no client can act on it, so it is exactly the case a
    # traceback is for. Only the client-driven refusal loses one.
    unbounded = strawberry.Schema(query=Query, extensions=[])

    with caplog.at_level(logging.ERROR, logger="strawberry.execution"):
        result = unbounded.execute_sync(document, context_value={"session": year_session})

    assert NOT_SEEDED in result.errors[0].message
    records = _strawberry_records(caplog)
    assert records, "a misconfigured schema must not go unlogged"
    assert all(record.exc_info for record in records)
    # Named, not just truthy: `exc_info` would also be set if the refusal had
    # simply kept raising ValueError, which is the state this ticket removes.
    assert all(issubclass(record.exc_info[0], RuntimeError) for record in records)


@pytest.mark.parametrize("document", REFUSED_DOCUMENTS)
def test_a_budget_refusal_still_tells_the_client_where_it_happened(year_client, document):
    # `locations` and `path` are what make the refusal actionable: they name
    # the field in the client's own document that overspent. They survive only
    # because the refusal is raised carrying the resolver's nodes and path --
    # drop either and graphql-core rebuilds the error around the raised one,
    # which is precisely what puts the traceback back. So this is the assertion
    # that fails if the mechanism above is quietly undone.
    body = year_client.post(GRAPHQL_PATH, json={"query": document}).get_json()

    error = body["errors"][0]
    assert "budget exhausted" in error["message"]
    assert error["locations"], "a refusal with no location cannot be traced to a field"
    assert error["path"], "a refusal with no path cannot be traced to a field"


# ── AU-050: one statement per batch of tracks, not two per track ───────────
@pytest.fixture
def track_rows_loaded():
    """Count the ORM track points every session materialises while the fixture is alive."""
    loaded = [0]

    def tally(_session, obj):
        if isinstance(obj, models.TrackPoint):
            loaded[0] += 1

    # On the Session class, not an instance: create_app opens its own session
    # per request, so a test client gives no session to attach to.
    event.listen(Session, "loaded_as_persistent", tally)
    try:
        yield loaded
    finally:
        event.remove(Session, "loaded_as_persistent", tally)


@pytest.mark.parametrize("points", [1, 2, 3, 5, 7, 10, 37, 150, 599, 600, 1000])
def test_batched_tracks_match_the_reference_downsampler(varied_session, points):
    # The constraint the whole ticket hangs on. Every track in this batch is a
    # different length, so each one needs its own stride and its own rounding:
    # a batch that derived one stride for all of them, or that rounded in SQL
    # rather than in Python, would come back off by a row here and nowhere else.
    batched = service.tracks(varied_session, VARIED_IDS, points)

    for activity_id, stored in zip(VARIED_IDS, VARIED_TRACK_LENGTHS, strict=True):
        expected = downsample(list(range(stored)), points)
        got = [row["sec"] for row in batched[activity_id]]
        assert got == expected, f"{activity_id}: stored={stored} points={points}"


def test_a_batched_track_carries_the_same_columns_as_a_single_one(varied_session):
    points = 7
    batched = service.tracks(varied_session, VARIED_IDS, points)
    for activity_id in VARIED_IDS:
        assert batched[activity_id] == service.track(varied_session, activity_id, points)


def test_a_track_read_in_a_batch_matches_the_same_track_read_alone(varied_client):
    # Through GraphQL, so the two paths differ in batch width rather than only
    # in which service function was called: the page reads nine tracks in one
    # statement, ``activity(id:)`` reads one.
    selection = "track(points: 7) { sec lat lon elevationM distanceM speedMps cadence tempC }"
    page = gql(varied_client, _one_page(len(VARIED_IDS), selection))["activities"]

    for activity_id, row in zip(VARIED_IDS, page, strict=True):
        alone = gql(varied_client, f'{{ activity(id: "{activity_id}") {{ {selection} }} }}')
        assert row["track"] == alone["activity"]["track"], activity_id


def test_an_activity_without_a_stored_track_comes_back_empty_from_a_batch(varied_client):
    rows = gql(varied_client, _one_page(len(VARIED_IDS), "id track(points: 7) { sec }"))
    by_id = {row["id"]: row["track"] for row in rows["activities"]}

    empty = [i for i, stored in enumerate(VARIED_TRACK_LENGTHS) if stored == 0]
    assert empty, "the fixture must contain a trackless activity"
    for i in empty:
        assert by_id[VARIED_IDS[i]] == []
    # And the batch is not poisoned by them: the tracks beside them still come.
    assert any(track for track in by_id.values())


def test_asking_for_more_points_than_are_stored_returns_the_whole_track(varied_session):
    batched = service.tracks(varied_session, VARIED_IDS, MAX_TRACK_POINTS)
    for activity_id, stored in zip(VARIED_IDS, VARIED_TRACK_LENGTHS, strict=True):
        assert len(batched[activity_id]) == min(stored, MAX_TRACK_POINTS)


@pytest.mark.parametrize("activities", [1, 2, 8, 32, MAX_TRACK_FIELDS_PER_REQUEST])
def test_a_page_of_tracks_costs_the_same_statements_however_wide_it_is(
    year_client, sql_count, activities
):
    # AU-050 in one assertion: the statement count is flat in the width of the
    # fan-out. Before batching this read SQL_PER_LIST_FIELD + 2 x activities.
    served = gql(year_client, _cheap_tracks(activities))["activities"]

    assert len(served) == activities
    assert sql_count[0] == SQL_PER_LIST_FIELD + SQL_PER_TRACK_BATCH


def test_a_batch_is_keyed_by_points_as_well_as_by_activity(year_client, sql_count):
    # Two samples of one track are two different answers, so they cannot share
    # a cached read. Same activity, different `points`: one batch each, and
    # the rows that come back differ accordingly.
    asks = (3, 5, 9)
    fields = " ".join(f"p{n}: track(points: {n}) {{ sec }}" for n in asks)
    served = gql(year_client, _document(f'activity(id: "{TRACKED_ACTIVITY_ID}") {{ {fields} }}'))

    activity = served["activity"]
    assert [len(activity[f"p{n}"]) for n in asks] == list(asks)
    assert sql_count[0] == SQL_PER_ACTIVITY_FIELD + len(asks) * SQL_PER_TRACK_BATCH


PREFETCH_DB_ACTIVITIES = 2 * (MAX_TRACK_POINTS_PER_REQUEST // MAX_TRACK_POINTS)
"""Activities in the prefetch fixture: twice as many as a full-track page can afford."""

PREFETCH_DB_STORED = 12
"""Track rows each of those activities stores. Small, so the count below is exact."""


@pytest.fixture
def prefetch_client(tmp_path):
    path = tmp_path / "prefetch.db"
    _write_track_db(path, [PREFETCH_DB_STORED] * PREFETCH_DB_ACTIVITIES, prefix="p")
    app = create_app(path, graphiql=False)
    app.testing = True
    return app.test_client()


def test_the_batch_never_reads_further_than_the_points_budget_reaches(
    prefetch_client, track_rows_loaded
):
    # A batch is a prefetch: it reads tracks for fields whose charge has not
    # been levied yet. If it read the whole page it would materialise rows the
    # budgets exist to refuse -- which is the entire point of charging before
    # the SQL goes out. So it stops where the budget does, and this is where
    # the two are held against each other.
    affordable = MAX_TRACK_POINTS_PER_REQUEST // MAX_TRACK_POINTS
    assert affordable < PREFETCH_DB_ACTIVITIES, "the budget must be the binding limit here"

    body = gql_partial(prefetch_client, _fan_out_query(PREFETCH_DB_ACTIVITIES, MAX_TRACK_POINTS))

    assert str(MAX_TRACK_POINTS_PER_REQUEST) in " ".join(e["message"] for e in body["errors"])
    # Every activity here stores a track, so a batch that read the whole page
    # would land PREFETCH_DB_ACTIVITIES x PREFETCH_DB_STORED rows in memory on
    # a request that is refused. It reads the affordable prefix and no more.
    assert track_rows_loaded[0] == affordable * PREFETCH_DB_STORED


def test_the_batch_never_reads_further_than_the_field_budget_reaches(year_client, sql_count):
    # The other budget, on a page one wider than the field cap. `track` is a
    # non-null list, so refusing one nulls the page around it -- what is left
    # to check is that the batch stopped at the cap rather than reading the
    # 65th, and that it still cost the flat two statements.
    body = gql_partial(year_client, _cheap_tracks(MAX_TRACK_FIELDS_PER_REQUEST + 1))

    assert str(MAX_TRACK_FIELDS_PER_REQUEST) in " ".join(e["message"] for e in body["errors"])
    assert sql_count[0] == SQL_PER_LIST_FIELD + SQL_PER_TRACK_BATCH


# ── AU-050: batching moved the cost from round trips to bound parameters ───
SQLITE_BOUND_PARAMETER_CEILING = 32766
"""SQLite's default ``SQLITE_MAX_VARIABLE_NUMBER`` since 3.32 (2020).

Deliberately the compiled-in default rather than this machine's reading, which
is higher: the number that matters is the one the smallest build the API might
run against enforces, and a statement over it fails outright with "too many SQL
variables" rather than degrading.
"""


def batch_parameters(activities: int, points: int) -> int:
    """Bound parameters the batch SELECT carries for *activities* sampled tracks.

    Four terms. Three of them are small, and W-020 is why they are written out
    rather than waved at: the sample list is the big one, but it is not the
    only one, and the shape that binds the most is decided by the small ones.

    - ``activities * points`` sample keys, one per sample;
    - ``activities``: every track binds its id once, in the numbered
      subquery's ``IN``. Before CUI-0019 it bound it a second time, in the
      ``==`` arm ``_sample_filter`` gave each track; the id now travels inside
      the sample keys already counted above;
    - ``+ 1`` for the ``- 1`` in ``row_number() OVER (...) - 1``, which
      SQLAlchemy binds as a parameter rather than inlining;
    - ``+ 1`` for ``SAMPLE_KEY_SEPARATOR`` in the key expression, bound once
      for the whole statement however wide the batch is.

    A track taken *whole* is cheaper than this -- it binds no sample keys and
    joins one shared ``IN`` list -- so an all-sampled batch is the expensive
    shape, which is what both fixtures below build.
    """
    return activities * points + activities + 2


POINTS_BUDGET_BATCH_ACTIVITIES = 50
"""Activities in the batch that spends the *points* budget exactly.

Chosen with POINTS_BUDGET_BATCH_POINTS so their product is exactly
MAX_TRACK_POINTS_PER_REQUEST: the most sample keys the points budget can let
one batch bind.

Since CUI-0019 that also makes it the most *parameters*, which is a ranking
this fixture has now held, lost and regained, so it is named for its shape and
not for its rank. Both it and the field-cap shape below are measured, and
``test_no_batch_the_budgets_allow_binds_more_parameters_than_the_maximum``
is what decides between them rather than either docstring.
"""

POINTS_BUDGET_BATCH_POINTS = MAX_TRACK_POINTS_PER_REQUEST // POINTS_BUDGET_BATCH_ACTIVITIES
"""Samples per track in that fixture, so the batch spends the budget exactly."""

FIELD_CAP_BATCH_ACTIVITIES = MAX_TRACK_FIELDS_PER_REQUEST
"""Activities in the batch that spends the *field* cap instead.

Fewer sample keys than the shape above -- 64 x 156 is 9,984 against 50 x 200's
10,000 -- but more tracks, and every track costs a parameter of its own beyond
its keys. While that per-track cost was 2 (an id in the subquery's ``IN`` and
an id in the track's own ``==`` arm) the 14 extra tracks outweighed the 16
missing keys and this was the parameter maximum, which is what W-020 found.
CUI-0019 removed the ``==`` arm, halving the per-track term to 1, and the
ranking went back the other way: 10,050 here against 10,052 above.

Two parameters apart is close enough that nothing should rest on which side
wins, so both shapes stay measured.
"""

FIELD_CAP_BATCH_POINTS = MAX_TRACK_POINTS_PER_REQUEST // FIELD_CAP_BATCH_ACTIVITIES
"""Samples per track there: as many as the points budget affords across the field cap."""


@pytest.fixture
def batch_session(tmp_path):
    """Return a factory opening a session on a database of *activities* tracks."""
    with ExitStack() as scopes:

        def build(activities: int, points: int) -> Session:
            path = tmp_path / f"batch-{activities}x{points}.db"
            # One row more than the ask, so every track is sampled rather than
            # taken whole: an unsampled track binds its id and no positions.
            _write_track_db(path, [points + 1] * activities, prefix="w")
            return scopes.enter_context(db.session_scope(db.make_engine(path)))

        yield build


@pytest.fixture
def sql_params():
    """Record how many bound parameters each statement carries."""
    counts: list[int] = []

    def tally(_conn, _cursor, _statement, parameters, *_args, **_kwargs):
        counts.append(len(parameters or ()))

    event.listen(Engine, "before_cursor_execute", tally)
    try:
        yield counts
    finally:
        event.remove(Engine, "before_cursor_execute", tally)


@pytest.mark.parametrize(
    "activities,points,expected",
    [
        pytest.param(
            POINTS_BUDGET_BATCH_ACTIVITIES,
            POINTS_BUDGET_BATCH_POINTS,
            10052,
            id="spends-the-points-budget",
        ),
        pytest.param(
            FIELD_CAP_BATCH_ACTIVITIES, FIELD_CAP_BATCH_POINTS, 10050, id="spends-the-field-cap"
        ),
    ],
)
def test_the_widest_batch_stays_under_sqlites_bound_parameter_ceiling(
    batch_session, sql_params, activities, points, expected
):
    # What batching trades away. Every sample the batch wants is named by a key
    # of its own, so the parameters that used to be spread over 2N statements
    # now arrive in one -- and a statement over the ceiling does not run slowly,
    # it raises. The points budget is what holds the total down, which is why
    # AU-050 must not be read as a reason to raise it: at 10,000 points this
    # sits inside the ceiling with room, and it scales one for one.
    #
    # Both shapes, because the budgets bound two different things and the
    # parameter maximum is not always the same one of them: which shape wins
    # has already moved once with W-020 and back again with CUI-0019, so
    # neither is left unmeasured.
    assert activities <= MAX_TRACK_FIELDS_PER_REQUEST
    assert points <= MAX_TRACK_POINTS
    assert activities * points <= MAX_TRACK_POINTS_PER_REQUEST, "the points budget must allow it"
    session = batch_session(activities, points)
    # The build above runs on the same Engine class the tally listens to, so
    # its inserts have to go before the batch is measured.
    sql_params.clear()

    batched = service.tracks(session, [f"w{i}" for i in range(activities)], points)

    assert all(len(rows) == points for rows in batched.values())
    widest = max(sql_params)
    # Pinned exactly, not just bounded: the composition is what W-020 got
    # wrong, so a term appearing or disappearing has to turn this red rather
    # than be absorbed by an inequality.
    assert widest == batch_parameters(activities, points) == expected
    # Not vacuous: the SELECT really does bind a key per sample, so this would
    # have caught an IN list that grew past what the budget allows.
    assert widest > MAX_TRACK_POINTS_PER_REQUEST
    assert widest <= MAX_TRACK_POINTS_PER_REQUEST + MAX_TRACK_FIELDS_PER_REQUEST + 2
    assert widest < SQLITE_BOUND_PARAMETER_CEILING


def test_no_batch_the_budgets_allow_binds_more_parameters_than_the_maximum(sql_params):
    # Which of the two measured shapes is the maximum, as a gate rather than as
    # prose: of every (activities, points) the three caps admit, none binds
    # more than the points-budget one the test above runs. Arithmetic rather
    # than 64 more databases -- batch_parameters is what the measured cases pin
    # it against, and they pin both sides of a 2-parameter gap.
    maximum = batch_parameters(POINTS_BUDGET_BATCH_ACTIVITIES, POINTS_BUDGET_BATCH_POINTS)
    allowed = [
        (activities, min(MAX_TRACK_POINTS, MAX_TRACK_POINTS_PER_REQUEST // activities))
        for activities in range(1, MAX_TRACK_FIELDS_PER_REQUEST + 1)
    ]

    assert max(batch_parameters(a, p) for a, p in allowed) == maximum
    assert batch_parameters(FIELD_CAP_BATCH_ACTIVITIES, FIELD_CAP_BATCH_POINTS) < maximum, (
        "the field-cap fixture must not be mistaken for the maximum again"
    )
    # The headroom the safety conclusion rests on, stated where it can go red:
    # the ceiling is over three times the widest batch the budgets can build.
    assert maximum * 3 < SQLITE_BOUND_PARAMETER_CEILING


# ── CUI-0019: what the batch predicate costs as the batch widens ───────────
BATCH_COST_PREFIX = "c"
"""Activity id prefix for the fixture below, kept clear of ``w`` and ``r``."""

BATCH_COST_STORED = 60
"""Track rows each activity in that fixture stores.

Any number above the ``points`` the reads below ask for would do: what has to
hold is that every track is *sampled* rather than taken whole, since a whole
track joins one shared ``IN`` list and contributes no arm of its own.
"""

BATCH_COST_WHOLE = 1
"""Track rows the *unsampled* half of the mixed fixture stores.

At or under the ``points`` those reads ask for, so those tracks come back
whole and join the one shared ``activity_id IN (...)`` arm instead of being
sampled.
"""

BATCH_COST_NARROW = 8
"""The narrow batch the wide one is compared against.

An eighth of MAX_TRACK_FIELDS_PER_REQUEST, so the widths differ by enough for
the growth to show over the fixed per-statement cost that dilutes it.
"""

BATCH_COST_SAMPLED_ARMS = 1
"""Arms the predicate of an all-sampled batch carries, at any width.

One ``IN`` over the composite sample key, however many tracks the batch holds.
This is the assertion CUI-0019 turns on: the AU-050 shape put one ``AND`` arm
per sampled track here, and SQLite evaluated the whole disjunction against
every row the numbered subquery scanned, so the work grew with batch width
times batch rows. A count that tracks the width again is that bug returning,
whatever the timings say.
"""

BATCH_COST_MIXED_ARMS = 2
"""Arms when the batch also holds tracks short enough to come back whole.

The sample key ``IN`` plus the one shared ``activity_id IN (...)`` those
tracks join -- pinned separately from BATCH_COST_SAMPLED_ARMS so that "fixed"
is read as fixed by construction rather than as "the fixture happens to build
one arm". Two is the most this predicate can ever carry.
"""

BATCH_COST_UNSAMPLED_ARMS = 1
"""Arms the whole-track control's statement carries, at any width.

The same number as BATCH_COST_SAMPLED_ARMS and *not* the same claim, which is
why it is pinned separately rather than borrowed: ``points=None`` never
reaches ``_sample_filter`` at all, so what this says is that the statement
carries no disjunction whatever -- not that the sample predicate is down to a
single ``IN``. The two could move apart without either being wrong.
"""

BATCH_COST_FLAT_CEILING = 1.1
"""How far a per-track reading is allowed to move between the two widths.

Both reads below are flat: the sampled one since CUI-0019, the whole-track
control always. The wide batch in fact measures a little *under* the narrow
one per track, because the fixed per-statement cost is spread over eight times
the tracks, so this ceiling is one-sided room for a SQLite build that emits a
different number of opcodes for the same plan rather than a measured spread.
The control is still here to keep the sampled reading from being read as "wide
batches touch more rows": it touches sixty times as many and does not move.
"""


def _or_arms(statement: str) -> int:
    """Return the number of arms in *statement*'s top-level disjunction."""
    return len(re.findall(r"\bOR\b", statement)) + 1


def _batch_read(engine, width: int, points: int | None) -> tuple[int, str]:
    """Return (VDBE steps, SELECT text) for one batch read of *width* tracks.

    Steps rather than seconds. SQLite's progress callback fires once per
    virtual-machine instruction, so what comes back is a count of work done --
    deterministic for a given build and fixture, where a wall time is not.
    This repo does not assert wall times anywhere and should not start: a
    second of CI is not a property of the code.
    """
    statements: list[str] = []
    steps = [0]

    def capture(_conn, _cursor, statement, *_args, **_kwargs):
        statements.append(statement)

    def tick():
        steps[0] += 1
        return 0

    with db.session_scope(engine) as session:
        raw = session.connection().connection.dbapi_connection
        event.listen(Engine, "before_cursor_execute", capture)
        raw.set_progress_handler(tick, 1)
        try:
            service.tracks(session, [f"{BATCH_COST_PREFIX}{i}" for i in range(width)], points)
        finally:
            raw.set_progress_handler(None, 1)
            event.remove(Engine, "before_cursor_execute", capture)
    return steps[0], statements[-1]


@pytest.fixture
def batch_cost_engine(tmp_path):
    """A database of MAX_TRACK_FIELDS_PER_REQUEST equally long tracks."""
    path = tmp_path / "batch-cost.db"
    _write_track_db(
        path,
        [BATCH_COST_STORED] * MAX_TRACK_FIELDS_PER_REQUEST,
        prefix=BATCH_COST_PREFIX,
    )
    engine = db.make_engine(path)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def mixed_batch_cost_engine(tmp_path):
    """The same widths, but every other track short enough to come back whole."""
    path = tmp_path / "batch-cost-mixed.db"
    lengths = [
        BATCH_COST_STORED if i % 2 else BATCH_COST_WHOLE
        for i in range(MAX_TRACK_FIELDS_PER_REQUEST)
    ]
    _write_track_db(path, lengths, prefix=BATCH_COST_PREFIX)
    engine = db.make_engine(path)
    try:
        yield engine
    finally:
        engine.dispose()


def test_the_batch_predicate_does_not_widen_with_the_batch(batch_cost_engine):
    # CUI-0019 as a gate. AU-050 bought its two statements with a predicate
    # carrying one OR arm per sampled track, and SQLite evaluated the whole
    # disjunction against every row the numbered subquery scanned -- so the
    # work grew with batch width times batch rows, and the cap on that width
    # became the wrong number to raise. QA measured it as wall clock on the
    # real export; wall clock in CI buys a flaky test, so what runs here is the
    # shape of the predicate and the work SQLite does.
    narrow_steps, narrow_sql = _batch_read(batch_cost_engine, BATCH_COST_NARROW, 1)
    wide_steps, wide_sql = _batch_read(batch_cost_engine, MAX_TRACK_FIELDS_PER_REQUEST, 1)

    # The cause, pinned on its own so a failure below says which half moved:
    # the predicate no longer knows how wide the batch is.
    assert _or_arms(narrow_sql) == BATCH_COST_SAMPLED_ARMS
    assert _or_arms(wide_sql) == BATCH_COST_SAMPLED_ARMS

    # And the effect: eight times the tracks costs about eight times the work,
    # not sixty-four. Per track, so what is compared is the slope.
    narrow_per_track = narrow_steps / BATCH_COST_NARROW
    wide_per_track = wide_steps / MAX_TRACK_FIELDS_PER_REQUEST
    assert wide_per_track < BATCH_COST_FLAT_CEILING * narrow_per_track, (
        "a track in a full batch has started costing more than one in a narrow "
        "batch again -- the sample predicate has gone back to growing with the "
        "width of the batch (CUI-0019)"
    )

    # The control: same fixture, same widths, sixty times the rows returned,
    # and flat before CUI-0019 as well. So what the reading above measures is
    # the predicate, not the batch's row count.
    flat_narrow, flat_narrow_sql = _batch_read(batch_cost_engine, BATCH_COST_NARROW, None)
    flat_wide, flat_wide_sql = _batch_read(batch_cost_engine, MAX_TRACK_FIELDS_PER_REQUEST, None)

    assert _or_arms(flat_narrow_sql) == _or_arms(flat_wide_sql) == BATCH_COST_UNSAMPLED_ARMS
    assert flat_wide / MAX_TRACK_FIELDS_PER_REQUEST < BATCH_COST_FLAT_CEILING * (
        flat_narrow / BATCH_COST_NARROW
    ), "the unsampled path is the flat one this test is calibrated against"


def test_a_batch_mixing_sampled_and_whole_tracks_stays_at_two_arms(mixed_batch_cost_engine):
    # The other half of "fixed by construction": the whole-track tracks join
    # one shared IN rather than bringing arms of their own, so the widest
    # mixture the cap admits carries the same two arms as the narrowest.
    narrow_steps, narrow_sql = _batch_read(mixed_batch_cost_engine, BATCH_COST_NARROW, 1)
    wide_steps, wide_sql = _batch_read(mixed_batch_cost_engine, MAX_TRACK_FIELDS_PER_REQUEST, 1)

    assert _or_arms(narrow_sql) == BATCH_COST_MIXED_ARMS
    assert _or_arms(wide_sql) == BATCH_COST_MIXED_ARMS
    assert wide_steps / MAX_TRACK_FIELDS_PER_REQUEST < BATCH_COST_FLAT_CEILING * (
        narrow_steps / BATCH_COST_NARROW
    )


# ── CUI-0027: a per-request bound on the rows a fan-out reads ──────────────
LIST_FLOOD_WIDTH = 166
"""Aliased ``activities`` fields MAX_QUERY_TOKENS admits, taking no ``track``.

998 of the 1000 tokens, and the shape CUI-0027 was filed as: 60,590 rows and
3.1 s on the real export, the slowest legal document measured for that ticket
bar an equally wide flood of ``year``. Written down so the tests below can be
read, but derived rather than trusted -- see ``_widest_list_flood``.
"""

LIST_FLOOD_TOKENS = 998
"""Tokens that flood lexes to, two short of MAX_QUERY_TOKENS.

The half of the shape that used to live in DOCUMENTED_WORST_CASES and is still
worth pinning: the parser is not what refuses this document, so a token limit
that quietly crept down to meet it would take the row budget's test with it.
"""

ROW_FLOOD_NARROW = 8
"""The narrow fan-out the wide one is compared against, in fields."""

ROW_FLOOD_WIDE = 64
"""The wide fan-out: eight times the narrow one, and still inside the budget.

An eighth and a whole, the same ratio ``BATCH_COST_NARROW`` takes against
MAX_TRACK_FIELDS_PER_REQUEST, so the widths differ by enough for growth to show
over the fixed per-statement cost that dilutes it.
"""

ROW_FLOOD_LIMIT = MAX_LIST_ROWS_PER_REQUEST // ROW_FLOOD_WIDE
"""Rows each field of those floods asks for, so the wide one just fits the budget."""

ROW_FLOOD_FLAT_CEILING = 1.1
"""How far a per-field reading may move between the two widths.

One-sided room for a SQLite build that emits a different number of opcodes for
the same plan, not a measured spread -- the same allowance and the same reason
as ``BATCH_COST_FLAT_CEILING``. Nothing about a page read grows with how many
*other* pages the document opens, so the true figure is flat.
"""

CLIENT_LIST_DOCUMENTS = (
    # Every document in frontend/src/data/api/queries.ts that spends this
    # budget, reduced to the field that spends it. Each names no window, so
    # each charges one DEFAULT_PAGE_SIZE page -- which is the whole of the
    # claim that MAX_LIST_ROWS_PER_REQUEST costs the front end nothing.
    ("ActivitiesQuery", "{ activities { id } }"),
    ("WeightQuery", "{ weight { date } }"),
    ("WeatherQuery", "{ weather { date } }"),
    ("WarningsQuery", "{ warnings { date } }"),
    ("YearQuery", "{ year { year } }"),
)
"""(name, reduced document) for each front-end query that pays the row budget."""


@pytest.fixture
def activity_rows_loaded():
    """Count the ORM activity rows every session materialises while the fixture is alive."""
    loaded = [0]

    def tally(_session, obj):
        if isinstance(obj, models.Activity):
            loaded[0] += 1

    # On the Session class, not an instance: create_app opens its own session
    # per request, so a test client gives no session to attach to.
    event.listen(Session, "loaded_as_persistent", tally)
    try:
        yield loaded
    finally:
        event.remove(Session, "loaded_as_persistent", tally)


def _widest_list_flood() -> int:
    """The most aliased ``activities`` fields MAX_QUERY_TOKENS lets through."""
    fields = 1
    while _token_count(_list_flood(fields + 1)) <= MAX_QUERY_TOKENS:
        fields += 1
    return fields


def _windowed_list_flood(fields: int, limit: int) -> str:
    """*fields* aliased ``activities``, each asking for *limit* rows and no track."""
    return _document(" ".join(f"a{n}: activities(limit: {limit}) {{ id }}" for n in range(fields)))


def test_the_widest_list_fan_out_reads_no_more_rows_than_the_budget(
    year_client, activity_rows_loaded
):
    # The shape CUI-0027 was filed as: 166 aliased `activities`, no `track`
    # anywhere, so neither track budget is spent and nothing used to bound it.
    # What it costs is the rows it returns -- 166 pages of a whole year, 60,590
    # of them, 3.1 s on the real export -- so that is what has to be bounded.
    # Rows materialised rather than wall clock, for the reason `_batch_read`
    # gives: a count is a property of the code where a second of CI is not.
    widest = _widest_list_flood()
    assert widest == LIST_FLOOD_WIDTH, "the token limit moved; so does this shape"
    assert _token_count(_list_flood(widest)) == LIST_FLOOD_TOKENS

    year_client.post(GRAPHQL_PATH, json={"query": _list_flood(widest), "variables": {}})

    # `<=` with about 27% of slack on this fixture, deliberately (CUI-0027
    # S-054). `year_db` holds 365 activities, so the eight pages the budget
    # affords return 2,920 rows against the 4,000 they are charged, and this
    # assertion is really 2920 <= 4000. Tightening it to that exact figure
    # would pin the test to the fixture's size without buying protection: what
    # it would catch is an undercharge, and `test_the_row_budget_is_the_boundary`
    # already catches that exactly, by serving at the budget and refusing at
    # budget + 1. Measured against a charge mutated to 75% of `limit`: that
    # test fails, along with three others, while an exact count here would be
    # the fourth rather than the only one. What this test is for is the
    # question none of those answer -- whether anything bounds the shape at all
    # -- and for that `<=` is the honest assertion. The 60,590 rows it read
    # before the budget existed are 15x the bound, not 1.3x.
    assert activity_rows_loaded[0] <= MAX_LIST_ROWS_PER_REQUEST, (
        f"{widest} aliased list fields read {activity_rows_loaded[0]} rows; "
        f"nothing is bounding the fan-out"
    )


def test_the_widest_list_fan_out_is_refused_after_the_budget(year_client, sql_count):
    # The same document from the other side: what it costs before it is turned
    # away. An error does not unwind the queries already sent, so what the
    # budget has to hold is the served prefix -- which is the whole reason the
    # charge lands before the SQL rather than after it.
    affordable = MAX_LIST_ROWS_PER_REQUEST // DEFAULT_PAGE_SIZE
    assert affordable < _widest_list_flood(), "the budget must be the binding limit here"

    sql_count[0] = 0
    body = gql_partial(year_client, _list_flood(_widest_list_flood()))

    assert str(MAX_LIST_ROWS_PER_REQUEST) in " ".join(e["message"] for e in body["errors"])
    assert sql_count[0] == affordable * SQL_PER_LIST_FIELD
    # A list field is a non-null type, so the refusal propagates to the root
    # and nulls the whole response rather than that one alias: 166 pages asked
    # for, eight paid for, nothing returned. The client gets less than it would
    # by asking for eight, which is the shape of every over-budget request here
    # and is why `data` cannot be read for which aliases survived.
    assert body["data"] is None


def test_the_row_budget_is_the_boundary(year_client):
    # Charged on the window asked for, not on the rows that come back, so the
    # refusal lands before the SQL does. `year_db` holds 365 activities, so
    # every page here returns fewer rows than it is charged for -- which is the
    # point: an exact charge could only be levied after the query.
    fits = _windowed_list_flood(ROW_FLOOD_WIDE, ROW_FLOOD_LIMIT)
    assert len(gql(year_client, fits)) == ROW_FLOOD_WIDE

    over = _windowed_list_flood(ROW_FLOOD_WIDE + 1, ROW_FLOOD_LIMIT)
    assert _token_count(over) <= MAX_QUERY_TOKENS, "the parser must not be the one refusing"
    message = gql_errors(year_client, over)
    assert "row" in message.lower()
    assert str(MAX_LIST_ROWS_PER_REQUEST) in message


def test_a_refused_page_stops_the_operation_where_it_stands(year_client, sql_count):
    # What follows from that non-null propagation, and the reason the two track
    # budgets' sticky/not-sticky distinction has no counterpart here: the first
    # refusal ends the operation, so a cheap page queued behind an over-large
    # one is never reached whatever the counter was left holding. Spend all but
    # `left` rows, then ask for one row too many, then for one that would fit.
    spent = _windowed_list_flood(ROW_FLOOD_WIDE, ROW_FLOOD_LIMIT - 1)
    left = MAX_LIST_ROWS_PER_REQUEST - ROW_FLOOD_WIDE * (ROW_FLOOD_LIMIT - 1)
    document = _document(
        spent[2:-2],
        f"over: activities(limit: {left + 1}) {{ id }}",
        f"under: activities(limit: {left}) {{ id }}",
    )

    sql_count[0] = 0
    body = gql_partial(year_client, document)

    assert [e["path"] for e in body["errors"]] == [["over"]]
    assert str(MAX_LIST_ROWS_PER_REQUEST) in body["errors"][0]["message"]
    # The pages before `over` are paid for; `over` issues nothing, because it
    # is refused before its SQL; `under` is never resolved at all.
    assert sql_count[0] == ROW_FLOOD_WIDE * SQL_PER_LIST_FIELD


def test_the_row_budget_is_per_request_and_does_not_leak_across_requests(year_client):
    # Two back-to-back requests that each spend the whole budget. Module-level
    # mutable state would starve the second one.
    query = _windowed_list_flood(ROW_FLOOD_WIDE, ROW_FLOOD_LIMIT)
    for _ in range(2):
        assert len(gql(year_client, query)) == ROW_FLOOD_WIDE


def test_year_pays_a_page_of_the_row_budget(year_client):
    # `year` takes no `limit` and opens no window, but it reads a whole
    # calendar year of activities: 166 aliased `year` fields were the *slowest*
    # document measured for CUI-0027, ahead even of the list flood it was filed
    # as. Charging it one default page is what brings it inside the same bound.
    affordable = MAX_LIST_ROWS_PER_REQUEST // DEFAULT_PAGE_SIZE
    fits = _document(*(f"y{n}: year {{ year }}" for n in range(affordable)))
    assert len(gql(year_client, fits)) == affordable

    over = _document(*(f"y{n}: year {{ year }}" for n in range(affordable + 1)))
    assert _token_count(over) <= MAX_QUERY_TOKENS, "the parser must not be the one refusing"
    assert str(MAX_LIST_ROWS_PER_REQUEST) in gql_errors(year_client, over)


@pytest.mark.parametrize(
    ("name", "document"), CLIENT_LIST_DOCUMENTS, ids=[c[0] for c in CLIENT_LIST_DOCUMENTS]
)
def test_every_document_the_front_end_sends_is_inside_the_row_budget(
    name, document, year_client, activity_rows_loaded
):
    # The claim the constant's docstring rests on: this budget costs today's
    # clients nothing. Each of these charges one DEFAULT_PAGE_SIZE page, which
    # is an eighth of it, so all five could be sent in one request and still be
    # served -- and that is asserted rather than argued, below.
    assert gql(year_client, document), f"{name} must still be served whole"
    assert activity_rows_loaded[0] <= DEFAULT_PAGE_SIZE


def test_the_whole_front_end_in_one_request_is_still_inside_the_row_budget(year_client):
    every = _document(*(document[2:-2] for _, document in CLIENT_LIST_DOCUMENTS))
    assert len(gql(year_client, every)) == len(CLIENT_LIST_DOCUMENTS)


def test_every_list_field_at_the_maximum_page_is_still_inside_the_row_budget(year_client):
    # The other end of "8x above real demand": all four list fields taken at
    # MAX_PAGE_SIZE in one request is 4,000 rows exactly, and is served. A
    # budget below that would make paging at the documented maximum a thing a
    # client could only do one field at a time.
    lists = ("activities", "weight", "weather", "warnings")
    assert len(lists) * MAX_PAGE_SIZE == MAX_LIST_ROWS_PER_REQUEST

    every = _document(*(f"{field}(limit: {MAX_PAGE_SIZE}) {{ date }}" for field in lists))
    assert len(gql(year_client, every)) == len(lists)


def _flood_steps(session, document: str) -> int:
    """Return the VDBE steps *document* costs, executed against *session*.

    Steps rather than seconds, for the reason ``_batch_read`` gives: SQLite's
    progress callback fires once per virtual-machine instruction, so what comes
    back is a count of work done -- deterministic for a given build and fixture
    where a wall time is not.
    """
    steps = [0]

    def tick():
        steps[0] += 1
        return 0

    raw = session.connection().connection.dbapi_connection
    raw.set_progress_handler(tick, 1)
    try:
        result = schema.execute_sync(document, context_value={"session": session})
    finally:
        raw.set_progress_handler(None, 1)
    assert not result.errors, result.errors
    return steps[0]


def test_the_fan_out_cost_does_not_widen_with_the_document(year_session):
    # The CUI-0019 lesson, asked of this half. That ticket's cause was AU-050
    # buying a flat statement count with a predicate that grew with the batch,
    # so the work went up with the square of the width while the count stayed
    # put -- a statement count alone could not see it. The same trap is open
    # here: the ticket's second suggested route was to batch the list fields
    # too. Whatever bounds this fan-out, eight times the fields must cost about
    # eight times the work and not sixty-four, so what is compared is the slope,
    # in steps rather than seconds.
    narrow = _flood_steps(year_session, _windowed_list_flood(ROW_FLOOD_NARROW, ROW_FLOOD_LIMIT))
    wide = _flood_steps(year_session, _windowed_list_flood(ROW_FLOOD_WIDE, ROW_FLOOD_LIMIT))

    assert wide / ROW_FLOOD_WIDE < ROW_FLOOD_FLAT_CEILING * (narrow / ROW_FLOOD_NARROW), (
        "a field in a wide fan-out has started costing more than one in a "
        "narrow fan-out: the list path has picked up a term that grows with "
        "the width of the document (CUI-0019 in the other half)"
    )


COLLIDING_IDS = ("1", "01", "5", "05", "50", "500")
"""Activity ids that are pure decimal and, unlike every other fixture here, not one width.

This is the shape the rest of the suite has no fixture for, and the reason
:data:`~run365days.api.service.SAMPLE_KEY_SEPARATOR` could be set to a decimal
digit or dropped entirely without a single test going red (CUI-0028). The other
fixtures number their activities ``r0`` and ``v0``..``v8``: a non-decimal prefix
at a fixed width, so ``position + separator + id`` stays unambiguous whatever
sits between the halves. Production ids are pure decimal but ten characters
wide, and a fixed width is injective for the same reason. Neither shape can
make the separator carry anything.

Variable width is what makes it load-bearing. ``position=10`` on id ``"5"`` and
``position=1`` on id ``"05"`` are one character apart in the key and are told
apart only by the separator: at ``":"`` they build ``"10:5"`` and ``"1:05"``, at
a decimal digit or at ``""`` they build the same string. The ``IN`` in
:func:`~run365days.api.service._sample_filter` compares keys whole, so a
collision does not raise -- the loser's rows simply come back as well, and the
track is thinned to the wrong rows in silence. That pair is live in this
fixture: ``"05"`` comes back with 10 rows instead of 7.

These ids are not the ones CUI-0028 was filed with, and the reason is coverage
rather than correctness: the ticket's own ``("1","01","10","2","20","002")``
collides too, ``"01"`` coming back with 10 rows instead of 7 under both ``"0"``
and ``""``, exactly as the ticket recorded. An execution note claiming that
shape did not collide was wrong and has been withdrawn (W-029). What this
fixture adds over it is the degenerate 0- and 1-row tracks and a different
length per id, so the batch exercises the whole-track arm and the sampled arm
at once.
"""

COLLIDING_TRACK_LENGTHS = (0, 1, 60, 101, 121, 201)
"""Rows stored per id above, a different length each, keeping the degenerate 0 and 1."""


def _write_id_track_db(path: Path, ids, lengths) -> None:
    """A track database whose activity ids are given rather than derived from the index.

    :func:`_write_track_db` names its activities ``prefix + index``, which can
    only ever produce one width of id and a non-decimal prefix. This one takes
    the ids, which is what :data:`COLLIDING_IDS` needs.
    """
    engine = create_engine(sqlite_url(path))
    models.Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                models.Meta(key="year", value="2021"),
                models.Meta(key="generated_at", value="2026-01-01T00:00:00"),
            ]
        )
        for index, (activity_id, stored) in enumerate(zip(ids, lengths, strict=True)):
            activity = _activity_row(index, stored, has_gps=stored > 0)
            activity.id = activity_id
            session.add(activity)
        session.commit()
    engine.dispose()


@pytest.fixture
def colliding_session(tmp_path):
    path = tmp_path / "colliding.db"
    _write_id_track_db(path, COLLIDING_IDS, COLLIDING_TRACK_LENGTHS)
    with db.session_scope(db.make_engine(path)) as session:
        yield session


def test_the_sample_key_separator_cannot_occur_in_a_position():
    # The property SAMPLE_KEY_SEPARATOR's docstring names as the one thing the
    # sample key rests on, asserted directly. Free: no database, no fixture,
    # just the arithmetic that feeds the Python half of the key. The test below
    # proves the consequence; this one states the intent, and is the one that
    # says which way the constant may not be changed.
    positions = sorted(
        {
            position
            for total in COLLIDING_TRACK_LENGTHS + VARIED_TRACK_LENGTHS
            for points in (1, 2, 3, 7, 21)
            if 0 < points < total
            for position in service._even_positions(total, points)
        }
    )
    # Not vacuous, and not digit-blind: a separator set to a digit this sweep
    # never rendered would otherwise slip through the loop below.
    assert set("".join(str(position) for position in positions)) == set("0123456789")

    # Called out on its own because "" is in every string, so the loop would
    # report it as a collision at the first position rather than as what it is.
    assert service.SAMPLE_KEY_SEPARATOR, (
        "an empty separator sits in every key, so the two halves run together "
        "and the key stops being injective"
    )
    for position in positions:
        assert service.SAMPLE_KEY_SEPARATOR not in str(position), (
            f"position {position} renders with SAMPLE_KEY_SEPARATOR "
            f"{service.SAMPLE_KEY_SEPARATOR!r} inside it. Then a position and an "
            "activity id can run together into a key another pair also builds, "
            "and _sample_filter thins the wrong rows silently rather than raising"
        )


@pytest.mark.parametrize("points", [3, 5, 7, 10, 21, 37])
def test_a_batch_of_variable_length_ids_thins_each_track_independently(colliding_session, points):
    # The consequence, on the one fixture shape that can show it. Every id here
    # is decimal and they are not all one width, so the sample key is injective
    # only because of its separator -- see COLLIDING_IDS. Held against the same
    # reference downsampler the fixed-width batch is held against, so a key
    # collision reads as the wrong rows rather than as a count.
    batched = service.tracks(colliding_session, COLLIDING_IDS, points)

    for activity_id, stored in zip(COLLIDING_IDS, COLLIDING_TRACK_LENGTHS, strict=True):
        expected = downsample(list(range(stored)), points)
        got = [row["sec"] for row in batched[activity_id]]
        assert got == expected, f"{activity_id!r}: stored={stored} points={points}"


def test_duplicate_ids_in_a_batch_collapse_to_one_entry(varied_session, sql_params):
    # ``tracks`` documents that duplicates are collapsed, and nothing asserted
    # it. The returned mapping alone cannot: it is keyed by activity id, so a
    # repeated id folds into one entry whether or not the dedupe is there. What
    # the dedupe actually buys is bound parameters -- a repeated id would bind
    # its own subquery id and a key per sample all over again, against a
    # statement that raises rather than slows once it passes SQLite's ceiling
    # (test_the_widest_batch_stays_under_sqlites_bound_parameter_ceiling). So
    # the cost is measured, and that is the half of this test with teeth.
    points = 7
    repeated_ids = VARIED_IDS * 3
    assert len(repeated_ids) > len(set(repeated_ids)), "the batch must actually repeat ids"

    sql_params.clear()
    repeated = service.tracks(varied_session, repeated_ids, points)
    repeated_cost = list(sql_params)
    sql_params.clear()
    distinct = service.tracks(varied_session, VARIED_IDS, points)
    distinct_cost = list(sql_params)

    assert list(repeated) == list(VARIED_IDS), "one entry per distinct id, in the order asked"
    assert repeated == distinct
    assert repeated_cost == distinct_cost, (
        "a repeated id is paying for itself again: the batch binds parameters "
        "per copy rather than per distinct track"
    )
