import importlib.util
from datetime import date, timedelta
from pathlib import Path
from types import ModuleType

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from graphql import GraphQLObjectType, get_introspection_query, get_named_type
from graphql import build_schema as build_sdl_schema
from run365days.api import db, service
from run365days.api.app import GRAPHQL_PATH, HEALTH_PATH, create_app
from run365days.api.schema import (
    MAX_PAGE_SIZE,
    MAX_QUERY_DEPTH,
    MAX_QUERY_TOKENS,
    MAX_TRACK_POINTS,
    build_schema,
)
from run365days.dashboard.builder import downsample
from run365days.export import models
from run365days.export.sqlite import sqlite_url, write_sqlite

YEAR_DAYS = 365
STORED_TRACK_POINTS = 600
TRACKED_ACTIVITY_ID = "r0"
GRAPHIQL_ENV = "RUN365_GRAPHIQL"
VERCEL_ENTRY = Path(__file__).resolve().parents[1] / "api" / "graphql.py"
HTTP_OK = 200
HTTP_NOT_FOUND = 404
BROWSER_HEADERS = {"Accept": "text/html"}

# The deepest document the front end sends (queries.ts YearQuery): year ->
# personalBests -> longest -> weather -> leaf.
DEEPEST_CLIENT_QUERY = """
    { year { personalBests { longest { weather { description } track { sec } } } } }
"""


@pytest.fixture
def api_db(sample_records, tmp_path):
    """A database file on disk, for tests that build their own app."""
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    return path


@pytest.fixture
def client(api_db):
    app = create_app(api_db, graphiql=False)
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


# ── CUI-0004: `points: 1` is a documented degenerate case, not an accident ──
def test_track_with_one_point_returns_the_final_sample(year_session):
    rows = service.track(year_session, TRACKED_ACTIVITY_ID, 1)
    assert [r["sec"] for r in rows] == [STORED_TRACK_POINTS - 1]


def test_single_point_track_matches_the_builder_downsample(year_session):
    """The degenerate case is shared with the dashboard, not local to the API."""
    rows = service.track(year_session, TRACKED_ACTIVITY_ID, 1)
    assert [r["sec"] for r in rows] == downsample(list(range(STORED_TRACK_POINTS)), 1)


def test_graphql_accepts_one_point_and_returns_the_final_sample(year_client):
    d = gql(year_client, '{ activity(id: "r0") { track(points: 1) { sec } } }')
    assert [p["sec"] for p in d["activity"]["track"]] == [STORED_TRACK_POINTS - 1]


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


# ── CUI-0003: the depth limit is pinned to the schema's own deepest query ──
def _published_query_type() -> GraphQLObjectType:
    """Return the Query type as a client sees it, rebuilt from the published SDL."""
    return build_sdl_schema(build_schema().as_str()).query_type


def _deepest_chain(gql_type, seen: frozenset[str] = frozenset()) -> list[str]:
    """Return the longest field-name path from *gql_type* down to a scalar leaf.

    The path length is the operation depth QueryDepthLimiter would count, minus
    the trailing leaf, which carries no selection set and so adds no level.
    A type reachable from itself would make depth unbounded -- the one change
    that turns MAX_QUERY_DEPTH from a formality into the only bound left -- so
    this fails loudly rather than recursing.
    """
    named = get_named_type(gql_type)
    if not isinstance(named, GraphQLObjectType):
        return []
    assert named.name not in seen, (
        f"{named.name} is now reachable from itself, so query depth is unbounded "
        "and MAX_QUERY_DEPTH is the only thing bounding it. Re-argue the value."
    )
    return max(
        ([name, *_deepest_chain(f.type, seen | {named.name})] for name, f in named.fields.items()),
        key=len,
    )


def _nested_query(chain: list[str]) -> str:
    """Render *chain* as one nested selection set, outermost field first."""
    query = chain[-1]
    for name in reversed(chain[:-1]):
        query = f"{name} {{ {query} }}"
    return f"{{ {query} }}"


def test_depth_limit_equals_the_deepest_query_the_schema_allows():
    chain = _deepest_chain(_published_query_type())
    assert len(chain) - 1 == MAX_QUERY_DEPTH, (
        f"deepest query the schema allows is {' -> '.join(chain)} at depth "
        f"{len(chain) - 1}, but MAX_QUERY_DEPTH is {MAX_QUERY_DEPTH}. A limit above "
        "that depth can never reject anything; one below it rejects a legal query."
    )


def test_configured_schema_rejects_a_document_one_level_too_deep():
    """The shipped limit, not a test-only shallow one, runs the reject branch."""
    too_deep = _nested_query([*_deepest_chain(_published_query_type()), "oneLevelTooDeep"])
    result = build_schema().execute_sync(too_deep)
    assert any("depth" in str(e).lower() for e in result.errors), result.errors


def test_one_level_tighter_would_reject_the_deepest_client_query():
    result = build_schema(max_depth=MAX_QUERY_DEPTH - 1).execute_sync(DEEPEST_CLIENT_QUERY)
    assert any("depth" in str(e).lower() for e in result.errors), result.errors


def test_introspection_is_exempt_from_the_depth_limit(monkeypatch):
    """Why a depth tight to the data graph does not break GraphiQL."""
    monkeypatch.setenv(GRAPHIQL_ENV, "1")
    result = build_schema(max_depth=1, max_tokens=100_000).execute_sync(get_introspection_query())
    assert not result.errors


# ── S-012: every enforced limit is visible in the SDL ──────────────────────
@pytest.mark.parametrize(
    "published",
    [
        f"1 to {MAX_PAGE_SIZE}",
        f"1 to {MAX_TRACK_POINTS}",
        f"{MAX_QUERY_DEPTH} levels",
        f"{MAX_QUERY_TOKENS} tokens",
    ],
)
def test_sdl_publishes_the_limits_the_api_enforces(published):
    assert published in build_schema().as_str()


# ── CUI-0019: numPoints counted pre-downsample points and is withdrawn ─────
def test_sdl_no_longer_declares_num_points():
    assert "numPoints" not in build_schema().as_str()


def test_querying_num_points_is_rejected(client):
    assert "numPoints" in gql_errors(client, "{ activities { id numPoints } }")


def test_the_stored_activity_has_no_point_count_column():
    assert not hasattr(models.Activity, "num_points")


# ── AU-001: GraphiQL is off unless the environment asks for it ─────────────
def _load_vercel_entry(monkeypatch, db_path, graphiql_env=None) -> ModuleType:
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


# ── AU-049: the factory default follows the flag instead of being a second one ─
def _ide_status(db_path, **kwargs) -> int:
    """Ask for the endpoint as a browser would, and report the status."""
    app = create_app(db_path, **kwargs)
    app.testing = True
    return app.test_client().get(GRAPHQL_PATH, headers=BROWSER_HEADERS).status_code


def test_the_ide_is_not_served_when_no_argument_and_no_env_var(monkeypatch, api_db):
    monkeypatch.delenv(GRAPHIQL_ENV, raising=False)
    assert _ide_status(api_db) == HTTP_NOT_FOUND


def test_the_ide_is_served_when_the_caller_asks_for_it_explicitly(monkeypatch, api_db):
    monkeypatch.delenv(GRAPHIQL_ENV, raising=False)
    assert _ide_status(api_db, graphiql=True) == HTTP_OK


def test_the_ide_follows_the_env_var_when_no_argument_is_given(monkeypatch, api_db):
    monkeypatch.setenv(GRAPHIQL_ENV, "1")
    assert _ide_status(api_db) == HTTP_OK


def test_an_explicit_false_overrides_the_env_var(monkeypatch, api_db):
    monkeypatch.setenv(GRAPHIQL_ENV, "1")
    assert _ide_status(api_db, graphiql=False) == HTTP_NOT_FOUND


def test_the_env_var_is_read_per_call_rather_than_frozen_at_import(monkeypatch, api_db):
    monkeypatch.delenv(GRAPHIQL_ENV, raising=False)
    while_unset = _ide_status(api_db)
    monkeypatch.setenv(GRAPHIQL_ENV, "1")
    assert (while_unset, _ide_status(api_db)) == (HTTP_NOT_FOUND, HTTP_OK)


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
