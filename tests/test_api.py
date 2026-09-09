import pytest

from run365days.api import db
from run365days.api.app import GRAPHQL_PATH, HEALTH_PATH, create_app
from run365days.export.sqlite import write_sqlite


@pytest.fixture
def client(sample_records, tmp_path):
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    app = create_app(path, graphiql=False)
    app.testing = True
    return app.test_client()


def gql(client, query, variables=None):
    r = client.post(GRAPHQL_PATH, json={"query": query, "variables": variables or {}})
    assert r.status_code == 200, r.data
    body = r.get_json()
    assert "errors" not in body, body["errors"]
    return body["data"]


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
