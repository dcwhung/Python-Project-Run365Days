from run365days.export.records import TRACK_COLUMNS, activity_record, build_records


def test_activities_sorted_by_start(sample_records):
    assert [a["id"] for a in sample_records.activities] == ["a", "b"]


def test_activity_record_shape(sample_records):
    a = sample_records.activities[0]
    assert a["date"] == "2021-01-08"
    assert a["start_time"] == "12:00"
    assert a["day_of_year"] == 8
    assert a["distance_km"] == 5.0
    assert a["duration_sec"] == 1800
    assert a["pace_sec_per_km"] == 360
    assert a["calories"] == 300
    assert a["avg_cadence"] == 166  # 83 * 2
    assert a["avg_temp_c"] == 18.4  # from the GPX twin
    assert a["elevation_min_m"] == 330
    assert a["elevation_max_m"] == 339
    assert a["has_gps"] is True
    assert a["num_points"] == 10


def test_nearest_hourly_weather_is_nested(sample_records):
    a = sample_records.activities[0]
    assert a["weather"] == {
        "description": "Clear weather",
        "temp_c": 19.0,
        "humidity_pct": 65.0,
        "wind_kmh": 10.0,
    }


def test_missing_weather_and_gpx_give_none(sample_records):
    b = sample_records.activities[1]
    assert b["weather"] is None
    assert b["avg_temp_c"] is None
    assert b["calories"] is None
    assert b["has_gps"] is False
    assert b["warnings"] == []


def test_warnings_deduplicated_per_activity(sample_records):
    assert sample_records.activities[0]["warnings"] == ["RED FIRE DANGER WARNING"]


def test_warning_rows_without_signal_are_dropped(sample_records):
    assert len(sample_records.warnings) == 2
    assert sample_records.warnings[0] == {
        "date": "2021-01-08",
        "type": "Fire Danger",
        "signal": "RED FIRE DANGER WARNING",
        "start_time": "2021-01-08 06:00:00",
        "end_time": "2021-01-08 18:00:00",
    }


def test_tracks_downsampled_and_column_aligned(sample_records):
    rows = sample_records.tracks["a"]
    assert len(rows) == 4  # point_limit
    assert all(len(r) == len(TRACK_COLUMNS) for r in rows)
    first = dict(zip(TRACK_COLUMNS, rows[0], strict=True))
    assert first["sec"] == 0
    assert first["temp_c"] == 18.0  # merged from GPX
    assert rows[-1][0] == 54  # last point kept


def test_weight_and_weather_shapes(sample_records):
    assert sample_records.weight[0] == {
        "date": "2021-01-08",
        "weight_lbs": 154.8,
        "weight_kg": 70.28,
        "bmi": 24.3,
    }
    day = sample_records.daily_weather[0]
    assert day["date"] == "2021-01-08"
    assert day["max_temp_c"] == 21.0
    assert day["rainfall_mm"] is None  # "Trace" is not a number
    assert day["sunrise"] == "07:03"


def test_generated_at_defaults_to_now(sample_sources):
    s = sample_sources
    records = build_records(s["year"], s["tcx"], [], [], [], [], [])
    assert records.generated_at.startswith("20")
    assert records.year == 2021


def test_activity_record_without_points():
    from tests.conftest import make_activity

    a = make_activity("z", n_points=0)
    rec = activity_record(a, None, None, [])
    assert rec["elevation_min_m"] is None
    assert rec["ascent_m"] == 0
    assert rec["has_gps"] is False
