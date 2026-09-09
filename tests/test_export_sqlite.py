from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from run365days.export import models
from run365days.export.sqlite import sqlite_url, write_sqlite


def _session(path):
    return Session(create_engine(sqlite_url(path, read_only=True)))


def test_writes_every_table(sample_records, tmp_path):
    db = tmp_path / "run365.db"
    write_sqlite(sample_records, db)
    with _session(db) as s:
        assert s.scalar(select(func.count()).select_from(models.Activity)) == 2
        assert s.scalar(select(func.count()).select_from(models.TrackPoint)) == 8
        assert s.scalar(select(func.count()).select_from(models.ActivityWarning)) == 1
        assert s.scalar(select(func.count()).select_from(models.WeightEntry)) == 2
        assert s.scalar(select(func.count()).select_from(models.DailyWeather)) == 1
        assert s.scalar(select(func.count()).select_from(models.WeatherWarning)) == 2
        meta = {m.key: m.value for m in s.scalars(select(models.Meta))}
        assert meta == {"year": "2021", "generated_at": "2026-01-01T00:00:00"}


def test_activity_row_and_relationships(sample_records, tmp_path):
    db = tmp_path / "run365.db"
    write_sqlite(sample_records, db)
    with _session(db) as s:
        a = s.get(models.Activity, "a")
        assert a.weather_description == "Clear weather"
        assert a.weather_temp_c == 19.0
        assert [w.signal for w in a.warnings] == ["RED FIRE DANGER WARNING"]
        assert [p.seq for p in a.track_points] == [0, 1, 2, 3]
        assert a.track_points[0].temp_c == 18.0
        b = s.get(models.Activity, "b")
        assert b.weather_description is None
        assert b.warnings == []


def test_rewrites_existing_file(sample_records, tmp_path):
    db = tmp_path / "run365.db"
    write_sqlite(sample_records, db)
    sample_records.activities.pop()
    del sample_records.tracks["b"]
    write_sqlite(sample_records, db)
    with _session(db) as s:
        assert s.scalar(select(func.count()).select_from(models.Activity)) == 1


def test_sqlite_url_forms(tmp_path):
    p = tmp_path / "x.db"
    assert sqlite_url(p) == f"sqlite:///{p}"
    assert sqlite_url(p, read_only=True) == f"sqlite:///file:{p}?mode=ro&uri=true"
