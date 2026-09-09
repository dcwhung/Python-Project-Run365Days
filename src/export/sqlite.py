"""Write :class:`~run365days.export.records.ExportRecords` to a SQLite file."""

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from run365days.export import models
from run365days.export.records import TRACK_COLUMNS, ExportRecords


def sqlite_url(path: Path, read_only: bool = False) -> str:
    """Return the SQLAlchemy URL for *path*.

    Args:
        path: Location of the database file.
        read_only: Open with SQLite's ``mode=ro`` URI flag, which is what the
            API uses on a read-only filesystem.
    """
    if read_only:
        return f"sqlite:///file:{path}?mode=ro&uri=true"
    return f"sqlite:///{path}"


def _activity_row(record: dict) -> models.Activity:
    weather = record.get("weather") or {}
    return models.Activity(
        id=record["id"],
        date=record["date"],
        start_time=record["start_time"],
        day_of_year=record["day_of_year"],
        distance_km=record["distance_km"],
        duration_sec=record["duration_sec"],
        pace_sec_per_km=record["pace_sec_per_km"],
        calories=record["calories"],
        avg_cadence=record["avg_cadence"],
        avg_temp_c=record["avg_temp_c"],
        elevation_min_m=record["elevation_min_m"],
        elevation_max_m=record["elevation_max_m"],
        ascent_m=record["ascent_m"],
        has_gps=record["has_gps"],
        num_points=record["num_points"],
        weather_description=weather.get("description"),
        weather_temp_c=weather.get("temp_c"),
        weather_humidity_pct=weather.get("humidity_pct"),
        weather_wind_kmh=weather.get("wind_kmh"),
        warnings=[models.ActivityWarning(signal=s) for s in record["warnings"]],
    )


def _track_rows(activity_id: str, rows: list[list]) -> list[models.TrackPoint]:
    return [
        models.TrackPoint(
            activity_id=activity_id, seq=seq, **dict(zip(TRACK_COLUMNS, row, strict=True))
        )
        for seq, row in enumerate(rows)
    ]


def write_sqlite(records: ExportRecords, path: Path) -> None:
    """Create (or replace) the SQLite database at *path* from *records*.

    Args:
        records: The shaped export.
        path: Destination file; parents are created, an existing file is
            removed first so the schema is always fresh.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    engine = create_engine(sqlite_url(path))
    models.Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add_all(
            [
                models.Meta(key="year", value=str(records.year)),
                models.Meta(key="generated_at", value=records.generated_at),
            ]
        )
        session.add_all(_activity_row(a) for a in records.activities)
        for activity_id, rows in records.tracks.items():
            session.add_all(_track_rows(activity_id, rows))
        session.add_all(models.WeightEntry(**w) for w in records.weight)
        session.add_all(models.DailyWeather(**d) for d in records.daily_weather)
        session.add_all(models.WeatherWarning(**w) for w in records.warnings)
        session.commit()
    engine.dispose()
