"""SQLAlchemy 2.0 ORM models for ``run365.db``.

The schema mirrors :class:`~run365days.export.records.ExportRecords` one to
one. It is read-only at runtime: the API opens the file that the export step
produced and never writes to it.
"""

from sqlalchemy import Boolean, Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base for every table."""


class Meta(Base):
    """Single-row table describing the export."""

    __tablename__ = "meta"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    value: Mapped[str] = mapped_column(String)


class Activity(Base):
    """One run with its summary metrics and the weather at start time."""

    __tablename__ = "activities"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    date: Mapped[str] = mapped_column(String, index=True)
    start_time: Mapped[str] = mapped_column(String)
    day_of_year: Mapped[int] = mapped_column(Integer)
    distance_km: Mapped[float] = mapped_column(Float)
    duration_sec: Mapped[int] = mapped_column(Integer)
    pace_sec_per_km: Mapped[int | None] = mapped_column(Integer)
    calories: Mapped[int | None] = mapped_column(Integer)
    avg_cadence: Mapped[float | None] = mapped_column(Float)
    avg_temp_c: Mapped[float | None] = mapped_column(Float)
    elevation_min_m: Mapped[float | None] = mapped_column(Float)
    elevation_max_m: Mapped[float | None] = mapped_column(Float)
    ascent_m: Mapped[float | None] = mapped_column(Float)
    has_gps: Mapped[bool] = mapped_column(Boolean)
    num_points: Mapped[int] = mapped_column(Integer)
    weather_description: Mapped[str | None] = mapped_column(String)
    weather_temp_c: Mapped[float | None] = mapped_column(Float)
    weather_humidity_pct: Mapped[float | None] = mapped_column(Float)
    weather_wind_kmh: Mapped[float | None] = mapped_column(Float)

    track_points: Mapped[list["TrackPoint"]] = relationship(
        back_populates="activity", order_by="TrackPoint.seq", cascade="all, delete-orphan"
    )
    warnings: Mapped[list["ActivityWarning"]] = relationship(
        back_populates="activity", cascade="all, delete-orphan"
    )


class TrackPoint(Base):
    """One stored sample of a run's track (already downsampled at export)."""

    __tablename__ = "track_points"

    activity_id: Mapped[str] = mapped_column(ForeignKey("activities.id"), primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    sec: Mapped[int] = mapped_column(Integer)
    lat: Mapped[float | None] = mapped_column(Float)
    lon: Mapped[float | None] = mapped_column(Float)
    elevation_m: Mapped[float | None] = mapped_column(Float)
    distance_m: Mapped[float | None] = mapped_column(Float)
    speed_mps: Mapped[float | None] = mapped_column(Float)
    cadence: Mapped[int | None] = mapped_column(Integer)
    temp_c: Mapped[float | None] = mapped_column(Float)

    activity: Mapped[Activity] = relationship(back_populates="track_points")


class ActivityWarning(Base):
    """An HKO warning signal in force on the day of a run."""

    __tablename__ = "activity_warnings"

    activity_id: Mapped[str] = mapped_column(ForeignKey("activities.id"), primary_key=True)
    signal: Mapped[str] = mapped_column(String, primary_key=True)

    activity: Mapped[Activity] = relationship(back_populates="warnings")


class WeightEntry(Base):
    """One daily weigh-in."""

    __tablename__ = "weight"

    date: Mapped[str] = mapped_column(String, primary_key=True)
    weight_lbs: Mapped[float] = mapped_column(Float)
    weight_kg: Mapped[float] = mapped_column(Float)
    bmi: Mapped[float] = mapped_column(Float)


class DailyWeather(Base):
    """One day of the HKO daily extract."""

    __tablename__ = "daily_weather"

    date: Mapped[str] = mapped_column(String, primary_key=True)
    max_temp_c: Mapped[float | None] = mapped_column(Float)
    avg_temp_c: Mapped[float | None] = mapped_column(Float)
    min_temp_c: Mapped[float | None] = mapped_column(Float)
    humidity_pct: Mapped[float | None] = mapped_column(Float)
    rainfall_mm: Mapped[float | None] = mapped_column(Float)
    wind_kmh: Mapped[float | None] = mapped_column(Float)
    sunrise: Mapped[str | None] = mapped_column(String)
    sunset: Mapped[str | None] = mapped_column(String)


class WeatherWarning(Base):
    """One HKO warning or tropical cyclone signal."""

    __tablename__ = "weather_warnings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[str] = mapped_column(String, index=True)
    type: Mapped[str | None] = mapped_column(String)
    signal: Mapped[str] = mapped_column(String)
    start_time: Mapped[str | None] = mapped_column(String)
    end_time: Mapped[str | None] = mapped_column(String)
