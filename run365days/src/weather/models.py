"""Dataclasses for the weather sources used to annotate runs."""

from dataclasses import dataclass


@dataclass
class HourlyWeather:
    """One hourly observation scraped from freemeteo.hk.

    Attributes:
        date: Observation date ``YYYY-MM-DD``.
        time: Observation time ``HH:MM`` (local).
        temperature_c: Air temperature in Celsius.
        wind_kmh: Wind speed in km/h.
        humidity_pct: Relative humidity in percent.
        description: Human-readable sky condition (e.g. ``"Rain"``).
    """

    date: str  # 'YYYY-MM-DD'
    time: str  # 'HH:MM'
    temperature_c: float | None
    wind_kmh: float | None
    humidity_pct: float | None
    description: str


@dataclass
class WeatherWarning:
    """One Hong Kong Observatory warning or tropical cyclone signal.

    Attributes:
        date: The day the warning was queried for, ``YYYY-MM-DD``.
        warning_type: Category from the HKO legend (e.g. ``"Rainstorm"``).
        warning_signal: Signal code in upper case (e.g. ``"AMBER"``, ``"T8"``).
        start_time: Local start timestamp ``YYYY-MM-DD HH:MM:SS``.
        end_time: Local end timestamp ``YYYY-MM-DD HH:MM:SS``.
        icon_url: Relative URL of the HKO signal icon.
    """

    date: str
    warning_type: str
    warning_signal: str
    start_time: str
    end_time: str
    icon_url: str


@dataclass
class DailyWeather:
    """One row of the HKO daily weather extract.

    Attributes:
        date: Observation date ``YYYY-MM-DD``.
        mean_temp_c: Mean air temperature in Celsius.
        max_temp_c: Maximum air temperature in Celsius.
        min_temp_c: Minimum air temperature in Celsius.
        mean_humidity_pct: Mean relative humidity in percent.
        total_rainfall_mm: Total rainfall in millimetres.
        mean_wind_kmh: Mean wind speed in km/h.
    """

    date: str
    mean_temp_c: float | None = None
    max_temp_c: float | None = None
    min_temp_c: float | None = None
    mean_humidity_pct: float | None = None
    total_rainfall_mm: float | None = None
    mean_wind_kmh: float | None = None


@dataclass
class SunMoon:
    """Sunrise, sunset and moon times for one day.

    Attributes:
        date: Calendar date ``YYYY-MM-DD``.
        sunrise: Local sunrise time ``HH:MM``.
        sunset: Local sunset time ``HH:MM``.
        solar_noon: Local solar noon ``HH:MM``.
        day_length: Day length ``HH:MM:SS``.
        moonrise: Local moonrise time, if the moon rises that day.
        moonset: Local moonset time, if the moon sets that day.
        moon_illumination_pct: Illuminated fraction of the moon in percent.
    """

    date: str
    sunrise: str
    sunset: str
    solar_noon: str
    day_length: str
    moonrise: str | None = None
    moonset: str | None = None
    moon_illumination_pct: float | None = None
