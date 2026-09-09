from dataclasses import dataclass


@dataclass
class HourlyWeather:
    date: str  # 'YYYY-MM-DD'
    time: str  # 'HH:MM'
    temperature_c: float | None
    wind_kmh: float | None
    humidity_pct: float | None
    description: str


@dataclass
class WeatherWarning:
    date: str
    warning_type: str
    warning_signal: str
    start_time: str
    end_time: str
    icon_url: str


@dataclass
class DailyWeather:
    date: str
    mean_temp_c: float | None = None
    max_temp_c: float | None = None
    min_temp_c: float | None = None
    mean_humidity_pct: float | None = None
    total_rainfall_mm: float | None = None
    mean_wind_kmh: float | None = None


@dataclass
class SunMoon:
    date: str
    sunrise: str
    sunset: str
    solar_noon: str
    day_length: str
    moonrise: str | None = None
    moonset: str | None = None
    moon_illumination_pct: float | None = None
