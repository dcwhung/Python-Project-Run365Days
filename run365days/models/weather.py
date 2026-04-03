from dataclasses import dataclass
from typing import Optional


@dataclass
class HourlyWeather:
    date: str        # 'YYYY-MM-DD'
    time: str        # 'HH:MM'
    temperature_c: Optional[float]
    wind_kmh: Optional[float]
    humidity_pct: Optional[float]
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
    mean_temp_c: Optional[float] = None
    max_temp_c: Optional[float] = None
    min_temp_c: Optional[float] = None
    mean_humidity_pct: Optional[float] = None
    total_rainfall_mm: Optional[float] = None
    mean_wind_kmh: Optional[float] = None


@dataclass
class SunMoon:
    date: str
    sunrise: str
    sunset: str
    solar_noon: str
    day_length: str
    moonrise: Optional[str] = None
    moonset: Optional[str] = None
    moon_illumination_pct: Optional[float] = None
