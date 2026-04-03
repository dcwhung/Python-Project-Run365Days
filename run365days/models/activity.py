from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional


@dataclass
class TrackPoint:
    lat: Optional[float]
    lon: Optional[float]
    time: str
    elevation: Optional[float] = None
    temperature: Optional[float] = None
    cadence: Optional[int] = None
    speed: Optional[float] = None
    distance_m: Optional[float] = None


@dataclass
class Activity:
    activity_id: str
    date: str                        # ISO format: '2021-01-01 06:10:00'
    total_time: str                  # 'HH:MM:SS'
    total_sec: float
    distance_km: float
    distance_by_coord_km: float
    pacing: Optional[str] = None     # 'MM:SS' per km

    # GPX-only fields
    avg_cadence: Optional[float] = None
    max_cadence: Optional[float] = None
    min_elevation: Optional[float] = None
    max_elevation: Optional[float] = None
    avg_temp: Optional[float] = None
    min_temp: Optional[float] = None
    max_temp: Optional[float] = None

    # TCX-only fields
    calories: Optional[int] = None

    num_track_points: int = 0
    track_points: list = field(default_factory=list)
