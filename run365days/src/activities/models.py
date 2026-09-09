from dataclasses import dataclass, field


@dataclass
class TrackPoint:
    lat: float | None
    lon: float | None
    time: str
    elevation: float | None = None
    temperature: float | None = None
    cadence: int | None = None
    speed: float | None = None
    distance_m: float | None = None


@dataclass
class Activity:
    activity_id: str
    date: str  # ISO format: '2021-01-01 06:10:00'
    total_time: str  # 'HH:MM:SS'
    total_sec: float
    distance_km: float
    distance_by_coord_km: float
    pacing: str | None = None  # 'MM:SS' per km

    # GPX-only fields
    avg_cadence: float | None = None
    max_cadence: float | None = None
    min_elevation: float | None = None
    max_elevation: float | None = None
    avg_temp: float | None = None
    min_temp: float | None = None
    max_temp: float | None = None

    # TCX-only fields
    calories: int | None = None

    num_track_points: int = 0
    track_points: list = field(default_factory=list)
