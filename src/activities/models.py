"""Dataclasses describing one Garmin activity and its GPS track."""

from dataclasses import dataclass, field


@dataclass
class TrackPoint:
    """One sample of a GPS track.

    Attributes:
        lat: Latitude in degrees, or ``None`` for indoor (treadmill) samples.
        lon: Longitude in degrees, or ``None`` for indoor samples.
        time: Local timestamp formatted ``YYYY-MM-DD HH:MM:SS``.
        elevation: Altitude in metres (TCX / GPX only).
        temperature: Ambient temperature in Celsius (GPX only).
        cadence: Steps per minute for one foot, as reported by Garmin.
        speed: Instantaneous speed in metres per second (TCX only).
        distance_m: Cumulative distance in metres (TCX only).
    """

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
    """A single run parsed from a Garmin export file.

    Attributes:
        activity_id: Garmin activity id taken from the file name.
        date: Local start time formatted ``YYYY-MM-DD HH:MM:SS``.
        total_time: Elapsed time formatted ``HH:MM:SS``.
        total_sec: Elapsed time in seconds.
        distance_km: Distance reported by the device (0.0 for GPX).
        distance_by_coord_km: Distance recomputed from the GPS track.
        pacing: Pace formatted ``MM:SS`` per kilometre.
        avg_cadence: Mean cadence in steps per minute (GPX only).
        max_cadence: Peak cadence in steps per minute (GPX only).
        min_elevation: Lowest altitude in metres (GPX only).
        max_elevation: Highest altitude in metres (GPX only).
        avg_temp: Mean ambient temperature in Celsius (GPX only).
        min_temp: Lowest ambient temperature in Celsius (GPX only).
        max_temp: Highest ambient temperature in Celsius (GPX only).
        calories: Kilocalories reported by the device (TCX only).
        num_track_points: Number of GPS segments used for the distance.
        track_points: The full list of :class:`TrackPoint` samples.
    """

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
