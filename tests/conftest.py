"""Shared fixtures: a tiny but complete set of parsed sources."""

import pytest

from run365days.activities.models import Activity, TrackPoint
from run365days.export.records import ExportRecords, build_records
from run365days.weight.analysis import WeightRecord


def make_point(i: int, lat=22.3, lon=114.2, ele=330.0, cad=83, speed=2.7, temp=None) -> TrackPoint:
    """Return one track point *i* seconds into a run on 2021-01-08."""
    return TrackPoint(
        lat=lat,
        lon=lon,
        time=f"2021-01-08 12:{i // 60:02d}:{i % 60:02d}",
        elevation=ele,
        temperature=temp,
        cadence=cad,
        speed=speed,
        distance_m=i * 2.7,
    )


def make_activity(activity_id="1", n_points=10, calories=300, gps=True, **overrides) -> Activity:
    """Return a 30-minute, 5 km run with *n_points* samples."""
    pts = [
        make_point(i * 6, lat=22.3 if gps else None, lon=114.2 if gps else None, ele=330 + i)
        for i in range(n_points)
    ]
    base = dict(
        activity_id=activity_id,
        date="2021-01-08 12:00:00",
        total_time="00:30:00",
        total_sec=1800.0,
        distance_km=5.0,
        distance_by_coord_km=4.98,
        calories=calories,
        num_track_points=n_points,
        track_points=pts,
    )
    base.update(overrides)
    return Activity(**base)


@pytest.fixture
def sample_sources() -> dict:
    """Two runs, a GPX twin with temperature, weight and weather rows."""
    tcx_a = make_activity("a", date="2021-01-08 12:00:00")
    tcx_b = make_activity("b", date="2021-01-09 06:30:00", gps=False, calories=None)
    gpx_a = make_activity("a", date="2021-01-08 12:00:00", avg_temp=18.4)
    for p in gpx_a.track_points:
        p.temperature = 18.0
    return {
        "year": 2021,
        "tcx": [tcx_b, tcx_a],  # deliberately unsorted
        "gpx": [gpx_a],
        "weight": [
            WeightRecord(1, "2021-01-08", 154.8, 70.28, 24.3),
            WeightRecord(2, "2021-01-09", 154.2, 70.01, 24.2),
        ],
        "hko": [
            {
                "Date": "2021-01-08",
                "Max. Temp": "21.0",
                "Avg. Temp": "18.5",
                "Min. Temp": "15.0",
                "Humidity (%)": "70",
                "Total Rainfall (mm)": "Trace",
                "Avg. Wind Speed (km/h)": "12.0",
                "Sunrise": "07:03",
                "Sunset": "17:55",
            }
        ],
        "hourly": [
            {
                "Date": "2021-01-08",
                "Time": "12:00",
                "Description": "Clear weather",
                "Temperature (°C)": "19.0",
                "Humidity (%)": "65",
                "Wind (Km/h)": "10",
            },
            {
                "Date": "2021-01-08",
                "Time": "15:00",
                "Description": "Cloudy skies",
                "Temperature (°C)": "21.0",
                "Humidity (%)": "60",
                "Wind (Km/h)": "12",
            },
        ],
        "warnings": [
            {
                "Date": "2021-01-08",
                "Type": "Fire Danger",
                "Warning_Signal": "RED FIRE DANGER WARNING",
                "Start_Time": "2021-01-08 06:00:00",
                "End_Time": "2021-01-08 18:00:00",
            },
            {
                "Date": "2021-01-08",
                "Type": "Fire Danger",
                "Warning_Signal": "RED FIRE DANGER WARNING",  # duplicate signal
                "Start_Time": "2021-01-08 06:00:00",
                "End_Time": "2021-01-08 18:00:00",
            },
            {"Date": "2021-01-09", "Type": "Unknown", "Warning_Signal": ""},  # no signal
        ],
    }


@pytest.fixture
def sample_records(sample_sources) -> ExportRecords:
    """The fixture above run through :func:`build_records`."""
    s = sample_sources
    return build_records(
        s["year"],
        s["tcx"],
        s["gpx"],
        s["weight"],
        s["hko"],
        s["hourly"],
        s["warnings"],
        point_limit=4,
        generated_at="2026-01-01T00:00:00",
    )
