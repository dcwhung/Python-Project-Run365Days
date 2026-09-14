"""Parse Garmin TCX (Training Center XML) exports.

TCX is the richest of the three formats: per-lap distance, time and
calories plus per-point speed, cadence, altitude and cumulative distance.
It is the primary source for the dashboard.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from run365days.activities.models import Activity, TrackPoint
from run365days.activities.parsers.base import (
    ActivityParseError,
    ActivitySkipped,
    BaseActivityParser,
    element_text,
    ensure_finite,
    optional_float,
    optional_int,
    required_float,
    required_text,
)
from run365days.common.geo import total_track_distance
from run365days.common.time import pace_str, parse_datetime, seconds_to_hhmmss

_NS = {
    "ns": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2",
    "ns3": "http://www.garmin.com/xmlschemas/ActivityExtension/v2",
}

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_METRES_PER_KM = 1000
_DISTANCE_DECIMALS = 2


class TCXParser(BaseActivityParser):
    """Parser for Garmin ``*.tcx`` files."""

    FORMAT = "tcx"

    def parse(self, file_path: Path) -> Activity:
        """Parse one TCX file.

        Args:
            file_path: Path to a ``*.tcx`` file exported from Garmin Connect.

        Returns:
            The parsed activity with device distance, calories and the full
            track (speed, cadence, altitude per point).

        Raises:
            ActivitySkipped: If the activity is older than ``current_year``
                or the sport is not running.
            ActivityParseError: If a mandatory element is missing.
        """
        # S314: the input is the user's own Garmin export on local disk
        # (config.TCX_DIR), never a network fetch or an upload, so an
        # entity-expansion bomb would have to be self-planted. Moving to
        # defusedxml is a dependency change rather than a lint change.
        root = ET.parse(file_path).getroot()  # noqa: S314

        activity_id = file_path.stem.rsplit("_", 1)[-1]
        activity = root.find("ns:Activities/ns:Activity", _NS)
        if activity is None:
            raise ActivityParseError("missing required element ns:Activities/ns:Activity")

        act_time = parse_datetime(required_text(activity, "ns:Id", _NS))

        if act_time.year < self.current_year:
            raise ActivitySkipped(f"Skipping old activity: {activity_id}")

        if "Running" not in activity.get("Sport", ""):
            raise ActivitySkipped(f"Not a running activity: {activity_id}")

        track_points: list[TrackPoint] = []
        lap_rows: list[dict] = []

        for lap in activity.findall("ns:Lap", _NS):
            lap_rows.append(self._lap_row(lap))
            track_points.extend(self._track_points(lap))

        if not lap_rows:
            raise ActivityParseError(f"no laps found in {file_path.name}")

        lap_df = pd.DataFrame(lap_rows)
        # Finite laps can still sum to inf, and int(inf) inside seconds_to_hhmmss
        # raises OverflowError, which no per-file except branch catches: one file
        # would take the whole export down with it (CUI-0001).
        total_sec = ensure_finite(lap_df["Time"].sum(), "total time")
        dist_km = round(
            ensure_finite(lap_df["Distance"].sum(), "total distance") / _METRES_PER_KM,
            _DISTANCE_DECIMALS,
        )
        calories = int(lap_df["Calories"].sum())

        coords = [(tp.lat, tp.lon) for tp in track_points]
        dist_by_coord, num_pts = total_track_distance(coords)

        return Activity(
            activity_id=activity_id,
            date=act_time.strftime(_TIMESTAMP_FORMAT),
            total_time=seconds_to_hhmmss(total_sec),
            total_sec=total_sec,
            distance_km=dist_km,
            distance_by_coord_km=dist_by_coord,
            pacing=pace_str(total_sec, dist_km),
            calories=calories,
            num_track_points=num_pts,
            track_points=track_points,
        )

    @staticmethod
    def _lap_row(lap: ET.Element) -> dict:
        """Return one lap's totals; only time and distance are mandatory."""
        return {
            "Time": required_float(lap, "ns:TotalTimeSeconds", _NS),
            "Distance": required_float(lap, "ns:DistanceMeters", _NS),
            "MaxSpeed": optional_float(lap, "ns:MaximumSpeed", _NS) or 0.0,
            "Calories": optional_int(lap, "ns:Calories", _NS) or 0,
        }

    @staticmethod
    def _track_points(lap: ET.Element) -> list[TrackPoint]:
        """Return the lap's track points, skipping any that carry no timestamp."""
        points = []
        for trkpt in lap.findall(".//ns:Trackpoint", _NS):
            # Position, AltitudeMeters, DistanceMeters and the TPX extensions are
            # all optional in the TCX v2 schema; indoor runs carry none of them.
            time_text = element_text(trkpt, "ns:Time", _NS)
            if time_text is None:
                continue

            pos = trkpt.find(".//ns:Position", _NS)
            points.append(
                TrackPoint(
                    lat=optional_float(pos, ".//ns:LatitudeDegrees", _NS)
                    if pos is not None
                    else None,
                    lon=optional_float(pos, ".//ns:LongitudeDegrees", _NS)
                    if pos is not None
                    else None,
                    time=parse_datetime(time_text).strftime(_TIMESTAMP_FORMAT),
                    elevation=optional_float(trkpt, "ns:AltitudeMeters", _NS),
                    distance_m=optional_float(trkpt, "ns:DistanceMeters", _NS),
                    speed=optional_float(trkpt, ".//ns3:Speed", _NS),
                    cadence=optional_int(trkpt, ".//ns3:RunCadence", _NS),
                )
            )
        return points
