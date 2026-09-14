"""Parse Garmin GPX 1.1 exports.

GPX carries the per-point ambient temperature and cadence that TCX lacks,
but no device distance or calories, so ``distance_km`` is always ``0.0``
and callers rely on ``distance_by_coord_km``.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import pandas as pd

from run365days.activities.models import Activity, TrackPoint
from run365days.activities.parsers.base import (
    ActivityParseError,
    ActivitySkipped,
    BaseActivityParser,
    element_text,
    optional_float,
    optional_int,
    parse_finite_float,
    required_text,
)
from run365days.common.geo import total_track_distance
from run365days.common.time import pace_str, parse_datetime, seconds_to_hhmmss

_NS = {
    "ns": "http://www.topografix.com/GPX/1/1",
    "ns3": "http://www.garmin.com/xmlschemas/TrackPointExtension/v1",
}

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_RUNNING_TYPE = "running"
_STEPS_PER_CADENCE_SAMPLE = 2


class GPXParser(BaseActivityParser):
    """Parser for Garmin ``*.gpx`` files."""

    FORMAT = "gpx"

    def parse(self, file_path: Path) -> Activity:
        """Parse one GPX file.

        Args:
            file_path: Path to a ``*.gpx`` file exported from Garmin Connect.

        Returns:
            The parsed activity with temperature, cadence and elevation
            statistics computed from the track.

        Raises:
            ActivitySkipped: If the activity is older than ``current_year``
                or is not a running activity.
            ActivityParseError: If a mandatory element is missing.
        """
        root = ET.parse(file_path).getroot()

        activity_id = file_path.stem.rsplit("_", 1)[-1]

        metadata = root.find("ns:metadata", _NS)
        if metadata is None:
            raise ActivityParseError("missing required element ns:metadata")
        act_time = parse_datetime(required_text(metadata, "ns:time", _NS))

        if act_time.year < self.current_year:
            raise ActivitySkipped(f"Skipping old activity: {activity_id}")

        trk = root.find("ns:trk", _NS)
        if trk is None or element_text(trk, "ns:type", _NS) != _RUNNING_TYPE:
            raise ActivitySkipped(f"Not a running activity: {activity_id}")

        track_points = self._track_points(trk)
        total_sec = self._elapsed_seconds(track_points)

        coords = [(tp.lat, tp.lon) for tp in track_points]
        dist_km, num_pts = total_track_distance(coords)

        ele_series = _describe(tp.elevation for tp in track_points)
        cad_series = _describe(
            tp.cadence * _STEPS_PER_CADENCE_SAMPLE for tp in track_points if tp.cadence is not None
        )
        temp_series = _describe(tp.temperature for tp in track_points)

        return Activity(
            activity_id=activity_id,
            date=act_time.strftime(_TIMESTAMP_FORMAT),
            total_time=seconds_to_hhmmss(total_sec),
            total_sec=total_sec,
            distance_km=0.0,
            distance_by_coord_km=dist_km,
            pacing=pace_str(total_sec, dist_km),
            avg_cadence=cad_series.get("mean", np.nan),
            max_cadence=cad_series.get("max", np.nan),
            min_elevation=ele_series.get("min", np.nan),
            max_elevation=ele_series.get("max", np.nan),
            avg_temp=temp_series.get("mean", np.nan),
            min_temp=temp_series.get("min", np.nan),
            max_temp=temp_series.get("max", np.nan),
            num_track_points=num_pts,
            track_points=track_points,
        )

    @staticmethod
    def _track_points(trk: ET.Element) -> list[TrackPoint]:
        """Return the track's points, skipping any without a timestamp or position.

        Raises:
            ActivityParseError: If a point carries a coordinate that is present
                but not a finite number.
        """
        points = []
        for trkpt in trk.findall(".//ns:trkpt", _NS):
            # ele and the TrackPointExtension block are optional in GPX 1.1;
            # older devices and indoor runs omit them.
            time_text = element_text(trkpt, "ns:time", _NS)
            lat, lon = trkpt.get("lat"), trkpt.get("lon")
            if time_text is None or lat is None or lon is None:
                continue

            points.append(
                TrackPoint(
                    lat=parse_finite_float(lat, "trkpt lat"),
                    lon=parse_finite_float(lon, "trkpt lon"),
                    time=parse_datetime(time_text).strftime(_TIMESTAMP_FORMAT),
                    elevation=optional_float(trkpt, "ns:ele", _NS),
                    temperature=optional_float(trkpt, ".//ns3:atemp", _NS),
                    cadence=optional_int(trkpt, ".//ns3:cad", _NS),
                )
            )
        return points

    @staticmethod
    def _elapsed_seconds(track_points: list[TrackPoint]) -> float:
        """Return the span between the first and last point, or 0.0 if empty."""
        if not track_points:
            return 0.0
        beg = parse_datetime(track_points[0].time)
        end = parse_datetime(track_points[-1].time)
        return (end - beg).total_seconds()


def _describe(values) -> pd.Series:
    """Return pandas summary statistics over *values*, ignoring ``None``."""
    kept = [v for v in values if v is not None]
    return pd.Series(kept, dtype="float64").describe()
