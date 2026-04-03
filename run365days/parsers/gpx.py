import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import pandas as pd

from run365days.models.activity import Activity, TrackPoint
from run365days.parsers.base import BaseActivityParser
from run365days.utils.geo import total_track_distance
from run365days.utils.time import parse_datetime, pace_str, seconds_to_hhmmss

_NS = {
    "ns": "http://www.topografix.com/GPX/1/1",
    "ns3": "http://www.garmin.com/xmlschemas/TrackPointExtension/v1",
}


class GPXParser(BaseActivityParser):
    FORMAT = "gpx"

    def parse(self, file_path: Path) -> Activity:
        tree = ET.parse(file_path)
        root = tree.getroot()

        activity_id = file_path.stem.rsplit("_", 1)[-1]

        metadata = root.find("ns:metadata", _NS)
        act_time = parse_datetime(metadata.find("ns:time", _NS).text)

        if act_time.year < self.current_year:
            raise ValueError(f"Skipping old activity: {activity_id}")

        trk = root.find("ns:trk", _NS)
        if trk is None or trk.find("ns:type", _NS).text != "running":
            raise ValueError(f"Not a running activity: {activity_id}")

        trkpts = trk.findall(".//ns:trkpt", _NS)
        track_points, beg_time, end_time = [], None, None

        for trkpt in trkpts:
            t = parse_datetime(trkpt.find("ns:time", _NS).text)
            if beg_time is None:
                beg_time = t
            end_time = t

            track_points.append(
                TrackPoint(
                    lat=float(trkpt.get("lat")),
                    lon=float(trkpt.get("lon")),
                    time=t.strftime("%Y-%m-%d %H:%M:%S"),
                    elevation=float(trkpt.find("ns:ele", _NS).text),
                    temperature=float(trkpt.find(".//ns3:atemp", _NS).text),
                    cadence=int(trkpt.find(".//ns3:cad", _NS).text),
                )
            )

        total_sec = (
            (end_time - beg_time).total_seconds() if beg_time and end_time else 0.0
        )
        coords = [(tp.lat, tp.lon) for tp in track_points]
        dist_km, num_pts = total_track_distance(coords)

        elevations = [tp.elevation for tp in track_points if tp.elevation is not None]
        ele_series = pd.Series(elevations, dtype="float64").describe()

        cad_values = [tp.cadence * 2 for tp in track_points if tp.cadence is not None]
        cad_series = pd.Series(cad_values, dtype="float64").describe()

        temps = [tp.temperature for tp in track_points if tp.temperature is not None]
        temp_series = pd.Series(temps, dtype="float64").describe()

        return Activity(
            activity_id=activity_id,
            date=act_time.strftime("%Y-%m-%d %H:%M:%S"),
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
