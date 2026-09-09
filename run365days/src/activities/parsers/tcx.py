import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from run365days.activities.models import Activity, TrackPoint
from run365days.activities.parsers.base import BaseActivityParser
from run365days.common.geo import total_track_distance
from run365days.common.time import pace_str, parse_datetime, seconds_to_hhmmss

_NS = {
    "ns": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2",
    "ns3": "http://www.garmin.com/xmlschemas/ActivityExtension/v2",
}


class TCXParser(BaseActivityParser):
    FORMAT = "tcx"

    def parse(self, file_path: Path) -> Activity:
        tree = ET.parse(file_path)
        root = tree.getroot()

        activity_id = file_path.stem.rsplit("_", 1)[-1]
        activity = root.find("ns:Activities", _NS).find("ns:Activity", _NS)
        act_time = parse_datetime(activity.find("ns:Id", _NS).text)

        if act_time.year < self.current_year:
            raise ValueError(f"Skipping old activity: {activity_id}")

        if "Running" not in activity.get("Sport", ""):
            raise ValueError(f"Not a running activity: {activity_id}")

        track_points = []
        lap_rows = []

        for lap in activity.findall("ns:Lap", _NS):
            lap_rows.append(
                {
                    "Time": float(lap.find("ns:TotalTimeSeconds", _NS).text),
                    "Distance": float(lap.find("ns:DistanceMeters", _NS).text),
                    "MaxSpeed": float(lap.find("ns:MaximumSpeed", _NS).text),
                    "Calories": int(lap.find("ns:Calories", _NS).text),
                }
            )

            trk = lap.find("ns:Track", _NS)
            for trkpt in trk.findall(".//ns:Trackpoint", _NS):
                pos = trkpt.find(".//ns:Position", _NS)
                lat = (
                    float(pos.find(".//ns:LatitudeDegrees", _NS).text) if pos is not None else None
                )
                lon = (
                    float(pos.find(".//ns:LongitudeDegrees", _NS).text) if pos is not None else None
                )

                spd_el = trkpt.find(".//ns3:Speed", _NS)
                cad_el = trkpt.find(".//ns3:RunCadence", _NS)

                track_points.append(
                    TrackPoint(
                        lat=lat,
                        lon=lon,
                        time=parse_datetime(trkpt.find("ns:Time", _NS).text).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        elevation=float(trkpt.find("ns:AltitudeMeters", _NS).text),
                        distance_m=float(trkpt.find("ns:DistanceMeters", _NS).text),
                        speed=float(spd_el.text) if spd_el is not None else None,
                        cadence=int(cad_el.text) if cad_el is not None else None,
                    )
                )

        lap_df = pd.DataFrame(lap_rows)
        total_sec = lap_df["Time"].sum()
        dist_km = round(lap_df["Distance"].sum() / 1000, 2)
        calories = int(lap_df["Calories"].sum())

        coords = [(tp.lat, tp.lon) for tp in track_points]
        dist_by_coord, num_pts = total_track_distance(coords)

        return Activity(
            activity_id=activity_id,
            date=act_time.strftime("%Y-%m-%d %H:%M:%S"),
            total_time=seconds_to_hhmmss(total_sec),
            total_sec=total_sec,
            distance_km=dist_km,
            distance_by_coord_km=dist_by_coord,
            pacing=pace_str(total_sec, dist_km),
            calories=calories,
            num_track_points=num_pts,
            track_points=track_points,
        )
