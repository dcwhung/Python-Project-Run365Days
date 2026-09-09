"""Parse Garmin KML 2.1 exports.

The KML export embeds lap statistics as an HTML table inside each lap
placemark, which is parsed with BeautifulSoup. Track points carry
coordinates and timestamps only.
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from run365days.activities.models import Activity, TrackPoint
from run365days.activities.parsers.base import BaseActivityParser
from run365days.common.geo import total_track_distance
from run365days.common.time import pace_str, parse_datetime, seconds_to_hhmmss

_NS = {"ns": "http://earth.google.com/kml/2.1"}


class KMLParser(BaseActivityParser):
    """Parser for Garmin ``*.kml`` files."""

    FORMAT = "kml"

    def parse(self, file_path: Path) -> Activity:
        """Parse one KML file.

        Args:
            file_path: Path to a ``*.kml`` file exported from Garmin Connect.

        Returns:
            The parsed activity with lap totals and the coordinate track.

        Raises:
            ValueError: If the file is not a running activity or is older
                than ``current_year``.
        """
        tree = ET.parse(file_path)
        root = tree.getroot()

        activity_id = file_path.stem.rsplit("_", 1)[-1]
        folder = root.find("ns:Folder", _NS)

        if folder is None or "Running" not in folder.find("ns:name", _NS).text:
            raise ValueError(f"Not a running activity: {activity_id}")

        act_time = None
        track_points = []
        lap_rows = []

        for subfolder in folder.findall("ns:Folder", _NS):
            name = subfolder.find("ns:name", _NS).text

            if name == "Laps":
                for placemark in subfolder.findall("ns:Placemark", _NS):
                    lap_name = placemark.find("ns:name", _NS).text.split()
                    if "Lap" not in lap_name[0]:
                        continue

                    row = {"Lap": lap_name[1]}
                    bs = BeautifulSoup(
                        placemark.find("ns:description", _NS).text.strip(),
                        "html.parser",
                    )
                    for tr in bs.find_all("tr"):
                        tds = tr.find_all("td")
                        if tds and not (tds[0].has_attr("colspan") and int(tds[0]["colspan"]) == 2):
                            row[tds[0].text.replace(":", "")] = tds[1].text.strip()
                    lap_rows.append(row)

            elif name == "Track Points":
                for placemark in subfolder.findall("ns:Placemark", _NS):
                    beg = parse_datetime(
                        placemark.find("ns:TimeSpan", _NS).find("ns:begin", _NS).text
                    )
                    if act_time is None:
                        act_time = beg

                    raw = (
                        placemark.find("ns:Point", _NS)
                        .find("ns:coordinates", _NS)
                        .text.split(", ")[0]
                        .split(",")
                    )
                    lat, lon = float(raw[1]), float(raw[0])
                    track_points.append(
                        TrackPoint(
                            lat=lat,
                            lon=lon,
                            time=beg.strftime("%Y-%m-%d %H:%M:%S"),
                        )
                    )

        if act_time is None or act_time.year < self.current_year:
            raise ValueError(f"Skipping activity: {activity_id}")

        lap_df = pd.DataFrame(lap_rows)
        lap_df["Time"] = lap_df["Time"].apply(
            lambda v: (datetime.strptime(v, "%H:%M:%S") - datetime(1900, 1, 1)).total_seconds()
        )
        for col in ("Distance", "Elevation Gain", "Elevation Loss", "Max Speed"):
            lap_df[col] = lap_df[col].apply(lambda v: float(v.split()[0]))

        total_sec = lap_df["Time"].sum()
        dist_km = lap_df["Distance"].sum()

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
            num_track_points=num_pts,
            track_points=track_points,
        )
