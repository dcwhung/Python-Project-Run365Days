"""Parse Garmin KML 2.1 exports.

The KML export embeds lap statistics as an HTML table inside each lap
placemark, which is parsed with BeautifulSoup. Track points carry
coordinates and timestamps only.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

from bs4 import BeautifulSoup

from run365days.activities.models import Activity, TrackPoint
from run365days.activities.parsers.base import (
    ActivityParseError,
    ActivitySkipped,
    BaseActivityParser,
    element_text,
    ensure_finite,
    parse_finite_float,
)
from run365days.common.geo import total_track_distance
from run365days.common.time import hhmmss_to_seconds, pace_str, parse_datetime, seconds_to_hhmmss

_NS = {"ns": "http://earth.google.com/kml/2.1"}

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_RUNNING_NAME = "Running"
_LAPS_FOLDER = "Laps"
_TRACK_POINTS_FOLDER = "Track Points"
_LAP_KEY = "Lap"
_LAP_TIME_KEY = "Time"
_LAP_DISTANCE_KEY = "Distance"
_SUMMARY_ROW_COLSPAN = 2


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
            ActivitySkipped: If the file is not a running activity or is
                older than ``current_year``.
            ActivityParseError: If a mandatory element is missing, or the file
                carries no track points and therefore no start time.
        """
        root = ET.parse(file_path).getroot()

        activity_id = file_path.stem.rsplit("_", 1)[-1]
        folder = root.find("ns:Folder", _NS)

        if folder is None or _RUNNING_NAME not in (element_text(folder, "ns:name", _NS) or ""):
            raise ActivitySkipped(f"Not a running activity: {activity_id}")

        lap_rows: list[dict] = []
        track_points: list[TrackPoint] = []

        for subfolder in folder.findall("ns:Folder", _NS):
            name = element_text(subfolder, "ns:name", _NS)
            if name == _LAPS_FOLDER:
                lap_rows.extend(_parse_laps(subfolder))
            elif name == _TRACK_POINTS_FOLDER:
                track_points.extend(_parse_track_points(subfolder))

        # A KML carries no timestamp outside its track points, so a file with
        # none of them has no start time at all and cannot become an Activity.
        # That is a data problem to report, not a deliberate year filter (W-004).
        if not track_points:
            raise ActivityParseError(f"no track points in {file_path.name}")

        act_time = parse_datetime(track_points[0].time)
        if act_time.year < self.current_year:
            raise ActivitySkipped(f"Skipping old activity: {activity_id}")

        if not lap_rows:
            raise ActivityParseError(f"no laps found in {file_path.name}")

        total_sec, dist_km = _aggregate_laps(lap_rows)
        dist_by_coord, num_pts = total_track_distance([(tp.lat, tp.lon) for tp in track_points])

        return Activity(
            activity_id=activity_id,
            date=act_time.strftime(_TIMESTAMP_FORMAT),
            total_time=seconds_to_hhmmss(total_sec),
            total_sec=total_sec,
            distance_km=dist_km,
            distance_by_coord_km=dist_by_coord,
            pacing=pace_str(total_sec, dist_km),
            num_track_points=num_pts,
            track_points=track_points,
        )


def _parse_laps(subfolder: ET.Element) -> list[dict]:
    """Return one row per lap placemark, read from its embedded HTML table."""
    rows = []
    for placemark in subfolder.findall("ns:Placemark", _NS):
        name = element_text(placemark, "ns:name", _NS)
        description = element_text(placemark, "ns:description", _NS)
        if not name or not description:
            continue

        parts = name.split()
        if "Lap" not in parts[0]:
            continue

        row = {_LAP_KEY: parts[1] if len(parts) > 1 else parts[0]}
        row.update(_lap_table_cells(description))
        rows.append(row)
    return rows


def _lap_table_cells(description: str) -> dict[str, str]:
    """Return the ``label -> value`` pairs of a lap's two-column HTML table."""
    cells = {}
    soup = BeautifulSoup(description, "html.parser")
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        # A colspan=2 cell is the table's title row, not a statistic.
        if tds[0].has_attr("colspan") and int(tds[0]["colspan"]) == _SUMMARY_ROW_COLSPAN:
            continue
        if len(tds) < _SUMMARY_ROW_COLSPAN:
            continue
        cells[tds[0].text.replace(":", "").strip()] = tds[1].text.strip()
    return cells


def _parse_track_points(subfolder: ET.Element) -> list[TrackPoint]:
    """Return the folder's track points, skipping placemarks without time or coordinates.

    Raises:
        ActivityParseError: If a placemark carries a coordinate that is present
            but not a finite number.
    """
    points = []
    for placemark in subfolder.findall("ns:Placemark", _NS):
        begin = element_text(placemark, "ns:TimeSpan/ns:begin", _NS)
        raw = element_text(placemark, "ns:Point/ns:coordinates", _NS)
        if begin is None or raw is None:
            continue

        lon_lat = raw.split(", ")[0].split(",")
        if len(lon_lat) < _SUMMARY_ROW_COLSPAN:
            continue

        points.append(
            TrackPoint(
                lat=parse_finite_float(lon_lat[1], "track point latitude"),
                lon=parse_finite_float(lon_lat[0], "track point longitude"),
                time=parse_datetime(begin).strftime(_TIMESTAMP_FORMAT),
            )
        )
    return points


def _aggregate_laps(lap_rows: list[dict]) -> tuple[float, float]:
    """Return ``(total_seconds, total_km)`` summed over the lap rows.

    Time and Distance are mandatory, matching how TCX reads the same two
    numbers through ``required_text``. They are the only inputs to
    ``total_sec``, ``distance_km`` and ``pacing``, so a lap that omits one
    would publish a short total and a wrong pace with nothing in the log. A
    reported, dropped activity is the better failure (W-006).

    Args:
        lap_rows: One ``label -> value`` mapping per lap placemark.

    Returns:
        The summed seconds and kilometres.

    Raises:
        ActivityParseError: If any lap omits Time or Distance, or either is
            unreadable or not finite.
    """
    total_sec = 0.0
    total_km = 0.0
    for row in lap_rows:
        for key in (_LAP_TIME_KEY, _LAP_DISTANCE_KEY):
            if not row.get(key):
                raise ActivityParseError(f"lap {row.get(_LAP_KEY)} is missing {key}")
        try:
            lap_sec = hhmmss_to_seconds(row[_LAP_TIME_KEY])
            lap_km = float(row[_LAP_DISTANCE_KEY].split()[0])
        except (IndexError, ValueError) as exc:
            raise ActivityParseError(
                f"lap {row.get(_LAP_KEY)} has an unreadable total: {exc}"
            ) from exc
        total_sec += lap_sec
        total_km += ensure_finite(lap_km, f"lap {row.get(_LAP_KEY)} {_LAP_DISTANCE_KEY}")
    # hhmmss_to_seconds is bounded by strptime, but the kilometres are not, and a
    # non-finite total reaches the two writers as two different answers (CUI-0001).
    return total_sec, ensure_finite(total_km, "total distance")
