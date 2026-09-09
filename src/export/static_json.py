"""Write :class:`~run365days.export.records.ExportRecords` as split JSON files.

The static build of the dashboard fetches these directly, so the layout is
chosen for the browser: one small file per collection and one file per
track so a view only downloads what it renders.

Layout under the output directory::

    meta.json            {year, generated_at, counts}
    activities.json      [activity, ...]   (no tracks)
    weight.json          [weigh-in, ...]
    weather.json         [daily row, ...]
    warnings.json        [warning, ...]
    tracks/<id>.json     {columns, rows}
"""

import json
import shutil
from pathlib import Path

from run365days.export.records import TRACK_COLUMNS, ExportRecords

ACTIVITIES_FILE = "activities.json"
WEIGHT_FILE = "weight.json"
WEATHER_FILE = "weather.json"
WARNINGS_FILE = "warnings.json"
META_FILE = "meta.json"
TRACKS_DIR = "tracks"


def _dump(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, separators=(",", ":"), allow_nan=False))


def write_static_json(records: ExportRecords, out_dir: Path) -> None:
    """Write every collection under *out_dir*, replacing previous output.

    Args:
        records: The shaped export.
        out_dir: Destination directory; recreated from scratch.
    """
    if out_dir.exists():
        shutil.rmtree(out_dir)
    tracks_dir = out_dir / TRACKS_DIR
    tracks_dir.mkdir(parents=True)

    _dump(
        out_dir / META_FILE,
        {
            "year": records.year,
            "generated_at": records.generated_at,
            "counts": {
                "activities": len(records.activities),
                "weight": len(records.weight),
                "weather": len(records.daily_weather),
                "warnings": len(records.warnings),
            },
        },
    )
    _dump(out_dir / ACTIVITIES_FILE, records.activities)
    _dump(out_dir / WEIGHT_FILE, records.weight)
    _dump(out_dir / WEATHER_FILE, records.daily_weather)
    _dump(out_dir / WARNINGS_FILE, records.warnings)
    for activity_id, rows in records.tracks.items():
        _dump(tracks_dir / f"{activity_id}.json", {"columns": list(TRACK_COLUMNS), "rows": rows})
