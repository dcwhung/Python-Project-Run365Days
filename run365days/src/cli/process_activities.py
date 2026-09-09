"""Parse Garmin activity files and write normalised JSON Lines.

Usage
-----
    run365-activities --format gpx
    run365-activities --format kml
    run365-activities --format tcx
    run365-activities --format all --year 2021

or, without installing the package::

    python -m run365days.cli.process_activities --format all
"""

import argparse
import json
from pathlib import Path

from run365days.activities.parsers.gpx import GPXParser
from run365days.activities.parsers.kml import KMLParser
from run365days.activities.parsers.tcx import TCXParser
from run365days.common import config

_PARSERS = {
    "gpx": (GPXParser, config.GPX_DIR, config.GPX_JSON),
    "kml": (KMLParser, config.KML_DIR, config.KML_JSON),
    "tcx": (TCXParser, config.TCX_DIR, config.TCX_JSON),
}


def _activities_to_jsonl(activities, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for activity in activities:
            record = {k: v for k, v in activity.__dict__.items() if k != "track_points"}
            f.write(json.dumps(record) + "\n")
    print(f"  Wrote {len(activities)} records to {out_path}")


def run(fmt: str, year: int) -> None:
    targets = list(_PARSERS) if fmt == "all" else [fmt]
    for name in targets:
        parser_cls, directory, out_json = _PARSERS[name]
        print(f"\n--- Processing {name.upper()} files from {directory} ---")
        if not directory.exists():
            print(f"  Directory not found, skipping: {directory}")
            continue
        activities = parser_cls(current_year=year).parse_all(directory)
        print(f"  Parsed {len(activities)} activities")
        _activities_to_jsonl(activities, out_json)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--format",
        choices=[*_PARSERS, "all"],
        default="all",
        help="Activity file format to process (default: all)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=config.DEFAULT_YEAR,
        help="Target year; activities from other years are skipped",
    )
    args = parser.parse_args()
    run(args.format, args.year)


if __name__ == "__main__":
    main()
