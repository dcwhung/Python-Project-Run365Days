"""CLI entry point for processing Garmin activity files.

Usage
-----
    python scripts/process.py --format gpx
    python scripts/process.py --format kml
    python scripts/process.py --format tcx
    python scripts/process.py --format all
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent))

from run365days import config
from run365days.parsers.gpx import GPXParser
from run365days.parsers.kml import KMLParser
from run365days.parsers.tcx import TCXParser


def _activities_to_json(activities, out_path: Path) -> None:
    records = []
    for a in activities:
        rec = {k: v for k, v in a.__dict__.items() if k != "track_points"}
        records.append(rec)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    print(f"  Wrote {len(records)} records to {out_path}")


def _run(fmt: str, year: int) -> None:
    parsers = {
        "gpx": (GPXParser, config.GPX_DIR, config.GPX_JSON),
        "kml": (KMLParser, config.KML_DIR, config.KML_JSON),
        "tcx": (TCXParser, config.TCX_DIR, config.TCX_JSON),
    }

    targets = list(parsers.keys()) if fmt == "all" else [fmt]

    for name in targets:
        cls, directory, out_json = parsers[name]
        print(f"\n--- Processing {name.upper()} files from {directory} ---")
        if not directory.exists():
            print(f"  Directory not found, skipping: {directory}")
            continue
        parser = cls(current_year=year)
        activities = parser.parse_all(directory)
        print(f"  Parsed {len(activities)} activities")
        _activities_to_json(activities, out_json)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse Garmin activity files and write normalised JSON."
    )
    parser.add_argument(
        "--format",
        choices=["gpx", "kml", "tcx", "all"],
        default="all",
        help="Activity file format to process (default: all)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=datetime.today().year,
        help="Target year; activities from other years are skipped",
    )
    args = parser.parse_args()
    _run(args.format, args.year)


if __name__ == "__main__":
    main()
