"""Parse Garmin activity files and write normalised JSON Lines.

Usage
-----
    run365-activities --format gpx
    run365-activities --format kml
    run365-activities --format tcx
    run365-activities --format all --year 2021

or, without installing the package::

    python -m run365days.cli.process_activities --format all

Exit status
-----------
Non-zero when a format's directory exists but yields no activities at all: the
file is left alone rather than blanked, every other format still runs, and the
closing message names each empty source. A directory that is simply absent is
skipped without failing.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from run365days.activities.parsers.gpx import GPXParser
from run365days.activities.parsers.kml import KMLParser
from run365days.activities.parsers.tcx import TCXParser
from run365days.common import config

_PARSERS = {
    "gpx": (GPXParser, config.GPX_DIR, config.GPX_JSON),
    "kml": (KMLParser, config.KML_DIR, config.KML_JSON),
    "tcx": (TCXParser, config.TCX_DIR, config.TCX_JSON),
}


def _to_builtin(value: Any) -> Any:
    """JSON fallback: numpy scalars (int64, float64) become Python numbers."""
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _activities_to_jsonl(activities, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for activity in activities:
            record = {k: v for k, v in activity.__dict__.items() if k != "track_points"}
            f.write(json.dumps(record, default=_to_builtin) + "\n")
    print(f"  Wrote {len(activities)} records to {out_path}")


def run(fmt: str, year: int) -> dict[str, Path]:
    """Parse each requested format and write its records as JSON Lines.

    Args:
        fmt: One format name, or ``"all"``.
        year: Target year; activities from other years are skipped.

    Returns:
        The formats whose directory existed but yielded no activities, mapped
        to that directory. Empty when every processed format produced records.
    """
    targets = list(_PARSERS) if fmt == "all" else [fmt]
    empty: dict[str, Path] = {}
    for name in targets:
        parser_cls, directory, out_json = _PARSERS[name]
        print(f"\n--- Processing {name.upper()} files from {directory} ---")
        if not directory.exists():
            # An absent directory says this format was never exported here, not
            # that the parse failed, so --format all stays usable in an
            # environment that only ships one of the three.
            print(f"  Directory not found, skipping: {directory}")
            continue
        activities = parser_cls(current_year=year).parse_all(directory)
        print(f"  Parsed {len(activities)} activities")
        if not activities:
            # Zero is the one figure that needs no threshold to judge. A short
            # parse is normal -- the KML folder yields 357 of 365 because eight
            # indoor runs carry no track points (W-004) -- but nothing at all
            # means the directory or the year is wrong. Writing here would blank
            # the last good file on the way to a failure, so skip the write and
            # let main() report it.
            empty[name] = directory
            print(f"  No activities parsed; leaving {out_json} untouched")
            continue
        _activities_to_jsonl(activities, out_json)
    return empty


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
    empty = run(args.format, args.year)
    if empty:
        # Reported once every format has run rather than on the first empty one:
        # the three are independent, so a single pass should say whether the
        # whole data directory is wrong or only one folder is. Re-run with
        # logging at WARNING to see which files the parsers dropped and why.
        sources = ", ".join(f"{name.upper()} ({directory})" for name, directory in empty.items())
        sys.exit(
            f"No activities parsed for year {args.year} from {sources}; "
            f"refusing to write an empty JSONL file"
        )


if __name__ == "__main__":
    main()
