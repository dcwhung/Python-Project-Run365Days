"""Export the processed data set: SQLite for the API, JSON for the static build.

Usage
-----
    run365-export                          # data/processed/run365.db + data/processed/static/
    run365-export --year 2021 --points 600
    run365-export --db /tmp/run365.db --static-dir /tmp/static

or, without installing the package::

    python -m run365days.cli.export_data
"""

import argparse
import sys
from pathlib import Path

from run365days.activities.parsers.gpx import GPXParser
from run365days.activities.parsers.tcx import TCXParser
from run365days.common import config
from run365days.dashboard.builder import load_jsonl
from run365days.export.records import build_records
from run365days.export.sqlite import write_sqlite
from run365days.export.static_json import write_static_json
from run365days.weight.analysis import parse_weight_file

DEFAULT_POINT_LIMIT = 600
"""Track points kept per run; four times the v2 dashboard's 150 for smoother routes."""


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--year", type=int, default=config.DEFAULT_YEAR)
    ap.add_argument("--tcx-dir", type=Path, default=config.TCX_DIR)
    ap.add_argument("--gpx-dir", type=Path, default=config.GPX_DIR)
    ap.add_argument("--weight-file", type=Path, default=None)
    ap.add_argument("--points", type=int, default=DEFAULT_POINT_LIMIT)
    ap.add_argument("--db", type=Path, default=config.PROCESSED_DB)
    ap.add_argument("--static-dir", type=Path, default=config.STATIC_JSON_DIR)
    ap.add_argument("--skip-db", action="store_true", help="do not write the SQLite file")
    ap.add_argument("--skip-static", action="store_true", help="do not write the JSON files")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    weight_file = args.weight_file or config.daily_weight_file(args.year)
    if not args.tcx_dir.is_dir():
        sys.exit(f"TCX directory not found: {args.tcx_dir} (set RUN365_DATA_DIR or --tcx-dir)")

    print(f"Parsing TCX from {args.tcx_dir} ...")
    tcx = TCXParser(args.year).parse_all(args.tcx_dir)
    print(f"Parsing GPX from {args.gpx_dir} ...")
    gpx = GPXParser(args.year).parse_all(args.gpx_dir) if args.gpx_dir.exists() else []
    weight = parse_weight_file(weight_file, year=args.year) if weight_file.exists() else []

    records = build_records(
        args.year,
        tcx,
        gpx,
        weight,
        load_jsonl(config.HKO_DAILY_JSON),
        load_jsonl(config.WEATHER_HISTORY_JSON),
        load_jsonl(config.WEATHER_WARNING_JSON),
        point_limit=args.points,
    )
    print(
        f"Records: {len(records.activities)} activities, {len(records.weight)} weigh-ins, "
        f"{len(records.daily_weather)} weather days, {len(records.warnings)} warnings"
    )

    if not args.skip_db:
        write_sqlite(records, args.db)
        print(f"Wrote {args.db} ({args.db.stat().st_size / 1024:,.0f} KB)")
    if not args.skip_static:
        write_static_json(records, args.static_dir)
        size = sum(p.stat().st_size for p in args.static_dir.rglob("*.json"))
        print(f"Wrote {args.static_dir} ({size / 1024:,.0f} KB)")


if __name__ == "__main__":
    main()
