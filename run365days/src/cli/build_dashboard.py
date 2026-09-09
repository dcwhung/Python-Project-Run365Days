"""Build the dashboard data file from Garmin, weight and weather sources.

Usage
-----
    run365-dashboard                     # writes src/dashboard/static/data.js
    run365-dashboard --year 2021 --points 150
    run365-dashboard --single-file       # also writes a self-contained
                                         # src/dashboard/static/run365days.html

or, without installing the package::

    python -m run365days.cli.build_dashboard --single-file

Sources (defaults, override with flags):
    data/raw/garmin/tcx/*.tcx        distance, time, calories, speed, cadence
    data/raw/garmin/gpx/*.gpx        temperature per track point
    data/raw/weight/<year>_daily_weight.txt
    data/raw/weather/*.json          HKO daily extract, hourly history, warnings
"""

import argparse
from pathlib import Path

from run365days.activities.parsers.gpx import GPXParser
from run365days.activities.parsers.tcx import TCXParser
from run365days.common import config
from run365days.dashboard.builder import (
    TRACK_POINT_LIMIT,
    build_payload,
    inline_data,
    load_jsonl,
    write_data_js,
)
from run365days.weight.analysis import parse_weight_file


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--year", type=int, default=config.DEFAULT_YEAR)
    ap.add_argument("--tcx-dir", type=Path, default=config.TCX_DIR)
    ap.add_argument("--gpx-dir", type=Path, default=config.GPX_DIR)
    ap.add_argument(
        "--weight-file",
        type=Path,
        default=None,
        help="default: data/raw/weight/<year>_daily_weight.txt",
    )
    ap.add_argument(
        "--points",
        type=int,
        default=TRACK_POINT_LIMIT,
        help=f"max track points kept per activity (default {TRACK_POINT_LIMIT})",
    )
    ap.add_argument("--out", type=Path, default=config.DASHBOARD_DATA_JS)
    ap.add_argument(
        "--single-file",
        action="store_true",
        help="also write a self-contained HTML file with the data inlined",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    weight_file = args.weight_file or config.daily_weight_file(args.year)

    print(f"Parsing TCX from {args.tcx_dir} ...")
    tcx = TCXParser(args.year).parse_all(args.tcx_dir)
    print(f"  {len(tcx)} activities")
    print(f"Parsing GPX from {args.gpx_dir} ...")
    gpx = GPXParser(args.year).parse_all(args.gpx_dir) if args.gpx_dir.exists() else []
    print(f"  {len(gpx)} activities")

    weight = parse_weight_file(weight_file, year=args.year) if weight_file.exists() else []
    print(f"Weight records: {len(weight)}")

    hko = load_jsonl(config.HKO_DAILY_JSON)
    hourly = load_jsonl(config.WEATHER_HISTORY_JSON)
    warnings = load_jsonl(config.WEATHER_WARNING_JSON)
    print(f"Weather: {len(hko)} daily rows, {len(hourly)} hourly rows, {len(warnings)} warnings")

    payload = build_payload(args.year, tcx, gpx, weight, hko, hourly, warnings, args.points)
    write_data_js(payload, args.out)
    print(f"Wrote {args.out} ({args.out.stat().st_size / 1024:,.0f} KB)")

    if args.single_file:
        out = config.DASHBOARD_SINGLE_FILE_HTML
        out.write_text(inline_data(config.DASHBOARD_INDEX_HTML.read_text(), payload))
        print(f"Wrote {out} ({out.stat().st_size / 1024:,.0f} KB)")


if __name__ == "__main__":
    main()
