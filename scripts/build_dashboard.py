"""Build the dashboard data file from Garmin, weight and weather sources.

Usage
-----
    python scripts/build_dashboard.py                  # writes dashboard/data.js
    python scripts/build_dashboard.py --year 2021 --points 150
    python scripts/build_dashboard.py --single-file    # also writes a self-contained
                                                       # dashboard/run365days.html

Sources (defaults, override with flags):
    Garmin/tcx/bak/*.tcx     distance, time, calories, speed, cadence
    Garmin/gpx/bak/*.gpx     temperature per track point
    2021_DailyWeight.txt     daily weight
    Weather/*.json           HKO daily extract, hourly history, warnings
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from run365days import config
from run365days.analysis.weight import parse_weight_file
from run365days.export.dashboard import (
    build_payload,
    inline_data,
    load_jsonl,
    write_data_js,
)
from run365days.parsers.gpx import GPXParser
from run365days.parsers.tcx import TCXParser

DASHBOARD_DIR = config.ROOT_DIR / "dashboard"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--year", type=int, default=2021)
    ap.add_argument("--tcx-dir", type=Path, default=config.TCX_DIR / "bak")
    ap.add_argument("--gpx-dir", type=Path, default=config.GPX_DIR / "bak")
    ap.add_argument("--weight-file", type=Path, default=None,
                    help="default: <root>/<year>_DailyWeight.txt")
    ap.add_argument("--points", type=int, default=150,
                    help="max track points kept per activity (default 150)")
    ap.add_argument("--out", type=Path, default=DASHBOARD_DIR / "data.js")
    ap.add_argument("--single-file", action="store_true",
                    help="also write dashboard/run365days.html with data inlined")
    args = ap.parse_args()

    weight_file = args.weight_file or config.ROOT_DIR / f"{args.year}_DailyWeight.txt"

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
    size_kb = args.out.stat().st_size / 1024
    print(f"Wrote {args.out} ({size_kb:,.0f} KB)")

    if args.single_file:
        html_path = DASHBOARD_DIR / "index.html"
        out = DASHBOARD_DIR / "run365days.html"
        out.write_text(inline_data(html_path.read_text(), payload))
        print(f"Wrote {out} ({out.stat().st_size / 1024:,.0f} KB)")


if __name__ == "__main__":
    main()
