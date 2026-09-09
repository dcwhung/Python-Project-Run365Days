"""Collect weather data from online sources into data/raw/weather/.

Usage
-----
    run365-weather --source hourly
    run365-weather --source warnings
    run365-weather --source hko-daily --year 2021
    run365-weather --source all

or, without installing the package::

    python -m run365days.cli.collect_weather --source all
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

from run365days.common import config
from run365days.weather.collectors import hko_daily, hourly, warnings

_SOURCES = ("hourly", "warnings", "hko-daily")


def _write_jsonl(records, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for record in records:
            f.write(json.dumps(record.__dict__) + "\n")
    print(f"  Wrote {len(records)} records to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--source", choices=[*_SOURCES, "all"], default="all")
    parser.add_argument("--year", type=int, default=config.DEFAULT_YEAR)
    parser.add_argument(
        "--start-date",
        default=None,
        help="Override start date (YYYY-MM-DD). Defaults to Jan 1 of --year.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Override end date (YYYY-MM-DD). Defaults to today.",
    )
    args = parser.parse_args()

    start = args.start_date or f"{args.year}-01-01"
    end = args.end_date or datetime.today().strftime("%Y-%m-%d")

    if args.source in ("hourly", "all"):
        print("\n--- Collecting hourly weather history ---")
        _write_jsonl(hourly.fetch_range(start, end), config.WEATHER_HISTORY_JSON)

    if args.source in ("warnings", "all"):
        print("\n--- Collecting weather warnings ---")
        _write_jsonl(warnings.fetch_range(start, end), config.WEATHER_WARNING_JSON)

    if args.source in ("hko-daily", "all"):
        print("\n--- Collecting HKO daily weather extract ---")
        _write_jsonl(hko_daily.fetch_year(str(args.year)), config.HKO_DAILY_JSON)


if __name__ == "__main__":
    main()
