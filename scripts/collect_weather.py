"""CLI entry point for collecting weather data from online sources.

Usage
-----
    python scripts/collect_weather.py --source hourly
    python scripts/collect_weather.py --source warnings
    python scripts/collect_weather.py --source hko-daily --year 2021
    python scripts/collect_weather.py --source all
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from run365days import config
from run365days.collectors import hko_daily, weather, weather_warnings


def _write_jsonl(records, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for r in records:
            f.write(json.dumps(r.__dict__) + "\n")
    print(f"  Wrote {len(records)} records to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect weather data from online sources.")
    parser.add_argument(
        "--source",
        choices=["hourly", "warnings", "hko-daily", "all"],
        default="all",
    )
    parser.add_argument("--year", type=int, default=datetime.today().year)
    parser.add_argument(
        "--start-date",
        default=None,
        help="Override start date (YYYY-MM-DD). Defaults to Jan 1 of --year.",
    )
    args = parser.parse_args()

    start = args.start_date or f"{args.year}-01-01"
    end = datetime.today().strftime("%Y-%m-%d")

    if args.source in ("hourly", "all"):
        print("\n--- Collecting hourly weather history ---")
        records = weather.fetch_range(start, end)
        _write_jsonl(records, config.WEATHER_HISTORY_JSON)

    if args.source in ("warnings", "all"):
        print("\n--- Collecting weather warnings ---")
        records = weather_warnings.fetch_range(start, end)
        _write_jsonl(records, config.WEATHER_WARNING_JSON)

    if args.source in ("hko-daily", "all"):
        print("\n--- Collecting HKO daily weather extract ---")
        records = hko_daily.fetch_year(str(args.year))
        _write_jsonl(records, config.HKO_DAILY_JSON)


if __name__ == "__main__":
    main()
