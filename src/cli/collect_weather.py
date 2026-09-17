"""Collect weather data from online sources into data/raw/weather/.

Usage
-----
    run365-weather --source hourly
    run365-weather --source warnings
    run365-weather --source hko-daily --year 2021
    run365-weather --source all

or, without installing the package::

    python -m run365days.cli.collect_weather --source all

Exits non-zero when any selected source could not be collected.
"""

import argparse
import json
import logging
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import requests

from run365days.common import config
from run365days.weather.collectors import WeatherPageStructureError, hko_daily, hourly, warnings

logger = logging.getLogger(__name__)

_SOURCES = ("hourly", "warnings", "hko-daily")

# The two ways one source can die while the other two are in no trouble at all:
# a host that cannot be reached, and a page that no longer carries the landmark
# it is parsed by. Caught per source so a dead freemeteo does not also cost the
# run its HKO warnings, and reported through the exit code so that "collected
# nothing" never reads as "collected everything" to whatever ran this.
_COLLECTION_FAILURES = (requests.RequestException, WeatherPageStructureError)


def _write_jsonl(records, out_path: Path) -> None:
    """Write weather records as JSON Lines in the raw scraper column names.

    ``record.to_raw_row()`` rather than ``record.__dict__``: the export layer
    reads these files back by scraper column name, and writing dataclass field
    names instead is what made a re-collection break the pipeline -- loudly for
    the daily extract, silently for the hourly and warning files (CUI-0011).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for record in records:
            f.write(json.dumps(record.to_raw_row()) + "\n")
    print(f"  Wrote {len(records)} records to {out_path}")


def _collect_source(label: str, collect: Callable[[], list], out_path: Path) -> bool:
    """Run one collector and write whatever it returned.

    Progress goes to stdout, because it is the running commentary the user
    asked for by starting the command. A failure goes to the logger instead: it
    is a record of what the run did *not* do, and it belongs in the same stream
    as the collectors' own skip warnings rather than mixed into the progress a
    user reads as good news (AU-013).

    Args:
        label: Human-readable source name, used in the progress line.
        collect: Zero-argument call returning the records to write.
        out_path: File the records are written to.

    Returns:
        ``True`` when the source was collected and written.
    """
    print(f"\n--- Collecting {label} ---")
    try:
        records = collect()
    except _COLLECTION_FAILURES:
        logger.warning("Skipping %s: the source could not be collected", label, exc_info=True)
        return False
    _write_jsonl(records, out_path)
    return True


def run(source: str, start: str, end: str, year: int) -> int:
    """Collect the selected sources into the raw weather directory.

    Args:
        source: One of ``_SOURCES``, or ``"all"`` for every source.
        start: First day for the ranged sources, ``YYYY-MM-DD``.
        end: Last day for the ranged sources, ``YYYY-MM-DD``.
        year: Year to request from the HKO daily extract.

    Returns:
        How many of the selected sources could not be collected; zero means
        every selected source was written.
    """
    jobs = (
        (
            "hourly",
            "hourly weather history",
            lambda: hourly.fetch_range(start, end),
            config.WEATHER_HISTORY_JSON,
        ),
        (
            "warnings",
            "weather warnings",
            lambda: warnings.fetch_range(start, end),
            config.WEATHER_WARNING_JSON,
        ),
        (
            "hko-daily",
            "HKO daily weather extract",
            lambda: hko_daily.fetch_year(str(year)),
            config.HKO_DAILY_JSON,
        ),
    )
    selected = [job for job in jobs if source in (job[0], "all")]
    return sum(not _collect_source(label, collect, path) for _, label, collect, path in selected)


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

    # The collectors report skipped months and unreachable sources through the
    # logging module. With no handler configured those records fall to Python's
    # last-resort stderr writer, which drops the logger name and the level that
    # say which source went missing -- so the entry point configures one.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    start = args.start_date or f"{args.year}-01-01"
    end = args.end_date or datetime.today().strftime("%Y-%m-%d")

    if run(args.source, start, end, args.year):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
