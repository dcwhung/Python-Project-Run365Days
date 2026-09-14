"""Collect weather data from online sources into data/raw/weather/.

Usage
-----
    run365-weather --source hourly
    run365-weather --source warnings
    run365-weather --source hko-daily --year 2021
    run365-weather --source sun-moon --year 2021
    run365-weather --source all

or, without installing the package::

    python -m run365days.cli.collect_weather --source all

Exit status
-----------
Non-zero when a source collected nothing at all: its file is left alone rather
than blanked, every other source still runs, and the closing message names each
empty one.

Where the output goes
---------------------
Three destinations, split by what the line is rather than by habit:

* **Progress** -- which source is starting, how many records it produced, where
  they were written -- is ``print`` to stdout. It is the answer the operator
  asked for by running the command, it is wanted on a completely healthy run,
  and it is the same narrative ``run365-activities`` and ``run365-export``
  print. Routing it through ``logging`` would make the normal case depend on a
  handler being configured.
* **Per-day and per-row anomalies** -- a skipped month, a dropped row, a cell in
  an unexpected unit -- belong to the collectors and stay on the collectors'
  module loggers, at WARNING. They are conditional, they are aimed at whoever
  is diagnosing a scrape, and one full-year run can emit hundreds; mixing them
  into stdout would bury the four lines above. This CLI configures no handler,
  so they reach stderr through the logging default and an operator who wants
  them formatted or filtered can configure that without editing this file.
* **A run that produced nothing** ends in ``sys.exit`` with a message, which is
  stderr plus a non-zero status -- the only signal a caller or a CI step can act
  on (AU-002, S-005).
"""

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from run365days.common import config
from run365days.weather.collectors import hko_daily, hourly, sun_moon, warnings
from run365days.weather.models import DailyWeather, HourlyWeather, SunMoon, WeatherWarning


def _sources() -> dict:
    """Return each source's progress label, destination file and collector.

    Built per call rather than at import time so the destinations are read from
    :mod:`config` when the run starts, not when the module is first loaded.

    Returns:
        ``{name: (label, destination, collect(start, end, year))}``, in the
        order ``--source all`` runs them.
    """
    return {
        "hourly": (
            "hourly weather history",
            config.WEATHER_HISTORY_JSON,
            lambda start, end, year: hourly.fetch_range(start, end),
        ),
        "warnings": (
            "weather warnings",
            config.WEATHER_WARNING_JSON,
            lambda start, end, year: warnings.fetch_range(start, end),
        ),
        "hko-daily": (
            "HKO daily weather extract",
            config.HKO_DAILY_JSON,
            lambda start, end, year: hko_daily.fetch_year(str(year)),
        ),
        "sun-moon": (
            "sun and moon rise/set history",
            config.SUN_MOON_JSON,
            lambda start, end, year: sun_moon.fetch_year(str(year)),
        ),
    }


def _write_jsonl(
    records: Sequence[DailyWeather | HourlyWeather | SunMoon | WeatherWarning], out_path: Path
) -> None:
    """Write weather records as JSON Lines in the raw scraper column names.

    ``record.to_raw_row()`` rather than ``record.__dict__``: the export layer
    reads these files back by scraper column name, and writing dataclass field
    names instead is what made a re-collection break the pipeline -- loudly for
    the daily extract, silently for the hourly and warning files (CUI-0011).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record.to_raw_row()) + "\n")
    print(f"  Wrote {len(records)} records to {out_path}")


def run(source: str, start: str, end: str, year: int) -> dict[str, Path]:
    """Collect each requested source and write it as JSON Lines.

    Args:
        source: One source name, or ``"all"``.
        start: First day of the range, ``YYYY-MM-DD``.
        end: Last day of the range, ``YYYY-MM-DD``.
        year: Target year for the HKO daily extract.

    Returns:
        The sources that collected nothing, mapped to the file left untouched.
        Empty when every source ran produced records.
    """
    sources = _sources()
    targets = list(sources) if source == "all" else [source]
    empty: dict[str, Path] = {}
    for name in targets:
        label, out_path, collect = sources[name]
        print(f"\n--- Collecting {label} ---")
        records = collect(start, end, year)
        if not records:
            # Zero needs no threshold to judge. A short scrape is normal -- the
            # collectors drop a row whose cell is unreadable and say so -- but
            # nothing at all means the page moved or the range is wrong, and
            # every guard added since CUI-0012 turns that into an empty list
            # rather than a crash. Writing here would blank the committed
            # history on the way to reporting success, so skip the write and
            # let main() report it.
            empty[name] = out_path
            print(f"  Collected nothing; leaving {out_path} untouched")
            continue
        _write_jsonl(records, out_path)
    return empty


def main() -> None:
    sources = _sources()
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--source", choices=[*sources, "all"], default="all")
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

    empty = run(args.source, start, end, args.year)
    if empty:
        # Reported once every source has run rather than on the first empty one:
        # the three scrape different sites, so a single pass should say whether
        # the whole run is broken or only one source is. Re-run with logging at
        # WARNING to see which days and rows the collectors dropped and why.
        named = ", ".join(f"{name} ({path})" for name, path in empty.items())
        sys.exit(
            f"Collected no records for {start}..{end} from {named}; "
            f"refusing to overwrite the committed history with an empty file"
        )


if __name__ == "__main__":
    main()
