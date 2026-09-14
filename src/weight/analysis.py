"""Daily weight parsing.

Only the parse step lives here. The module used to publish four more
functions -- ``build_dataframe``, ``monthly_summary``, ``weekday_summary`` and
``describe_weight`` -- ported from the half of
``legacy/05_GetDailyWeightSummary.py`` that printed tables to a console and
drew matplotlib figures. Ported into a package they became public API with no
caller: ``cli/export_data.py`` imports ``parse_weight_file`` and nothing else,
and the API reads weight through ``WeightRecord``. Three of the four never had
a test; the fourth was reachable only from its own (AU-029).

They were also the only reason this package imported pandas and numpy, so
removing them leaves ``weight`` importable from the stdlib alone --
``tests/test_weight_analysis.py`` keeps that true, on the same dependency list
``tests/test_api_imports.py`` holds the API to.
"""

import re
from datetime import datetime, timedelta
from pathlib import Path

from run365days.common.config import BODY_HEIGHT_CM, LBS_TO_KG
from run365days.weight.models import WeightRecord

__all__ = [
    "WeightRecord",
    "parse_weight_file",
]


_DATED_LINE = re.compile(r"([\d.]+)\s*lbs\s*\((\d+)/(\d+)\)")
_UNDATED_LINE = re.compile(r"^\s*([\d.]+)\s*lbs\s*$")


def _read_weigh_in(
    line: str, year: int, last_date: datetime | None
) -> tuple[float, datetime] | None:
    """Read one file line as ``(weight_lbs, date)``, or ``None`` if it is not a weigh-in.

    An undated weight is dated the day after *last_date*; with no dated record
    before it there is nothing to count from, so the line is not a weigh-in.
    """
    dated = _DATED_LINE.search(line)
    if dated:
        day, month = int(dated.group(2)), int(dated.group(3))
        return float(dated.group(1)), datetime(year, month, day)
    undated = _UNDATED_LINE.match(line)
    if not undated or last_date is None:
        return None
    return float(undated.group(1)), last_date + timedelta(days=1)


def parse_weight_file(
    file_path: Path,
    year: int | None = None,
    height_cm: float = BODY_HEIGHT_CM,
) -> list[WeightRecord]:
    """Parse a daily weight text file.

    Each line is expected to match ``<weight> lbs (<day>/<month>)``, for
    example ``154.8 lbs (1/5)``. A line with a weight but no date (the file's
    final entry is often written this way) is taken as the day after the
    previous dated record. Lines that match neither form are skipped.

    ``day_number`` counts the weigh-ins kept, not the lines read: numbering by
    line meant a header or a blank line shifted every number after it, so a
    file starting with one comment produced day numbers 2 and 3 for the first
    and second weigh-ins (AU-007).

    Args:
        file_path: Path to the text file.
        year: Calendar year for the day/month values (default: current year).
        height_cm: Height used for the BMI column.

    Returns:
        Records in file order.
    """
    if year is None:
        year = datetime.today().year

    records: list[WeightRecord] = []
    last_date: datetime | None = None
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            weigh_in = _read_weigh_in(line, year, last_date)
            if weigh_in is None:
                continue
            weight_lbs, last_date = weigh_in
            weight_kg = weight_lbs * LBS_TO_KG
            records.append(
                WeightRecord(
                    day_number=len(records) + 1,
                    date=last_date.strftime("%Y-%m-%d"),
                    weight_lbs=weight_lbs,
                    weight_kg=round(weight_kg, 2),
                    bmi=round(weight_kg / ((height_cm / 100) ** 2), 2),
                )
            )
    return records
