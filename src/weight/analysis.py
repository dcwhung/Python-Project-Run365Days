"""Daily weight analysis: parsing, derived metrics, and summaries."""

import re
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from run365days.common.config import BODY_HEIGHT_CM, LBS_TO_KG
from run365days.weight.models import WeightRecord

__all__ = [
    "WeightRecord",
    "parse_weight_file",
    "build_dataframe",
    "monthly_summary",
    "weekday_summary",
    "describe_weight",
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


def _column(daily: list[WeightRecord | None], field: str) -> list[float]:
    """Read one field off each day's weigh-in, ``NaN`` for a day without one."""
    return [getattr(record, field) if record is not None else np.nan for record in daily]


def build_dataframe(
    records: list[WeightRecord],
    year: int | None = None,
) -> pd.DataFrame:
    """Expand weigh-ins into a full-year table with one row per day.

    ``Weight_(kg)`` and ``BMI`` are the values :func:`parse_weight_file`
    already derived, not a second calculation. Re-deriving them here meant a
    second copy of the height and the pound-to-kilogram factor, and the copy
    could not see the ``height_cm`` the caller passed to the parse step, so a
    non-default height produced two different BMIs for one weigh-in (AU-006).
    This step therefore takes no height at all: there is nothing left to
    configure in two places.

    Args:
        records: Parsed weigh-ins.
        year: Calendar year to cover (default: current year).

    Returns:
        A DataFrame with ``Day``, ``Date``, ``Weight_(lbs)``, ``Month``,
        ``Weekday``, ``Weight_(kg)``, ``BMI``, ``+/-`` (direction versus the
        previous day) and ``%`` (percentage change). Missing days are filled
        with ``"/"``.
    """
    if year is None:
        year = datetime.today().year

    date_range = pd.date_range(f"{year}-01-01", f"{year}-12-31")
    by_date = {r.date: r for r in records}
    daily = [by_date.get(d.strftime("%Y-%m-%d")) for d in date_range]

    df = pd.DataFrame(
        {
            "Day": range(1, len(date_range) + 1),
            "Date": date_range,
            "Weight_(lbs)": _column(daily, "weight_lbs"),
        }
    )
    df["Month"] = df["Date"].dt.month
    df["Weekday"] = df.apply(
        lambda row: f"{row['Date'].weekday()} - {row['Date'].strftime('%A').upper()[:3]}",
        axis=1,
    )
    df["Weight_(kg)"] = _column(daily, "weight_kg")
    df["BMI"] = _column(daily, "bmi")
    df["+/-"] = (
        df["Weight_(lbs)"]
        .diff()
        .map(lambda x: "+" if x > 0 else "-" if x < 0 else "/" if pd.notna(x) else "/")
    )
    df["%"] = df["Weight_(lbs)"].pct_change().mul(100)
    return df.fillna("/")


def monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Count up / down / unchanged days per month.

    Args:
        df: Output of :func:`build_dataframe`.

    Returns:
        A pivot table indexed by ``+/-`` with one column per month.
    """
    grp = (
        df.groupby(["Month", "+/-"])[["Weight_(lbs)"]]
        .count()
        .reset_index()
        .pivot(index="+/-", columns="Month", values="Weight_(lbs)")
    )
    return grp


def weekday_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Count up / down / unchanged days per weekday.

    Args:
        df: Output of :func:`build_dataframe`.

    Returns:
        A pivot table indexed by ``+/-`` with one column per weekday.
    """
    grp = (
        df.groupby(["Weekday", "+/-"])[["Weight_(lbs)"]]
        .count()
        .reset_index()
        .pivot(index="+/-", columns="Weekday", values="Weight_(lbs)")
    )
    return grp


def describe_weight(df: pd.DataFrame) -> dict:
    """Summarise the year's weight trend.

    Args:
        df: Output of :func:`build_dataframe`.

    Returns:
        A dict with ``min``, ``max`` and ``mean`` weight in pounds,
        ``drop_max_pct`` (max to min) and ``drop_cur_pct`` (max to latest).
    """
    series = df["Weight_(lbs)"].apply(lambda x: np.nan if x == "/" else x).dropna()
    stats = series.describe()
    last_valid = series.iloc[-1] if not series.empty else np.nan
    drop_max = pd.Series([stats["max"], stats["min"]]).pct_change().mul(100).iloc[1]
    drop_cur = pd.Series([stats["max"], last_valid]).pct_change().mul(100).iloc[1]
    return {
        "min": stats["min"],
        "max": stats["max"],
        "mean": stats["mean"],
        "drop_max_pct": round(drop_max, 2),
        "drop_cur_pct": round(drop_cur, 2),
    }
