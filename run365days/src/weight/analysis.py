"""Daily weight analysis: parsing, derived metrics, and summaries."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

_HEIGHT_CM_DEFAULT = 170.0


@dataclass
class WeightRecord:
    """One daily weigh-in.

    Attributes:
        day_number: 1-based line number in the source file.
        date: Calendar date ``YYYY-MM-DD``.
        weight_lbs: Weight in pounds as written in the file.
        weight_kg: Weight converted to kilograms.
        bmi: Body-mass index using the configured height.
    """

    day_number: int
    date: str  # 'YYYY-MM-DD'
    weight_lbs: float
    weight_kg: float
    bmi: float


def parse_weight_file(
    file_path: Path,
    year: int | None = None,
    height_cm: float = _HEIGHT_CM_DEFAULT,
) -> list[WeightRecord]:
    """Parse a daily weight text file.

    Each line is expected to match ``<weight> lbs (<day>/<month>)``, for
    example ``154.8 lbs (1/5)``. A line with a weight but no date (the file's
    final entry is often written this way) is taken as the day after the
    previous dated record. Lines that match neither form are skipped.

    Args:
        file_path: Path to the text file.
        year: Calendar year for the day/month values (default: current year).
        height_cm: Height used for the BMI column.

    Returns:
        Records in file order.
    """
    if year is None:
        year = datetime.today().year

    pattern = re.compile(r"([\d.]+)\s*lbs\s*\((\d+)/(\d+)\)")
    undated = re.compile(r"^\s*([\d.]+)\s*lbs\s*$")
    records: list[WeightRecord] = []
    last_date: datetime | None = None

    with open(file_path) as f:
        for day_number, line in enumerate(f, start=1):
            match = pattern.search(line)
            if match:
                weight_lbs = float(match.group(1))
                day = int(match.group(2))
                month = int(match.group(3))
                date_obj = datetime(year, month, day)
            else:
                match = undated.match(line)
                if not match or last_date is None:
                    continue
                weight_lbs = float(match.group(1))
                date_obj = last_date + timedelta(days=1)
            last_date = date_obj
            weight_kg = weight_lbs * 0.454
            bmi = weight_kg / ((height_cm / 100) ** 2)
            records.append(
                WeightRecord(
                    day_number=day_number,
                    date=date_obj.strftime("%Y-%m-%d"),
                    weight_lbs=weight_lbs,
                    weight_kg=round(weight_kg, 2),
                    bmi=round(bmi, 2),
                )
            )
    return records


def build_dataframe(
    records: list[WeightRecord],
    year: int | None = None,
) -> pd.DataFrame:
    """Expand weigh-ins into a full-year table with one row per day.

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
    weight_by_date = {r.date: r.weight_lbs for r in records}
    weights = [weight_by_date.get(d.strftime("%Y-%m-%d")) for d in date_range]

    df = pd.DataFrame(
        {
            "Day": range(1, len(date_range) + 1),
            "Date": date_range,
            "Weight_(lbs)": weights,
        }
    )
    df["Month"] = df["Date"].dt.month
    df["Weekday"] = df.apply(
        lambda row: f"{row['Date'].weekday()} - {row['Date'].strftime('%A').upper()[:3]}",
        axis=1,
    )
    df["Weight_(kg)"] = df["Weight_(lbs)"].apply(
        lambda x: round(x * 0.454, 2) if pd.notna(x) else np.nan
    )
    df["BMI"] = df["Weight_(kg)"].apply(
        lambda x: round(x / ((_HEIGHT_CM_DEFAULT / 100) ** 2), 2) if pd.notna(x) else np.nan
    )
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
