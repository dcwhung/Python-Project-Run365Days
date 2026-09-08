"""Daily weight analysis: parsing, derived metrics, and summaries."""
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

_HEIGHT_CM_DEFAULT = 170.0


@dataclass
class WeightRecord:
    day_number: int
    date: str          # 'YYYY-MM-DD'
    weight_lbs: float
    weight_kg: float
    bmi: float


def parse_weight_file(
    file_path: Path,
    year: Optional[int] = None,
    height_cm: float = _HEIGHT_CM_DEFAULT,
) -> List[WeightRecord]:
    """Parse a weight text file and return a list of WeightRecords.

    Each line is expected to match: ``<weight> lbs (<day>/<month>)``
    e.g. ``154.8 lbs (1/5)``. A line with a weight but no date (the file's
    final entry is often written this way) is taken as the day after the
    previous dated record.
    """
    if year is None:
        year = datetime.today().year

    pattern = re.compile(r"([\d.]+)\s*lbs\s*\((\d+)/(\d+)\)")
    undated = re.compile(r"^\s*([\d.]+)\s*lbs\s*$")
    records: List[WeightRecord] = []
    last_date: Optional[datetime] = None

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
    records: List[WeightRecord],
    year: Optional[int] = None,
) -> pd.DataFrame:
    """Return a full-year DataFrame with one row per day.

    Missing days are filled with NaN. Derived columns (+/-, %) are added.
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
    df["+/-"] = df["Weight_(lbs)"].diff().map(
        lambda x: "+" if x > 0 else "-" if x < 0 else "/"
        if pd.notna(x)
        else "/"
    )
    df["%"] = df["Weight_(lbs)"].pct_change().mul(100)
    return df.fillna("/")


def monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Return pivot of +/-/no-change counts by month."""
    grp = (
        df.groupby(["Month", "+/-"])[["Weight_(lbs)"]]
        .count()
        .reset_index()
        .pivot(index="+/-", columns="Month", values="Weight_(lbs)")
    )
    return grp


def weekday_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Return pivot of +/-/no-change counts by weekday."""
    grp = (
        df.groupby(["Weekday", "+/-"])[["Weight_(lbs)"]]
        .count()
        .reset_index()
        .pivot(index="+/-", columns="Weekday", values="Weight_(lbs)")
    )
    return grp


def describe_weight(df: pd.DataFrame) -> dict:
    """Return {min, max, mean, drop_max_pct, drop_cur_pct} statistics."""
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
