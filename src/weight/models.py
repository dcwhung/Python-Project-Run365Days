"""Dataclass for one daily weigh-in (kept free of pandas so the API can import it)."""

from dataclasses import dataclass


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
