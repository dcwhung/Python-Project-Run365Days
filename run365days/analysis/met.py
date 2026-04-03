"""Metabolic Equivalent (MET) calculations for running activities.

Reference
---------
https://www.healthline.com/health/what-are-mets#calorie-connection

Formula: METs × 3.5 × body_weight_kg / 200 = kcal/min
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

# MET lookup table keyed by pace (min/mile).
# Each entry represents one standardised intensity band.
@dataclass(frozen=True)
class _METEntry:
    mets: float
    mph: float
    min_per_mile: float


_MET_TABLE: List[_METEntry] = [
    _METEntry(6.0,  4.0,  13.0),
    _METEntry(8.3,  5.0,  12.0),
    _METEntry(9.0,  5.2,  11.5),
    _METEntry(9.8,  6.0,  10.0),
    _METEntry(10.5, 6.7,   9.0),
    _METEntry(11.0, 7.0,   8.5),
    _METEntry(11.5, 7.5,   8.0),
    _METEntry(11.8, 8.0,   7.5),
    _METEntry(12.3, 8.6,   7.0),
    _METEntry(12.8, 9.0,   6.5),
    _METEntry(14.5, 10.0,  6.0),
    _METEntry(16.0, 11.0,  5.5),
    _METEntry(19.0, 12.0,  5.0),
    _METEntry(19.8, 13.0,  4.6),
    _METEntry(23.0, 14.0,  4.3),
]

_MILES_PER_KM = 1 / 1.609344


def pace_min_per_km(duration_hhmmss: str, distance_km: float) -> float:
    """Return pace in decimal minutes per km."""
    total_sec = _hhmmss_to_sec(duration_hhmmss)
    return (total_sec / 60) / distance_km


def pace_min_per_mile(duration_hhmmss: str, distance_km: float) -> float:
    """Return pace in decimal minutes per mile."""
    total_sec = _hhmmss_to_sec(duration_hhmmss)
    return (total_sec / 60) / (distance_km * _MILES_PER_KM)


def speed_mph(duration_hhmmss: str, distance_km: float) -> float:
    """Return speed in miles per hour."""
    total_hours = _hhmmss_to_sec(duration_hhmmss) / 3600
    return (distance_km * _MILES_PER_KM) / total_hours


def get_mets(min_per_mile: float) -> float:
    """Return the MET value for the closest pace in the lookup table."""
    return min(
        _MET_TABLE,
        key=lambda entry: abs(entry.min_per_mile - min_per_mile),
    ).mets


def kcal_burned(mets: float, body_weight_kg: float, duration_hhmmss: str) -> float:
    """Return estimated kilocalories burned.

    Formula: METs × 3.5 × body_weight_kg / 200 × duration_minutes
    """
    total_min = _hhmmss_to_sec(duration_hhmmss) / 60
    return (mets * 3.5 * body_weight_kg / 200) * total_min


def _hhmmss_to_sec(time_str: str) -> float:
    return (
        datetime.strptime(time_str, "%H:%M:%S") - datetime(1900, 1, 1)
    ).total_seconds()
