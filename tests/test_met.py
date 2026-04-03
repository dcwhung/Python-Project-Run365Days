import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from run365days.analysis.met import (
    get_mets,
    kcal_burned,
    pace_min_per_km,
    pace_min_per_mile,
    speed_mph,
)


class TestPaceCalculations:
    # 32:39 over 6.09 km
    _DURATION = "00:32:39"
    _DIST_KM = 6.09

    def test_pace_min_per_km(self):
        pace = pace_min_per_km(self._DURATION, self._DIST_KM)
        # 32.65 min / 6.09 km ≈ 5.36 min/km
        assert 5.3 < pace < 5.5

    def test_pace_min_per_mile(self):
        pace = pace_min_per_mile(self._DURATION, self._DIST_KM)
        # ~8.6 min/mile
        assert 8.4 < pace < 8.9

    def test_speed_mph(self):
        mph = speed_mph(self._DURATION, self._DIST_KM)
        # ~6.95 mph
        assert 6.7 < mph < 7.2


class TestGetMETS:
    def test_exact_match(self):
        # 12 min/mile => 8.3 METS
        assert get_mets(12.0) == pytest.approx(8.3)

    def test_closest_match(self):
        # 11.7 min/mile is between 11.5 (9.0) and 12.0 (8.3) — closer to 11.5
        assert get_mets(11.7) == pytest.approx(9.0)

    def test_very_fast(self):
        # 4.3 min/mile => 23.0 METS
        assert get_mets(4.3) == pytest.approx(23.0)


class TestKcalBurned:
    def test_known_example(self):
        # 11.5 METS, 77.3 kg, 45 min => ~700 kcal
        kcal = kcal_burned(11.5, 77.3, "00:45:00")
        assert 680 < kcal < 720

    def test_zero_duration(self):
        assert kcal_burned(10.0, 70.0, "00:00:00") == pytest.approx(0.0)
