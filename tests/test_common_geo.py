import math
import warnings

import pytest

from run365days.common.geo import haversine_distance, total_track_distance


class TestHaversineDistance:
    def test_same_point_is_zero(self):
        assert haversine_distance((22.3, 114.2), (22.3, 114.2)) == pytest.approx(0.0)

    def test_known_distance(self):
        # Central HK to Kowloon: verified ~4.5 km via Haversine
        dist = haversine_distance((22.2800, 114.1588), (22.3193, 114.1694))
        assert 4.0 < dist < 5.0

    def test_none_coord_returns_nan(self):
        result = haversine_distance((None, 114.0), (22.3, 114.2))
        assert math.isnan(result)

    def test_none_in_destination_returns_nan(self):
        result = haversine_distance((22.3, 114.2), (None, None))
        assert math.isnan(result)


class TestHaversineRejectsNonFinite:
    """``None`` means "indoor run" and stays a nan; inf/nan mean corrupt input."""

    @pytest.mark.parametrize(
        "origin,destination",
        [
            ((math.inf, 114.2), (22.3, 114.2)),
            ((-math.inf, 114.2), (22.3, 114.2)),
            ((math.nan, 114.2), (22.3, 114.2)),
            ((22.3, math.inf), (22.3, 114.2)),
            ((22.3, math.nan), (22.3, 114.2)),
            ((22.3, 114.2), (math.inf, 114.2)),
            ((22.3, 114.2), (22.3, -math.inf)),
            ((22.3, 114.2), (math.nan, math.nan)),
        ],
    )
    def test_non_finite_coord_raises(self, origin, destination):
        with pytest.raises(ValueError, match="finite"):
            haversine_distance(origin, destination)

    def test_error_names_the_offending_pair(self):
        with pytest.raises(ValueError) as excinfo:
            haversine_distance((math.inf, 114.2), (22.3, 114.2))
        assert "inf" in str(excinfo.value)

    def test_non_finite_does_not_emit_a_runtime_warning(self):
        # The silent path this replaces only ever surfaced as a numpy
        # "invalid value encountered in sin" RuntimeWarning.
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with pytest.raises(ValueError):
                haversine_distance((math.nan, 114.2), (22.3, 114.2))

    def test_finite_coords_still_compute(self):
        assert haversine_distance((22.3, 114.2), (22.3, 114.2)) == pytest.approx(0.0)


class TestTotalTrackDistanceRejectsNonFinite:
    def test_non_finite_point_raises(self):
        coords = [(22.2800, 114.1588), (math.inf, 114.1694), (22.3500, 114.2000)]
        with pytest.raises(ValueError, match="finite"):
            total_track_distance(coords)

    def test_nan_point_raises(self):
        coords = [(22.2800, 114.1588), (22.3193, math.nan)]
        with pytest.raises(ValueError, match="finite"):
            total_track_distance(coords)

    def test_none_points_are_still_dropped_not_rejected(self):
        # Indoor stretches inside an outdoor track stay a drop, not an error.
        coords = [(22.2800, 114.1588), (None, None), (22.3193, 114.1694)]
        dist, num = total_track_distance(coords)
        assert num >= 0
        assert not math.isnan(dist)


class TestTotalTrackDistance:
    def test_empty_coords_returns_zero(self):
        dist, num = total_track_distance([])
        assert dist == 0.0
        assert num == 0

    def test_single_coord_returns_zero(self):
        dist, num = total_track_distance([(22.3, 114.2)])
        assert dist == 0.0
        assert num == 0

    def test_two_coords(self):
        coords = [(22.2800, 114.1588), (22.3193, 114.1694)]
        dist, num = total_track_distance(coords)
        assert num == 1
        assert 4.0 < dist < 5.0

    def test_three_coords_sums_segments(self):
        a = (22.2800, 114.1588)
        b = (22.3193, 114.1694)
        c = (22.3500, 114.2000)
        dist_ab = haversine_distance(a, b)
        dist_bc = haversine_distance(b, c)
        dist_total, num = total_track_distance([a, b, c])
        assert num == 2
        assert dist_total == pytest.approx(dist_ab + dist_bc, rel=1e-6)
