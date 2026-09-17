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


class TestHaversineRejectsNonFiniteCoords:
    """A non-finite coordinate is corrupt input, not an absent one (CUI-0008)."""

    @pytest.mark.parametrize(
        ("origin", "destination"),
        [
            ((math.inf, 114.2), (22.3, 114.2)),
            ((-math.inf, 114.2), (22.3, 114.2)),
            ((math.nan, 114.2), (22.3, 114.2)),
            ((22.3, math.inf), (22.3, 114.2)),
            ((22.3, math.nan), (22.3, 114.2)),
            ((22.3, 114.2), (math.inf, 114.2)),
            ((22.3, 114.2), (math.nan, 114.2)),
            ((22.3, 114.2), (22.3, -math.inf)),
            ((22.3, 114.2), (22.3, math.nan)),
            ((math.inf, math.nan), (math.nan, math.inf)),
        ],
    )
    def test_every_non_finite_component_raises(self, origin, destination):
        with pytest.raises(ValueError):
            haversine_distance(origin, destination)

    def test_guard_fires_before_numpy_sees_the_value(self):
        # Measured on the pre-fix code: +-inf reached np.sin/np.cos and answered
        # nan behind two RuntimeWarnings, while nan answered nan behind *no*
        # warning at all. Promoting every warning to an error pins that the
        # guard runs first: were it removed, the inf case would surface as
        # RuntimeWarning (not ValueError) and the nan case would simply return.
        for origin in ((math.inf, 114.2), (math.nan, 114.2)):
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                with pytest.raises(ValueError):
                    haversine_distance(origin, (22.3, 114.2))

    def test_message_carries_both_coordinates(self):
        with pytest.raises(ValueError, match=r"non-finite coordinate"):
            haversine_distance((22.3, 114.2), (math.inf, 114.2))


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

    def test_non_finite_point_is_rejected_instead_of_summing_to_zero(self):
        # Measured on the pre-fix code: this track answered (0.0, 2) -- every
        # segment touching the bad point became nan, nan dropped out of the sum
        # and the caller was handed a plain zero (CUI-0001's lat_inf_2102.gpx).
        for bad in ((math.nan, 114.16), (math.inf, 114.16)):
            coords = [(22.2800, 114.1588), bad, (22.3193, 114.1694)]
            with pytest.raises(ValueError):
                total_track_distance(coords)

    def test_none_point_still_drops_out_quietly(self):
        # None is an indoor sample, not corruption: it keeps answering nan so
        # its segments still fall out of the sum rather than failing the file.
        coords = [(22.2800, 114.1588), (None, None), (22.3193, 114.1694)]
        dist, num = total_track_distance(coords)
        assert dist == 0.0
        assert num == 2

    def test_three_coords_sums_segments(self):
        a = (22.2800, 114.1588)
        b = (22.3193, 114.1694)
        c = (22.3500, 114.2000)
        dist_ab = haversine_distance(a, b)
        dist_bc = haversine_distance(b, c)
        dist_total, num = total_track_distance([a, b, c])
        assert num == 2
        assert dist_total == pytest.approx(dist_ab + dist_bc, rel=1e-6)
