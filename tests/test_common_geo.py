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


def _unguarded_haversine(origin, destination):
    """Textbook haversine with no input screen at all -- the pre-CUI-0047 shape.

    Kept as an independent oracle so the readings quoted in
    ``TestHaversineRejectsImpossibleCoords`` and in ``geo.haversine_distance``'s
    docstring stay bound to an assertion (W-017). It deliberately does not go
    through ``haversine_distance``: the point is to record what the formula
    answers for an input the guard now refuses, which the guarded function can
    no longer be asked.
    """
    import numpy as np

    lat1, lon1 = origin
    lat2, lon2 = destination
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    return 6371 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


class TestHaversineRejectsImpossibleCoords:
    """A finite number is not automatically a coordinate (CUI-0047)."""

    @pytest.mark.parametrize(
        ("origin", "destination"),
        [
            ((400.0, 0.0), (0.0, 0.0)),
            ((91.0, 0.0), (0.0, 0.0)),
            ((-90.0001, 0.0), (0.0, 0.0)),
            ((0.0, 200.0), (0.0, 0.0)),
            ((0.0, -180.0001), (0.0, 0.0)),
            ((0.0, 0.0), (400.0, 0.0)),
            ((0.0, 0.0), (0.0, 200.0)),
            ((1e20, 1e20), (-1e20, -1e20)),
            ((1e308, 1e308), (-1e308, -1e308)),
        ],
    )
    def test_out_of_range_degrees_raise(self, origin, destination):
        with pytest.raises(ValueError):
            haversine_distance(origin, destination)

    @pytest.mark.parametrize(
        ("origin", "destination", "pre_fix_reading"),
        [
            ((400.0, 0.0), (0.0, 0.0), 4447.797065782349),
            ((91.0, 0.0), (0.0, 0.0), 10118.738324654845),
            ((0.0, 200.0), (0.0, 0.0), 17791.188263129396),
            ((1e20, 1e20), (-1e20, -1e20), 5560.056317082926),
        ],
    )
    def test_refused_inputs_used_to_answer_a_plausible_number(
        self, origin, destination, pre_fix_reading
    ):
        # This is the shape CUI-0047 is about: the formula does not fail on an
        # impossible coordinate, it answers a number that looks like a distance.
        # The oracle pins what the caller used to be handed, so the guard is
        # demonstrably removing a wrong answer and not a crash.
        assert _unguarded_haversine(origin, destination) == pytest.approx(pre_fix_reading)
        with pytest.raises(ValueError):
            haversine_distance(origin, destination)

    def test_enormous_finite_degrees_used_to_answer_nan_behind_a_warning(self):
        # The worst shape of all, and the one that reopens CUI-0001: 1e308 is
        # finite, so parse_finite_float accepts it and CUI-0008's screen waves
        # it through, but np.sin overflows to nan behind a RuntimeWarning that
        # nobody reads -- and pandas' sum drops nan silently, so the track just
        # reports short. The oracle pins both halves of that: nan, and the
        # warning. Promoting warnings to errors shows the guard now fires first.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert math.isnan(_unguarded_haversine((1e308, 1e308), (-1e308, -1e308)))
        assert any("invalid value encountered" in str(w.message) for w in caught)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with pytest.raises(ValueError):
                haversine_distance((1e308, 1e308), (-1e308, -1e308))

    @pytest.mark.parametrize(
        ("origin", "destination"),
        [
            ((90.0, 0.0), (-90.0, 0.0)),
            ((0.0, 180.0), (0.0, -180.0)),
            ((90.0, 180.0), (-90.0, -180.0)),
            ((-90.0, -180.0), (90.0, 180.0)),
        ],
    )
    def test_the_extremes_of_the_real_globe_are_still_accepted(self, origin, destination):
        # +-90 / +-180 are legal coordinates, not sentinels: an inclusive bound
        # is load-bearing here. A `<` in place of `<=` would refuse the poles
        # and the antimeridian.
        result = haversine_distance(origin, destination)
        assert math.isfinite(result)
        assert result >= 0.0

    def test_message_names_the_range_failure_separately(self):
        with pytest.raises(ValueError, match=r"out-of-range coordinate"):
            haversine_distance((400.0, 0.0), (0.0, 0.0))

    def test_total_track_distance_refuses_a_track_with_an_impossible_point(self):
        coords = [(22.2800, 114.1588), (400.0, 114.16), (22.3193, 114.1694)]
        with pytest.raises(ValueError):
            total_track_distance(coords)
