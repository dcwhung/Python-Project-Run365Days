"""Great-circle distance helpers."""

from collections.abc import Sequence

import numpy as np

Coord = tuple[float | None, float | None]

_EARTH_RADIUS_KM = 6371
"""Mean Earth radius; the sphere the haversine formula approximates."""

_MAX_REPORTED_OFFENDERS = 3
"""Offending points quoted in the error; a bad track is usually bad throughout."""

_NON_FINITE_MESSAGE = (
    "coordinates must be finite degrees, or None for an indoor point; got non-finite {offenders}"
)


def _finite_degrees(coords: Sequence[Coord]) -> np.ndarray:
    """Return *coords* as an ``(n, 2)`` float array, rejecting non-finite degrees.

    ``None`` is the documented "no GPS" marker and survives as ``nan``. An
    ``inf`` or a caller-supplied ``nan`` is corrupt input: numpy would only
    mutter ``RuntimeWarning: invalid value encountered in cos`` and hand back a
    ``nan`` that rides into the export as a silent ``0.0`` (CUI-0008). The
    parsers cannot reach here any more -- CUI-0001 hardened every lat/lon read
    -- but this is a public function, so in-process callers still can.

    The check is one vectorised pass over the whole track rather than a test
    per point, because it runs on every GPS fix of every activity (AU-039).

    Args:
        coords: Ordered ``(lat, lon)`` pairs in degrees.

    Returns:
        The degrees as a float array, ``None`` components rendered as ``nan``.

    Raises:
        ValueError: If any component is ``inf`` or ``nan``.
    """
    degrees = np.array(coords, dtype=float)
    missing = np.equal(np.array(coords, dtype=object), None)
    bad = ~np.isfinite(degrees) & ~missing
    if bad.any():
        offenders = [tuple(pair) for pair in np.asarray(coords, dtype=object)[bad.any(axis=1)]]
        raise ValueError(_NON_FINITE_MESSAGE.format(offenders=offenders[:_MAX_REPORTED_OFFENDERS]))
    return degrees


def _segment_distances(degrees: np.ndarray) -> np.ndarray:
    """Return the haversine distance of each consecutive pair in *degrees*.

    The single implementation of the formula: the scalar and the whole-track
    entry points both come through here, so the two cannot drift apart.

    Args:
        degrees: An ``(n, 2)`` array of ``(lat, lon)`` with ``n >= 2``.

    Returns:
        An array of ``n - 1`` distances in kilometres.
    """
    lat, lon = degrees[:, 0], degrees[:, 1]

    phi1, phi2 = np.radians(lat[:-1]), np.radians(lat[1:])
    delta_phi = np.radians(lat[1:] - lat[:-1])
    delta_lambda = np.radians(lon[1:] - lon[:-1])

    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    return _EARTH_RADIUS_KM * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def haversine_distance(origin: Coord, destination: Coord) -> float:
    """Return the great-circle distance between two coordinates.

    Args:
        origin: ``(lat, lon)`` in degrees.
        destination: ``(lat, lon)`` in degrees.

    Returns:
        Distance in kilometres, or ``numpy.nan`` if either coordinate has a
        ``None`` component (indoor runs).

    Raises:
        ValueError: If any component is ``inf`` or ``nan``. ``None`` means "not
            measured" and still yields ``nan``; a non-finite *number* means the
            caller's data is broken, and is refused rather than propagated.
    """
    degrees = _finite_degrees([origin, destination])

    if None in origin or None in destination:
        return np.nan

    return _segment_distances(degrees)[0]


def total_track_distance(coords: list) -> tuple[float, int]:
    """Sum the haversine distance along a list of coordinates.

    Args:
        coords: Ordered ``(lat, lon)`` pairs; ``None`` components contribute
            ``nan``, which the sum skips.

    Returns:
        A ``(total_km, num_segments)`` tuple.

    Raises:
        ValueError: If any component is ``inf`` or ``nan`` (CUI-0008).
    """
    # WHY the in-function import: pandas costs real cold-start time, and this
    # module is on the parsers' import path. Do NOT hoist it to the top of the
    # file -- the light dependency set is a contract, pinned by
    # tests/test_api_imports.py.
    import pandas as pd

    if len(coords) < 2:
        return 0.0, 0

    degrees = _finite_degrees(coords)
    distances = _segment_distances(degrees)

    # pandas' nan-skipping sum, kept from the row-wise original: it is what the
    # committed export bytes were produced with.
    return pd.Series(distances).sum(), len(distances)
