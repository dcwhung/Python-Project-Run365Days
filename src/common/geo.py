"""Great-circle distance helpers."""

import math

import numpy as np

Coord = tuple[float | None, float | None]


def haversine_distance(origin: Coord, destination: Coord) -> float:
    """Return the great-circle distance between two coordinates.

    ``None`` and a non-finite number arrive here for opposite reasons and get
    opposite answers. ``None`` is an indoor sample that simply carries no fix,
    so its segment answers ``nan`` and falls out of the sum. ``±inf`` / ``nan``
    is corrupt input, and answering it with ``nan`` is what made CUI-0001's
    ``lat_inf_2102.gpx`` report ``distance_by_coord_km = 0.0``: every segment
    touching the bad point became ``nan``, ``nan`` dropped out of the sum, and
    the caller got a plain zero. numpy does not help here -- ``±inf`` only
    trips a ``RuntimeWarning`` from ``sin`` / ``cos``, and ``nan`` passes
    through them without a warning at all -- so the value is rejected on the
    way in instead (CUI-0008).

    ``ValueError`` is deliberate: ``BaseActivityParser.parse_all`` already has a
    ``ValueError`` branch, so one corrupt file is logged by name at WARNING and
    dropped rather than taking the whole export down. Raising the parsers'
    ``ActivityParseError`` would read better but would invert the layering --
    ``activities`` imports ``common``, never the other way round.

    Args:
        origin: ``(lat, lon)`` in degrees.
        destination: ``(lat, lon)`` in degrees.

    Returns:
        Distance in kilometres, or ``numpy.nan`` if either coordinate has a
        ``None`` component (indoor runs).

    Raises:
        ValueError: If any present component is ``±inf`` or ``nan``.
    """
    lat1, lon1 = origin
    lat2, lon2 = destination

    if None in origin or None in destination:
        return np.nan

    # Four short-circuiting math.isfinite calls, not a vectorised pass. CUI-0008
    # asked for a vectorised screen on the grounds that this is a hot path; two
    # measurements say the premise does not hold at this scale, and both are
    # recorded in the ticket with their readings and the HEAD they were taken at:
    #
    #   1. Interleaved A/B on total_track_distance over a fixed synthetic track
    #      of realistic length (~1 Hz over a 10 km run), one fresh process per
    #      reading, arms alternated so machine drift hits both equally, median
    #      of five pairs. The two arms differ by less than the spread within
    #      either arm, in both directions -- the guard is not resolvable here.
    #   2. timeit in isolation (min of seven runs), which is not noise-limited:
    #      it puts the guard expression at a low single-digit percentage of one
    #      haversine_distance call, and under one percent of a whole segment
    #      once DataFrame.apply's per-row dispatch is counted.
    #
    # Vectorising would also have to happen one level up in total_track_distance,
    # where it would duplicate this work rather than replace it and would leave
    # the public entry point -- the only way a non-finite pair still reaches this
    # code -- unguarded. Re-run both measurements before changing the shape.
    if not (
        math.isfinite(lat1) and math.isfinite(lon1) and math.isfinite(lat2) and math.isfinite(lon2)
    ):
        raise ValueError(f"non-finite coordinate: origin={origin}, destination={destination}")

    r = 6371  # Earth radius in km

    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)

    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    return r * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def total_track_distance(coords: list) -> tuple[float, int]:
    """Sum the haversine distance along a list of coordinates.

    Args:
        coords: Ordered ``(lat, lon)`` pairs; ``None`` components are dropped.

    Returns:
        A ``(total_km, num_segments)`` tuple. ``num_segments`` is a numpy
        integer; convert with ``int()`` before JSON serialisation.

    Raises:
        ValueError: If any coordinate carries a non-finite component. The sum
            cannot report such a track honestly -- the bad segments turn into
            ``nan`` and vanish from it -- so the track is refused instead
            (CUI-0008).
    """
    import pandas as pd

    if len(coords) < 2:
        return 0.0, 0

    df = pd.DataFrame({"cur": coords})
    df["nxt"] = df["cur"].shift(-1)
    df = df.dropna()
    df["dist"] = df.apply(lambda row: haversine_distance(row["cur"], row["nxt"]), axis=1)
    return df["dist"].sum(), df["nxt"].count()
