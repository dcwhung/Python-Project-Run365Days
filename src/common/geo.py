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

    Being finite is not enough to be a coordinate, though, and CUI-0008's screen
    stopped there. A latitude of 400 does not fail: it answers a number that
    looks like a distance, and nothing downstream can tell it from a real one.
    Push the magnitude far enough -- around ``1e308``, which ``parse_finite_float``
    accepts because it genuinely is finite -- and ``np.sin`` overflows back to
    ``nan``, reopening the exact CUI-0001 shape from a door the finiteness guard
    does not cover: ``nan`` drops out of ``pandas``' sum and the track reports
    short. So the screen is a range, not a magnitude (CUI-0047); ``tests/
    test_common_geo.py`` pins what each refused input used to answer.

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
        ValueError: If any present component is ``±inf`` / ``nan`` (message
            ``non-finite coordinate``) or outside ``-90 <= lat <= 90`` /
            ``-180 <= lon <= 180`` (message ``out-of-range coordinate``). The
            bounds are inclusive: the poles and the antimeridian are valid.
    """
    lat1, lon1 = origin
    lat2, lon2 = destination

    if None in origin or None in destination:
        return np.nan

    # Four short-circuiting range tests that *replace* CUI-0008's four
    # math.isfinite calls rather than being added in front of them: abs(nan) is
    # nan and every comparison against nan is False, and no infinity survives
    # `<= 90.0`, so the finiteness screen is subsumed by the range screen rather
    # than run twice. CUI-0047 therefore changes what each of the four checks
    # says, not how many there are.
    #
    # The spelling is measured, not taste. Both float constants and abs() matter:
    # timeit (min of seven, one million evaluations) ranks the candidate guards
    # apart by well over their own run-to-run spread, and the readings are in the
    # ticket with the interpreter version they were taken on. The short version
    # is that a chained `-90 <= lat1 <= 90` against *int* literals is the most
    # expensive way to write this, and the form below the cheapest.
    #
    # Still not a vectorised pass. CUI-0008 asked for one on the grounds that
    # this is a hot path; two measurements say the premise does not hold at this
    # scale, and both are recorded in the ticket with their readings and the HEAD
    # they were taken at:
    #
    #   1. Interleaved A/B on total_track_distance over a fixed synthetic track
    #      of realistic length (~1 Hz over a one-hour run), one fresh process per
    #      reading, arms alternated so machine drift hits both equally, median
    #      of five pairs. The two arms differ by less than the spread within
    #      either arm, in both directions -- the guard is not resolvable here.
    #      Re-run for CUI-0047 with the two arms cut from this function's own
    #      source (inspect.getsource, one substitution) so they differ by the
    #      guard expression and nothing else; same verdict.
    #   2. timeit in isolation (min of seven runs), which is not noise-limited:
    #      it puts the guard expression at a low single-digit percentage of one
    #      haversine_distance call, and under one percent of a whole segment
    #      once DataFrame.apply's per-row dispatch is counted. Measurement 1
    #      cannot resolve a difference that small, so the two do not disagree.
    #
    # Vectorising would also have to happen one level up in total_track_distance,
    # where it would duplicate this work rather than replace it and would leave
    # the public entry point -- the only way a bad pair still reaches this code
    # -- unguarded. Re-run both measurements before changing the shape.
    #
    # The bounds are inclusive on purpose: the poles and the antimeridian are
    # real places. `<` here would refuse (90, 180).
    if not (abs(lat1) <= 90.0 and abs(lon1) <= 180.0 and abs(lat2) <= 90.0 and abs(lon2) <= 180.0):
        # Only reached on the way to an exception, so naming which of the two
        # failures it was costs nothing that matters: "non-finite" and
        # "out-of-range" send a reader to different places (a corrupt file
        # versus a unit or an axis swap), and parse_all logs this message.
        kind = (
            "non-finite"
            if not all(math.isfinite(v) for v in (lat1, lon1, lat2, lon2))
            else "out-of-range"
        )
        raise ValueError(f"{kind} coordinate: origin={origin}, destination={destination}")

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
        ValueError: If any coordinate carries a non-finite component (CUI-0008)
            or one outside the range of the globe (CUI-0047). The sum cannot
            report such a track honestly -- the bad segments turn into ``nan``
            and vanish from it, or worse, answer a plausible number -- so the
            track is refused instead.
    """
    import pandas as pd

    if len(coords) < 2:
        return 0.0, 0

    df = pd.DataFrame({"cur": coords})
    df["nxt"] = df["cur"].shift(-1)
    df = df.dropna()
    df["dist"] = df.apply(lambda row: haversine_distance(row["cur"], row["nxt"]), axis=1)
    return df["dist"].sum(), df["nxt"].count()
