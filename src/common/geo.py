"""Great-circle distance helpers."""

import numpy as np

Coord = tuple[float | None, float | None]


def haversine_distance(origin: Coord, destination: Coord) -> float:
    """Return the great-circle distance between two coordinates.

    Args:
        origin: ``(lat, lon)`` in degrees.
        destination: ``(lat, lon)`` in degrees.

    Returns:
        Distance in kilometres, or ``numpy.nan`` if either coordinate has a
        ``None`` component (indoor runs).
    """
    lat1, lon1 = origin
    lat2, lon2 = destination

    if None in origin or None in destination:
        return np.nan

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
    """
    import pandas as pd

    if len(coords) < 2:
        return 0.0, 0

    df = pd.DataFrame({"cur": coords})
    df["nxt"] = df["cur"].shift(-1)
    df = df.dropna()
    df["dist"] = df.apply(lambda row: haversine_distance(row["cur"], row["nxt"]), axis=1)
    return df["dist"].sum(), df["nxt"].count()
