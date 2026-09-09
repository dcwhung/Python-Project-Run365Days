import numpy as np

Coord = tuple[float | None, float | None]


def haversine_distance(origin: Coord, destination: Coord) -> float:
    """Return great-circle distance in km between two (lat, lon) pairs.

    Returns np.nan if either coordinate contains None.
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
    """Return (total_km, num_segments) for a list of (lat, lon) coords."""
    import pandas as pd

    if len(coords) < 2:
        return 0.0, 0

    df = pd.DataFrame({"cur": coords})
    df["nxt"] = df["cur"].shift(-1)
    df = df.dropna()
    df["dist"] = df.apply(lambda row: haversine_distance(row["cur"], row["nxt"]), axis=1)
    return df["dist"].sum(), df["nxt"].count()
