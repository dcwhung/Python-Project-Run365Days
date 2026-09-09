"""Year-level aggregations over exported activity records.

Every function here is pure and works on the plain dicts produced by
:func:`run365days.export.records.activity_record`, so the same numbers can
be served by the GraphQL API and checked in unit tests without a database.

Week boundaries follow the v2 dashboard: weeks start on Monday and the first
week of the year is the (possibly partial) week containing 1 January.
"""

import calendar
from collections import defaultdict
from datetime import date, timedelta

CTL_DAYS = 42
"""Time constant of the chronic training load (fitness) average."""
ATL_DAYS = 7
"""Time constant of the acute training load (fatigue) average."""
FASTEST_MIN_KM = 5.0
"""Only runs at least this long count for the fastest-pace record."""
MONTHS_PER_YEAR = 12
DAYS_PER_WEEK = 7


def days_in_year(year: int) -> int:
    """Return 366 for leap years, otherwise 365."""
    return 366 if calendar.isleap(year) else 365


def _sum(values) -> float:
    return sum(v for v in values if v is not None)


def _mean(values) -> float | None:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def _pace(distance_km: float, duration_sec: float) -> int | None:
    return int(round(duration_sec / distance_km)) if distance_km else None


def _fastest(activities: list[dict]) -> dict | None:
    eligible = [
        a
        for a in activities
        if a.get("pace_sec_per_km") and (a.get("distance_km") or 0) >= FASTEST_MIN_KM
    ]
    return min(eligible, key=lambda a: a["pace_sec_per_km"]) if eligible else None


def totals(activities: list[dict], year: int) -> dict:
    """Headline numbers for the year.

    Returns:
        ``runs``, ``days`` (in the year), ``active_days``, ``distance_km``,
        ``duration_sec``, ``calories``, ``avg_pace_sec_per_km``
        (distance-weighted), ``avg_cadence`` and ``avg_distance_km``.
    """
    km = _sum(a["distance_km"] for a in activities)
    sec = _sum(a["duration_sec"] for a in activities)
    return {
        "runs": len(activities),
        "days": days_in_year(year),
        "active_days": len({a["date"] for a in activities}),
        "distance_km": round(km, 2),
        "duration_sec": int(sec),
        "calories": int(_sum(a.get("calories") for a in activities)),
        "avg_pace_sec_per_km": _pace(km, sec),
        "avg_cadence": _mean(a.get("avg_cadence") for a in activities),
        "avg_distance_km": round(km / len(activities), 2) if activities else 0.0,
    }


def monthly(activities: list[dict]) -> list[dict]:
    """One row per calendar month, including months with no runs.

    Returns:
        ``month`` (1-12), ``runs``, ``distance_km``, ``duration_sec``,
        ``calories``, ``avg_pace_sec_per_km``, ``avg_cadence``,
        ``best_pace_sec_per_km`` and ``best_pace_activity_id`` (runs of at
        least :data:`FASTEST_MIN_KM`).
    """
    by_month: dict[int, list[dict]] = defaultdict(list)
    for a in activities:
        by_month[int(a["date"][5:7])].append(a)
    rows = []
    for month in range(1, MONTHS_PER_YEAR + 1):
        group = by_month.get(month, [])
        km = _sum(a["distance_km"] for a in group)
        sec = _sum(a["duration_sec"] for a in group)
        best = _fastest(group)
        rows.append(
            {
                "month": month,
                "runs": len(group),
                "distance_km": round(km, 2),
                "duration_sec": int(sec),
                "calories": int(_sum(a.get("calories") for a in group)),
                "avg_pace_sec_per_km": _pace(km, sec),
                "avg_cadence": _mean(a.get("avg_cadence") for a in group),
                "best_pace_sec_per_km": best["pace_sec_per_km"] if best else None,
                "best_pace_activity_id": best["id"] if best else None,
            }
        )
    return rows


def week_index(day: date, year: int) -> int:
    """Return the 0-based week number of *day* within *year* (weeks start Monday)."""
    start_dow = date(year, 1, 1).weekday()
    return (day.timetuple().tm_yday - 1 + start_dow) // DAYS_PER_WEEK


def weekly(activities: list[dict], year: int) -> list[dict]:
    """One row per week of the year, including empty weeks.

    Returns:
        ``week`` (0-based), ``week_start`` (ISO date, clamped to 1 January),
        ``runs``, ``distance_km``, ``duration_sec``, ``avg_pace_sec_per_km``,
        ``longest_km`` and ``activity_ids``.
    """
    jan1 = date(year, 1, 1)
    n_weeks = week_index(date(year, 12, 31), year) + 1
    weeks = []
    for w in range(n_weeks):
        start = jan1 + timedelta(days=w * DAYS_PER_WEEK - jan1.weekday())
        weeks.append(
            {
                "week": w,
                "week_start": max(start, jan1).isoformat(),
                "runs": 0,
                "distance_km": 0.0,
                "duration_sec": 0,
                "avg_pace_sec_per_km": None,
                "longest_km": 0.0,
                "activity_ids": [],
            }
        )
    for a in activities:
        wk = weeks[week_index(date.fromisoformat(a["date"]), year)]
        wk["runs"] += 1
        wk["distance_km"] += a["distance_km"] or 0
        wk["duration_sec"] += a["duration_sec"] or 0
        wk["longest_km"] = max(wk["longest_km"], a["distance_km"] or 0)
        wk["activity_ids"].append(a["id"])
    for wk in weeks:
        wk["distance_km"] = round(wk["distance_km"], 2)
        wk["avg_pace_sec_per_km"] = _pace(wk["distance_km"], wk["duration_sec"])
    return weeks


def daily_distance(activities: list[dict], year: int) -> list[dict]:
    """Kilometres per calendar day, zero on rest days.

    Returns:
        One ``{date, distance_km, activity_id}`` per day; ``activity_id`` is
        the first run that day or ``None``.
    """
    per_day: dict[str, dict] = {}
    for a in activities:
        row = per_day.setdefault(a["date"], {"distance_km": 0.0, "activity_id": a["id"]})
        row["distance_km"] += a["distance_km"] or 0
    jan1 = date(year, 1, 1)
    out = []
    for i in range(days_in_year(year)):
        d = (jan1 + timedelta(days=i)).isoformat()
        row = per_day.get(d)
        out.append(
            {
                "date": d,
                "distance_km": round(row["distance_km"], 2) if row else 0.0,
                "activity_id": row["activity_id"] if row else None,
            }
        )
    return out


def training_load(daily: list[dict]) -> list[dict]:
    """Exponentially weighted daily load (fitness, fatigue, form).

    Args:
        daily: Output of :func:`daily_distance`.

    Returns:
        One ``{date, ctl, atl, tsb}`` per day where ``ctl`` uses a
        :data:`CTL_DAYS` constant, ``atl`` :data:`ATL_DAYS`, and
        ``tsb = ctl - atl``.
    """
    ctl = atl = 0.0
    out = []
    for row in daily:
        km = row["distance_km"]
        ctl += (km - ctl) / CTL_DAYS
        atl += (km - atl) / ATL_DAYS
        out.append(
            {
                "date": row["date"],
                "ctl": round(ctl, 2),
                "atl": round(atl, 2),
                "tsb": round(ctl - atl, 2),
            }
        )
    return out


def personal_bests(activities: list[dict]) -> dict:
    """Record runs: ``longest``, ``fastest``, ``longest_time``, ``most_calories``, ``top_cadence``.

    Each value is the activity dict (or ``None`` when no run qualifies).
    """
    if not activities:
        return dict.fromkeys(["longest", "fastest", "longest_time", "most_calories", "top_cadence"])
    with_kcal = [a for a in activities if a.get("calories") is not None]
    with_cad = [a for a in activities if a.get("avg_cadence") is not None]
    return {
        "longest": max(activities, key=lambda a: a["distance_km"] or 0),
        "fastest": _fastest(activities),
        "longest_time": max(activities, key=lambda a: a["duration_sec"] or 0),
        "most_calories": max(with_kcal, key=lambda a: a["calories"]) if with_kcal else None,
        "top_cadence": max(with_cad, key=lambda a: a["avg_cadence"]) if with_cad else None,
    }
