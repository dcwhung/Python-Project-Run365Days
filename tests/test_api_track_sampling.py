"""AU-047: ``{ activities { track } }`` must not fan out to two queries per run.

The sampling itself is covered in ``test_api.py``; what is pinned here is the
number of SQL round trips it costs, and that batching the row counts leaves the
sampled points untouched.
"""

from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from run365days.api import db, service
from run365days.export import models
from run365days.export.sqlite import sqlite_url

# Deliberately mixed: longer than any sample request, shorter than it, and one
# run with no track at all -- a GPS-less activity is a row with zero points.
TRACK_LENGTHS = (600, 401, 250, 0, 97, 33)
SAMPLE_POINTS = 25
COUNT_QUERIES = 2
"""Round trips spent on row counts per session: one for the first track, one
grouped scan that answers every other."""


def _activity(index: int, n_points: int) -> models.Activity:
    """One run on day *index* carrying *n_points* stored samples."""
    day = f"2021-{index // 28 + 1:02d}-{index % 28 + 1:02d}"
    return models.Activity(
        id=f"r{index}",
        date=day,
        start_time=f"{day} 06:00:00",
        day_of_year=index + 1,
        distance_km=5.0,
        duration_sec=1800,
        has_gps=n_points > 0,
        track_points=[
            models.TrackPoint(seq=s, sec=s, lat=22.3 + s / 1e4, lon=114.2 - s / 1e4)
            for s in range(n_points)
        ],
    )


@pytest.fixture
def track_db(tmp_path) -> Path:
    """A database whose runs have deliberately uneven track lengths."""
    path = tmp_path / "tracks.db"
    engine = create_engine(sqlite_url(path))
    models.Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                models.Meta(key="year", value="2021"),
                models.Meta(key="generated_at", value="2026-01-01T00:00:00"),
            ]
        )
        session.add_all(_activity(i, n) for i, n in enumerate(TRACK_LENGTHS))
        session.commit()
    engine.dispose()
    return path


@pytest.fixture
def counting_engine(track_db):
    """The read-only engine plus a list every statement it runs is appended to."""
    engine = db.make_engine(track_db)
    executed: list[str] = []
    event.listen(
        engine,
        "before_cursor_execute",
        lambda conn, cur, stmt, params, ctx, many: executed.append(stmt),
    )
    yield engine, executed
    engine.dispose()


def _resolve_tracks(engine, activity_ids, points=SAMPLE_POINTS):
    """Resolve ``track`` for each id the way one GraphQL request would."""
    with Session(engine) as session:
        return [service.track(session, aid, points) for aid in activity_ids]


def _ids(engine) -> list[str]:
    with Session(engine) as session:
        return [a["id"] for a in service.activities(session)]


# ── the fan-out itself ─────────────────────────────────────────────────────
@pytest.mark.parametrize("n_activities", [3, 4, len(TRACK_LENGTHS)])
def test_track_costs_one_query_per_activity_plus_the_batched_counts(counting_engine, n_activities):
    engine, executed = counting_engine
    ids = _ids(engine)[:n_activities]
    executed.clear()

    _resolve_tracks(engine, ids)

    assert len(executed) == n_activities + COUNT_QUERIES
    assert len(executed) < 2 * n_activities, "still two round trips per activity"


def test_query_count_grows_by_one_per_extra_activity(counting_engine):
    """The slope is what matters: one row query each, not one row query and a count."""
    engine, executed = counting_engine
    ids = _ids(engine)

    executed.clear()
    _resolve_tracks(engine, ids[:2])
    small = len(executed)

    executed.clear()
    _resolve_tracks(engine, ids)
    large = len(executed)

    assert large - small == len(ids) - 2


def test_a_single_activity_request_is_not_made_slower(counting_engine):
    """A one-run page must not pay for a scan of every other run's points.

    Both statements stay narrowed to the one activity, which SQLite answers as a
    seek into the ``(activity_id, seq)`` index rather than a scan of all of it.
    """
    engine, executed = counting_engine
    ids = _ids(engine)[:1]
    executed.clear()

    _resolve_tracks(engine, ids)

    assert len(executed) == 2
    assert all("track_points.activity_id = ?" in stmt for stmt in executed)


def test_repeating_one_activity_does_not_recount_it(counting_engine):
    engine, executed = counting_engine
    aid = _ids(engine)[0]
    with Session(engine) as session:
        service.track(session, aid, SAMPLE_POINTS)
        executed.clear()
        service.track(session, aid, SAMPLE_POINTS)

    assert len(executed) == 1


def test_runs_without_a_track_cost_no_further_counting(counting_engine):
    """Empty tracks are absent from the grouped counts; they must not retrigger it."""
    engine, executed = counting_engine
    ids = _ids(engine)
    empty = [i for i, n in enumerate(TRACK_LENGTHS) if n == 0]
    assert empty, "fixture must contain a run with no stored points"

    with Session(engine) as session:
        service.track(session, ids[0], SAMPLE_POINTS)
        service.track(session, ids[empty[0]], SAMPLE_POINTS)
        executed.clear()
        rows = service.track(session, ids[empty[0]], SAMPLE_POINTS)

    assert rows == []
    assert len(executed) == 1


# ── the points themselves must not move ────────────────────────────────────
@pytest.mark.parametrize("points", [1, 2, 3, 25, 250, 599, 600, 1000])
def test_batched_counting_returns_the_same_points_as_counting_alone(track_db, points):
    """A warm count cache must be indistinguishable from a cold one, sample for sample."""
    engine = db.make_engine(track_db)
    ids = _ids(engine)

    with Session(engine) as shared:
        warm = [service.track(shared, aid, points) for aid in ids]
    cold = []
    for aid in ids:
        with Session(engine) as fresh:
            cold.append(service.track(fresh, aid, points))
    engine.dispose()

    assert warm == cold


def test_every_track_is_capped_at_the_requested_points(track_db):
    engine = db.make_engine(track_db)
    with Session(engine) as session:
        lengths = [len(service.track(session, a, SAMPLE_POINTS)) for a in _ids(engine)]
    engine.dispose()
    assert lengths == [min(n, SAMPLE_POINTS) for n in TRACK_LENGTHS]


def test_counts_are_read_from_the_database_not_the_activity_row(track_db):
    """The count must come from the stored points, which is what sampling indexes."""
    engine = db.make_engine(track_db)
    with Session(engine) as session:
        stored = {
            a: session.scalar(
                select(models.TrackPoint)
                .where(models.TrackPoint.activity_id == a)
                .order_by(models.TrackPoint.seq.desc())
            )
            for a in _ids(engine)
        }
        last = {a: service.track(session, a, 1) for a in _ids(engine)}
    engine.dispose()

    for aid, point in stored.items():
        expected = [] if point is None else [point.sec]
        assert [r["sec"] for r in last[aid]] == expected
