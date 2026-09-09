import json

import numpy as np

from run365days.activities.models import Activity
from run365days.cli.process_activities import _activities_to_jsonl


def _activity(**overrides) -> Activity:
    base = dict(
        activity_id="1",
        date="2021-01-01 06:10:00",
        total_time="00:30:00",
        total_sec=1800.0,
        distance_km=5.0,
        distance_by_coord_km=np.float64(4.98),
        num_track_points=np.int64(120),
        track_points=[object()],
    )
    base.update(overrides)
    return Activity(**base)


def test_numpy_scalars_are_serialised(tmp_path):
    out = tmp_path / "out.jsonl"
    _activities_to_jsonl([_activity()], out)
    record = json.loads(out.read_text().splitlines()[0])
    assert record["num_track_points"] == 120
    assert record["distance_by_coord_km"] == 4.98
    assert "track_points" not in record


def test_one_line_per_activity(tmp_path):
    out = tmp_path / "out.jsonl"
    _activities_to_jsonl([_activity(activity_id="a"), _activity(activity_id="b")], out)
    ids = [json.loads(line)["activity_id"] for line in out.read_text().splitlines()]
    assert ids == ["a", "b"]
