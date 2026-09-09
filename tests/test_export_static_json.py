import json

from run365days.export.records import TRACK_COLUMNS
from run365days.export.static_json import write_static_json


def _read(path):
    return json.loads(path.read_text())


def test_writes_collections_and_tracks(sample_records, tmp_path):
    out = tmp_path / "static"
    write_static_json(sample_records, out)
    assert sorted(p.name for p in out.iterdir()) == [
        "activities.json",
        "meta.json",
        "tracks",
        "warnings.json",
        "weather.json",
        "weight.json",
    ]
    assert sorted(p.name for p in (out / "tracks").iterdir()) == ["a.json", "b.json"]


def test_meta_counts_match_collections(sample_records, tmp_path):
    out = tmp_path / "static"
    write_static_json(sample_records, out)
    meta = _read(out / "meta.json")
    assert meta["year"] == 2021
    assert meta["generated_at"] == "2026-01-01T00:00:00"
    assert meta["counts"] == {"activities": 2, "weight": 2, "weather": 1, "warnings": 2}
    assert len(_read(out / "activities.json")) == 2


def test_activities_json_has_no_tracks_but_tracks_have_columns(sample_records, tmp_path):
    out = tmp_path / "static"
    write_static_json(sample_records, out)
    activity = _read(out / "activities.json")[0]
    assert "track" not in activity and "tracks" not in activity
    track = _read(out / "tracks" / "a.json")
    assert track["columns"] == list(TRACK_COLUMNS)
    assert len(track["rows"]) == 4


def test_output_dir_is_replaced(sample_records, tmp_path):
    out = tmp_path / "static"
    out.mkdir()
    (out / "stale.json").write_text("{}")
    write_static_json(sample_records, out)
    assert not (out / "stale.json").exists()
