import json
import shutil
import sys
from pathlib import Path

import numpy as np
import pytest

from run365days.activities.models import Activity
from run365days.activities.parsers.gpx import GPXParser
from run365days.activities.parsers.kml import KMLParser
from run365days.activities.parsers.tcx import TCXParser
from run365days.cli import process_activities
from run365days.cli.process_activities import _activities_to_jsonl

_PARSER_CLASSES = {"gpx": GPXParser, "kml": KMLParser, "tcx": TCXParser}

# Fixture files that parse, and files every parser drops on purpose. Together
# they reproduce the real KML folder's shape: a directory that is far from
# empty yet yields fewer activities than it holds.
_GOOD_TCX = ["activity_1001.tcx", "activity_1002.tcx"]
_GOOD_KML = ["activity_3001.kml", "activity_3002.kml"]
_SKIPPED_KML = ["activity_3003.kml", "activity_3004.kml", "activity_3008.kml"]


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


def _install_parsers(monkeypatch, tmp_path: Path, fixtures_dir: Path, layout: dict) -> dict:
    """Point the CLI's parser table at throwaway directories.

    Args:
        monkeypatch: pytest's patcher.
        tmp_path: Root for the throwaway directories.
        fixtures_dir: Where the hand-written exports live.
        layout: Format name -> fixture file names its directory should hold, or
            ``None`` when the directory should not exist at all.

    Returns:
        Format name -> the JSONL path the CLI would write for it.
    """
    table = {}
    outputs = {}
    for name, files in layout.items():
        directory = tmp_path / name
        outputs[name] = tmp_path / f"activities_{name}.jsonl"
        if files is not None:
            directory.mkdir()
            for file_name in files:
                shutil.copy(fixtures_dir / file_name, directory / file_name)
        table[name] = (_PARSER_CLASSES[name], directory, outputs[name])
    monkeypatch.setattr(process_activities, "_PARSERS", table)
    return outputs


def _run_cli(monkeypatch, *args: str) -> None:
    monkeypatch.setattr(sys, "argv", ["run365-activities", "--year", "2021", *args])
    process_activities.main()


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


def test_export_fails_loudly_without_tcx_dir(tmp_path, monkeypatch):
    from run365days.cli import export_data

    monkeypatch.setattr("sys.argv", ["run365-export", "--tcx-dir", str(tmp_path / "missing")])
    with pytest.raises(SystemExit) as exc:
        export_data.main()
    assert "TCX directory not found" in str(exc.value)


def test_should_exit_non_zero_when_a_format_parses_nothing(tmp_path, fixtures_dir, monkeypatch):
    # Given an existing but empty KML directory
    _install_parsers(monkeypatch, tmp_path, fixtures_dir, {"kml": []})

    # When / Then
    with pytest.raises(SystemExit) as exc:
        _run_cli(monkeypatch, "--format", "kml")
    assert exc.value.code not in (0, None)
    assert "kml" in str(exc.value).lower()


def test_should_exit_non_zero_when_every_file_in_a_format_is_skipped(
    tmp_path, fixtures_dir, monkeypatch
):
    # Given a KML directory holding only files the parser drops
    _install_parsers(monkeypatch, tmp_path, fixtures_dir, {"kml": _SKIPPED_KML})

    # When / Then
    with pytest.raises(SystemExit):
        _run_cli(monkeypatch, "--format", "kml")


def test_should_keep_the_previous_output_when_a_format_parses_nothing(
    tmp_path, fixtures_dir, monkeypatch
):
    # Given a JSONL file left by an earlier, healthy run
    outputs = _install_parsers(monkeypatch, tmp_path, fixtures_dir, {"kml": []})
    outputs["kml"].write_text('{"activity_id": "from-a-good-run"}\n')

    # When
    with pytest.raises(SystemExit):
        _run_cli(monkeypatch, "--format", "kml")

    # Then the guard must not have truncated it on the way out
    assert outputs["kml"].read_text() == '{"activity_id": "from-a-good-run"}\n'


def test_should_exit_zero_when_only_some_files_are_skipped(tmp_path, fixtures_dir, monkeypatch):
    # Given a KML directory where most files are skipped but some parse --
    # the shape of the real folder (357 of 365, W-004)
    outputs = _install_parsers(
        monkeypatch, tmp_path, fixtures_dir, {"kml": _GOOD_KML + _SKIPPED_KML}
    )

    # When
    _run_cli(monkeypatch, "--format", "kml")

    # Then
    assert len(outputs["kml"].read_text().splitlines()) == len(_GOOD_KML)


def test_should_process_every_format_before_failing(tmp_path, fixtures_dir, monkeypatch):
    # Given an empty KML directory ordered before a healthy TCX one
    outputs = _install_parsers(monkeypatch, tmp_path, fixtures_dir, {"kml": [], "tcx": _GOOD_TCX})

    # When
    with pytest.raises(SystemExit):
        _run_cli(monkeypatch, "--format", "all")

    # Then the later format still ran and wrote its records
    assert len(outputs["tcx"].read_text().splitlines()) == len(_GOOD_TCX)


def test_should_name_every_empty_format_in_the_failure(tmp_path, fixtures_dir, monkeypatch):
    # Given two empty formats and one healthy one
    _install_parsers(monkeypatch, tmp_path, fixtures_dir, {"gpx": [], "kml": [], "tcx": _GOOD_TCX})

    # When
    with pytest.raises(SystemExit) as exc:
        _run_cli(monkeypatch, "--format", "all")

    # Then
    message = str(exc.value).lower()
    assert "gpx" in message
    assert "kml" in message
    assert "tcx" not in message


def test_should_exit_zero_when_a_format_directory_does_not_exist(
    tmp_path, fixtures_dir, monkeypatch
):
    # Given an environment that only ships TCX exports
    outputs = _install_parsers(monkeypatch, tmp_path, fixtures_dir, {"kml": None, "tcx": _GOOD_TCX})

    # When / Then --format all stays usable: an absent directory is a
    # configuration statement, not a data failure
    _run_cli(monkeypatch, "--format", "all")
    assert not outputs["kml"].exists()
    assert len(outputs["tcx"].read_text().splitlines()) == len(_GOOD_TCX)


def _cli_help(capsys, monkeypatch) -> str:
    """Return the real ``run365-activities --help`` text."""
    monkeypatch.setattr(sys, "argv", ["run365-activities", "--help"])
    with pytest.raises(SystemExit):
        process_activities.main()
    return capsys.readouterr().out


class TestYearIsALowerBound:
    """CUI-0020: --year skips what precedes it and keeps everything after it.

    The filter every parser applies is ``act_time.year < current_year``, so the
    option is a lower bound, not an equality test. The behaviour is deliberate --
    a 2021 challenge should not discard a file recorded after the fact -- so the
    help text is what has to change.
    """

    def test_should_keep_activities_recorded_after_the_target_year(self, fixtures_dir, tmp_path):
        kml_dir = tmp_path / "kml"
        kml_dir.mkdir()
        for name in _GOOD_KML:
            shutil.copy(fixtures_dir / name, kml_dir / name)
        kept_1999 = [a.activity_id for a in KMLParser(current_year=1999).parse_all(kml_dir)]
        kept_2021 = [a.activity_id for a in KMLParser(current_year=2021).parse_all(kml_dir)]
        # A year long past keeps everything readable; a later one skips it all.
        assert kept_1999 == kept_2021 == ["3001", "3002"]
        assert KMLParser(current_year=2022).parse_all(kml_dir) == []

    def test_year_help_should_not_promise_an_exact_year_match(self, capsys, monkeypatch):
        help_text = _cli_help(capsys, monkeypatch)
        assert "activities from other years are skipped" not in help_text
        assert "earlier" in help_text

    def test_run_docstring_should_not_promise_an_exact_year_match(self):
        assert "activities from other years are skipped" not in process_activities.run.__doc__
