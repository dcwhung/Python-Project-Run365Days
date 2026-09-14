import pathlib
import re

import pytest

from run365days.activities.models import Activity, TrackPoint
from run365days.common import config
from run365days.dashboard.builder import (
    TRACK_POINT_LIMIT,
    downsample,
    hourly_at,
    total_ascent,
    track_rows,
    warnings_by_date,
)
from run365days.weight.analysis import parse_weight_file


def _pt(i, lat=22.3, lon=114.2, ele=330.0, cad=83, speed=2.7) -> TrackPoint:
    return TrackPoint(
        lat=lat,
        lon=lon,
        time=f"2021-01-08 12:{i // 60:02d}:{i % 60:02d}",
        elevation=ele,
        cadence=cad,
        speed=speed,
        distance_m=i * 2.7,
    )


def _activity(n=10, gps=True) -> Activity:
    pts = [_pt(i, lat=22.3 if gps else None, lon=114.2 if gps else None) for i in range(n)]
    return Activity(
        activity_id="123",
        date="2021-01-08 12:04:52",
        total_time="0:30:04",
        total_sec=1804.0,
        distance_km=6.35,
        distance_by_coord_km=6.2,
        calories=341,
        num_track_points=n,
        track_points=pts,
    )


class TestDownsample:
    def test_short_list_untouched(self):
        assert downsample([1, 2, 3], 10) == [1, 2, 3]

    def test_keeps_first_and_last(self):
        out = downsample(list(range(1000)), 50)
        assert len(out) == 50
        assert out[0] == 0 and out[-1] == 999

    def test_monotonic(self):
        out = downsample(list(range(1000)), 150)
        assert out == sorted(out)


class TestTotalAscent:
    def test_flat_is_zero(self):
        assert total_ascent([100.0] * 20) == 0.0

    def test_steady_climb(self):
        # smoothing flattens the first/last few samples, so slightly under 29
        assert 24.0 <= total_ascent([float(i) for i in range(30)]) <= 29.0

    def test_jitter_is_suppressed(self):
        # ±1 m alternating noise sums to 39 m raw; smoothing must cut most of it
        noisy = [100.0 + (1 if i % 2 else -1) for i in range(40)]
        assert total_ascent(noisy) < 6.0


class TestTrackRows:
    def test_row_shape(self):
        rows = track_rows(_activity(3), {"2021-01-08 12:00:01": 18.5})
        assert len(rows) == 3
        assert rows[0][0] == 0  # seconds from start
        assert rows[1][7] == 18.5  # temperature merged by timestamp
        assert rows[0][7] is None

    def test_no_gps_gives_null_coords(self):
        rows = track_rows(_activity(2, gps=False), {})
        assert rows[0][1] is None and rows[0][2] is None

    # CUI-0007: ±inf used to slip past the nan-only guard and reach the writers,
    # where json.dumps refuses it and SQLite happily stores Infinity.
    @pytest.mark.parametrize("bad", [float("inf"), float("-inf"), float("nan")])
    def test_non_finite_point_values_become_null(self, bad):
        point = _pt(0, lat=bad, lon=bad, ele=bad, cad=bad, speed=bad)
        point.distance_m = bad
        activity = _activity(1)
        activity.track_points = [point]
        row = track_rows(activity, {"2021-01-08 12:00:00": bad})
        assert row[0][1:] == [None] * 7
        assert row[0][0] == 0  # sec is NOT NULL and stays a number


class TestWeather:
    ROWS = [
        {
            "Date": "2021-01-08",
            "Time": "11:30",
            "Temperature (°C)": 17,
            "Humidity (%)": 50,
            "Wind (Km/h)": "10",
            "Description": "Rain",
        },
        {
            "Date": "2021-01-08",
            "Time": "12:00",
            "Temperature (°C)": 18,
            "Humidity (%)": 55,
            "Wind (Km/h)": "12",
            "Description": "Few clouds",
        },
        {
            "Date": "2021-01-09",
            "Time": "12:00",
            "Temperature (°C)": 25,
            "Humidity (%)": 60,
            "Wind (Km/h)": "5",
            "Description": "Clear weather",
        },
    ]

    def test_hourly_picks_nearest_slot(self):
        wx = hourly_at(self.ROWS, "2021-01-08", "12:04")
        assert wx["desc"] == "Few clouds" and wx["temp"] == 18.0 and wx["wind"] == 12.0

    def test_hourly_missing_date(self):
        assert hourly_at(self.ROWS, "2021-02-01", "12:00") is None

    # ── CUI-0009: "inf" / "nan" are legal float literals, so to_float lets them through ──
    def test_hourly_non_finite_readings_become_none(self):
        rows = [
            {
                "Date": "2021-01-08",
                "Time": "12:00",
                "Temperature (°C)": "inf",
                "Humidity (%)": "-inf",
                "Wind (Km/h)": "nan",
                "Description": "Few clouds",
            }
        ]
        wx = hourly_at(rows, "2021-01-08", "12:04")
        assert (wx["temp"], wx["hum"], wx["wind"]) == (None, None, None)

    def test_warnings_dedup_by_date(self):
        rows = [
            {"Date": "2021-07-01", "Warning_Signal": "THUNDERSTORM WARNING"},
            {"Date": "2021-07-01", "Warning_Signal": "THUNDERSTORM WARNING"},
            {"Date": "2021-07-01", "Warning_Signal": "AMBER RAINSTORM WARNING SIGNAL"},
        ]
        assert warnings_by_date(rows) == {
            "2021-07-01": ["THUNDERSTORM WARNING", "AMBER RAINSTORM WARNING SIGNAL"]
        }


class TestWeightUndatedLine:
    def test_last_undated_line_is_next_day(self, tmp_path):
        f = tmp_path / "w.txt"
        f.write_text("2021 Weight\n\n127.2 lbs (30/12)\n128.8 lbs\n")
        recs = parse_weight_file(f, year=2021)
        assert [(r.date, r.weight_lbs) for r in recs] == [
            ("2021-12-30", 127.2),
            ("2021-12-31", 128.8),
        ]

    def test_undated_first_line_is_ignored(self, tmp_path):
        f = tmp_path / "w.txt"
        f.write_text("128.8 lbs\n154.8 lbs (1/1)\n")
        assert len(parse_weight_file(f, year=2021)) == 1


_SRC = pathlib.Path(__file__).resolve().parents[1] / "src"


def _files_spelling(number: str) -> set[str]:
    """Modules under ``src`` that write *number* out, as a path relative to ``src``.

    A constant with one owner is spelled once. Comparing this set to a single
    expected file is what catches the next hand-typed copy, which is how the
    values in AU-008's table drifted apart in the first place: equality between
    two named constants only holds them together once someone has already
    thought to import one from the other.
    """
    pattern = re.compile(rf"(?<![\w.]){re.escape(number)}(?![\w.])")
    return {
        str(path.relative_to(_SRC))
        for path in _SRC.rglob("*.py")
        if pattern.search(path.read_text())
    }


class TestOneOwnerPerConstant:
    """Every domain constant is written down in exactly one module (AU-008)."""

    @pytest.mark.parametrize(
        ("value", "owner"),
        [
            (config.BODY_HEIGHT_CM, "common/config.py"),
            (config.LBS_TO_KG, "common/config.py"),
            (TRACK_POINT_LIMIT, "dashboard/builder.py"),
            (config.EXPORT_TRACK_POINTS, "common/config.py"),
        ],
        ids=["body height cm", "pounds to kilograms", "samples served", "samples stored"],
    )
    def test_the_number_is_written_down_once(self, value, owner):
        # Read off the constant rather than retyped, so changing one of these
        # values stays a one-line change and this test keeps guarding the shape
        # of the code -- one owner -- instead of pinning a particular number.
        assert _files_spelling(str(value)) == {owner}


class TestTrackPointBudgets:
    """The two track-point constants are different questions with one answer each.

    ``TRACK_POINT_LIMIT`` is how many samples a caller gets when it names no
    count; ``EXPORT_TRACK_POINTS`` is how many the export writes to disk. They
    are independent numbers but not an independent pair: the export cannot
    serve a default it never stored, so nothing may raise the served default
    past the stored one.
    """

    def test_the_export_stores_at_least_the_served_default(self):
        assert config.EXPORT_TRACK_POINTS >= TRACK_POINT_LIMIT

    def test_the_api_serves_the_downsampler_default(self):
        from run365days.api import schema

        assert schema.DEFAULT_TRACK_POINTS == TRACK_POINT_LIMIT

    def test_the_cli_stores_the_configured_count(self):
        from run365days.cli import export_data

        assert export_data.DEFAULT_POINT_LIMIT == config.EXPORT_TRACK_POINTS

    def test_the_published_schema_states_the_stored_count(self):
        from run365days.api.schema import TRACK_POINTS_DESCRIPTION

        assert f"{config.EXPORT_TRACK_POINTS} samples per run" in TRACK_POINTS_DESCRIPTION
