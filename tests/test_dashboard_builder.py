from run365days.activities.models import Activity, TrackPoint
from run365days.dashboard.builder import (
    downsample,
    hourly_at,
    total_ascent,
    track_rows,
    warnings_by_date,
)
from run365days.weight.analysis import parse_weight_file


def _pt(i, lat=22.3, lon=114.2, ele=330.0, cad=83, speed=2.7):
    return TrackPoint(
        lat=lat,
        lon=lon,
        time=f"2021-01-08 12:{i // 60:02d}:{i % 60:02d}",
        elevation=ele,
        cadence=cad,
        speed=speed,
        distance_m=i * 2.7,
    )


def _activity(n=10, gps=True):
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
