import pytest

from run365days.common.time import (
    hhmmss_to_seconds,
    pace_str,
    parse_datetime,
    seconds_to_hhmmss,
)


class TestParseDateTime:
    def test_utc_with_millis(self):
        dt = parse_datetime("2021-10-16T22:10:56.000Z")
        # Should be converted to HK time: +8h => 2021-10-17 06:10:56
        assert dt.year == 2021
        assert dt.month == 10
        assert dt.day == 17
        assert dt.hour == 6

    def test_iso_with_offset(self):
        dt = parse_datetime("2021-10-17T06:10:56+08:00")
        assert dt.hour == 6
        assert dt.minute == 10

    def test_unix_ms(self):
        dt = parse_datetime("1634451056000")
        assert dt.year == 2021

    def test_naive_local(self):
        dt = parse_datetime("2021-10-17 06:10:56")
        assert dt.hour == 6
        assert dt.minute == 10
        assert dt.second == 56


class TestTimeConversions:
    def test_seconds_to_hhmmss(self):
        assert seconds_to_hhmmss(3661) == "1:01:01"

    def test_seconds_to_hhmmss_under_hour(self):
        assert seconds_to_hhmmss(1959) == "0:32:39"

    def test_hhmmss_to_seconds(self):
        assert hhmmss_to_seconds("00:32:39") == pytest.approx(1959.0)

    def test_roundtrip(self):
        original = 7384.0
        assert hhmmss_to_seconds(seconds_to_hhmmss(original)) == pytest.approx(original, abs=1)


class TestPaceStr:
    def test_basic_pace(self):
        # 1959 sec over 6.09 km => ~5:21 per km
        result = pace_str(1959, 6.09)
        assert result.startswith("0:05:")

    def test_zero_distance_returns_empty(self):
        assert pace_str(1000, 0) == ""
