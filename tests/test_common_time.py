import sys
import zoneinfo
from datetime import timedelta

import pytest

from run365days.common.time import (
    MissingTimeZoneDataError,
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
        dt = parse_datetime("1634422256000")
        assert dt.year == 2021

    def test_naive_local(self):
        dt = parse_datetime("2021-10-17 06:10:56")
        assert dt.hour == 6
        assert dt.minute == 10
        assert dt.second == 56


class TestParseDateTimeOffset:
    """Every input path must land on the real HK offset, never the pre-1904 LMT."""

    def test_utc_with_millis_returns_plus_eight_offset(self):
        dt = parse_datetime("2021-10-16T22:10:56.000Z")
        assert dt.utcoffset() == timedelta(hours=8)

    def test_iso_with_offset_returns_plus_eight_offset(self):
        dt = parse_datetime("2021-10-17T06:10:56+08:00")
        assert dt.utcoffset() == timedelta(hours=8)

    def test_unix_ms_returns_plus_eight_offset(self):
        dt = parse_datetime("1634422256000")
        assert dt.utcoffset() == timedelta(hours=8)

    def test_naive_local_returns_plus_eight_offset(self):
        dt = parse_datetime("2021-10-17 06:10:56")
        assert dt.utcoffset() == timedelta(hours=8)

    def test_explicit_timezone_argument_returns_plus_eight_offset(self):
        dt = parse_datetime("2021-10-17 06:10:56", timezone="Asia/Hong_Kong")
        assert dt.utcoffset() == timedelta(hours=8)


class TestParseDateTimeEpochMilliseconds:
    """The epoch-ms path must convert the instant, not relabel the UTC wall clock."""

    def test_should_return_the_hong_kong_wall_clock_when_a_real_epoch_is_given(self):
        # 1634422256000 is a real beginTimestamp from data/raw/garmin/summarized_activities.json.
        dt = parse_datetime("1634422256000")
        assert dt.isoformat() == "2021-10-17T06:10:56+08:00"

    def test_should_return_eight_hours_later_when_the_fabricated_constant_is_given(self):
        # 1634451056000 is the constant the old tests used: exactly 28_800_000 ms above the
        # real one, because its author encoded the HK wall clock as if it were UTC. Pinning
        # it here documents why the pre-AU-048 suite was green against a broken conversion.
        dt = parse_datetime("1634451056000")
        assert dt.isoformat() == "2021-10-17T14:10:56+08:00"


class TestParseDateTimeWallClockUnchanged:
    """Pins the wall clock of every ``parse_datetime`` path.

    All four inputs below encode the same instant, so every path must land on the same
    Hong Kong wall clock. AU-048 replaced the epoch-ms case's fabricated ``1634451056000``
    with ``1634422256000``, the value that really appears in
    ``data/raw/garmin/summarized_activities.json``, once that path converted the epoch
    instead of relabelling it.
    """

    @pytest.mark.parametrize(
        ("rec_time", "expected"),
        [
            ("2021-10-16T22:10:56.000Z", (2021, 10, 17, 6, 10, 56)),
            ("2021-10-17T06:10:56+08:00", (2021, 10, 17, 6, 10, 56)),
            ("1634422256000", (2021, 10, 17, 6, 10, 56)),
            ("2021-10-17 06:10:56", (2021, 10, 17, 6, 10, 56)),
        ],
    )
    def test_wall_clock_is_preserved(self, rec_time, expected):
        dt = parse_datetime(rec_time)
        assert (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) == expected


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


@pytest.fixture
def no_tz_database(monkeypatch):
    """Simulate a host with neither /usr/share/zoneinfo nor the tzdata package."""
    # zoneinfo consults TZPATH first and only then the tzdata PyPI package, so
    # both routes have to be cut for the fixture to reproduce a bare container.
    monkeypatch.setitem(sys.modules, "tzdata", None)
    zoneinfo.reset_tzpath(to=[])
    zoneinfo.ZoneInfo.clear_cache()
    yield
    zoneinfo.reset_tzpath()
    zoneinfo.ZoneInfo.clear_cache()


class TestParseDateTimeWithoutTimeZoneDatabase:
    def test_should_name_tzdata_in_the_error_when_tz_database_missing(self, no_tz_database):
        with pytest.raises(MissingTimeZoneDataError) as excinfo:
            parse_datetime("2021-10-17 06:10:56")
        assert "tzdata" in str(excinfo.value)

    def test_should_not_raise_a_data_error_when_tz_database_missing(self, no_tz_database):
        # ZoneInfoNotFoundError is a KeyError, so callers that filter on bad-data
        # exceptions could mistake a missing tz database for an unreadable file.
        with pytest.raises(MissingTimeZoneDataError) as excinfo:
            parse_datetime("2021-10-17 06:10:56")
        assert not isinstance(excinfo.value, LookupError)
        assert not isinstance(excinfo.value, ValueError)
