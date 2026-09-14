import sys
import zoneinfo
from datetime import datetime, timedelta
from datetime import timezone as dt_timezone

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


class TestParseDateTimeIsoWithOffset:
    """The ISO-with-offset path must honour ``timezone`` and accept negative offsets."""

    @pytest.mark.parametrize(
        ("timezone", "expected"),
        [
            ("Asia/Hong_Kong", "2021-10-17T06:10:56+08:00"),
            ("America/New_York", "2021-10-16T18:10:56-04:00"),
            ("UTC", "2021-10-16T22:10:56+00:00"),
        ],
    )
    def test_should_convert_to_the_requested_zone_when_offset_is_positive(self, timezone, expected):
        dt = parse_datetime("2021-10-17T06:10:56+08:00", timezone)
        assert dt.isoformat() == expected

    @pytest.mark.parametrize(
        ("timezone", "expected"),
        [
            ("Asia/Hong_Kong", "2021-10-17T19:10:56+08:00"),
            ("America/New_York", "2021-10-17T07:10:56-04:00"),
            ("UTC", "2021-10-17T11:10:56+00:00"),
        ],
    )
    def test_should_convert_to_the_requested_zone_when_offset_is_negative(self, timezone, expected):
        dt = parse_datetime("2021-10-17T06:10:56-05:00", timezone)
        assert dt.isoformat() == expected

    @pytest.mark.parametrize("timezone", ["Asia/Hong_Kong", "America/New_York", "UTC"])
    def test_should_keep_the_same_instant_whatever_the_requested_zone(self, timezone):
        # Converting must only restate the instant in another zone, never move it.
        dt = parse_datetime("2021-10-17T06:10:56+08:00", timezone)
        assert dt == datetime(2021, 10, 16, 22, 10, 56, tzinfo=dt_timezone.utc)


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


# Every shape below is a legal ``parse_datetime`` input. The absolute-instant shapes name
# the UTC instant they encode; the naive shape is zone-relative by design (AU-003), so it
# names a wall clock that each zone re-anchors instead.
_ABSOLUTE_SHAPES = [
    ("2021-10-16T22:10:56.000Z", datetime(2021, 10, 16, 22, 10, 56, tzinfo=dt_timezone.utc)),
    ("2021-10-17T06:10:56Z", datetime(2021, 10, 17, 6, 10, 56, tzinfo=dt_timezone.utc)),
    ("2021-10-17T06:10:56+08:00", datetime(2021, 10, 16, 22, 10, 56, tzinfo=dt_timezone.utc)),
    ("2021-10-17T06:10:56-05:00", datetime(2021, 10, 17, 11, 10, 56, tzinfo=dt_timezone.utc)),
    ("2021-10-17T06:10:56+0800", datetime(2021, 10, 16, 22, 10, 56, tzinfo=dt_timezone.utc)),
    ("1634422256000", datetime(2021, 10, 16, 22, 10, 56, tzinfo=dt_timezone.utc)),
]

_ZONES = ["Asia/Hong_Kong", "America/New_York", "UTC"]


class TestParseDateTimeShapeMatrix:
    """Every legal shape crossed with every zone, pinned to an absolute instant.

    CUI-0006 (negative offsets) and CUI-0016 (UTC without milliseconds) were both missed
    because the parser guessed the shape from a surface feature -- ``"+" in rec_time``,
    then ``"." in rec_time`` -- instead of asking a parser to read the string. This matrix
    is the regression net for that whole class of bug, not for one shape at a time.
    """

    @pytest.mark.parametrize(("rec_time", "instant"), _ABSOLUTE_SHAPES)
    @pytest.mark.parametrize("timezone", _ZONES)
    def test_should_land_on_the_same_instant_when_zone_varies(self, rec_time, instant, timezone):
        assert parse_datetime(rec_time, timezone) == instant

    @pytest.mark.parametrize(("rec_time", "instant"), _ABSOLUTE_SHAPES)
    @pytest.mark.parametrize("timezone", _ZONES)
    def test_should_report_the_requested_zones_offset(self, rec_time, instant, timezone):
        expected_offset = instant.astimezone(zoneinfo.ZoneInfo(timezone)).utcoffset()
        assert parse_datetime(rec_time, timezone).utcoffset() == expected_offset

    @pytest.mark.parametrize(
        ("timezone", "expected"),
        [
            ("Asia/Hong_Kong", "2021-10-17T06:10:56+08:00"),
            ("America/New_York", "2021-10-17T06:10:56-04:00"),
            ("UTC", "2021-10-17T06:10:56+00:00"),
        ],
    )
    def test_should_keep_the_naive_shape_zone_relative(self, timezone, expected):
        # AU-003: a naive string carries no offset, so it means that wall clock *in the
        # requested zone*. Unlike the shapes above, its instant is expected to move.
        assert parse_datetime("2021-10-17 06:10:56", timezone).isoformat() == expected


class TestParseDateTimeUtcWithoutMilliseconds:
    """CUI-0016: ``2021-10-17T06:10:56Z`` is UTC whether or not milliseconds are present."""

    @pytest.mark.parametrize(
        ("timezone", "expected"),
        [
            ("Asia/Hong_Kong", "2021-10-17T14:10:56+08:00"),
            ("America/New_York", "2021-10-17T02:10:56-04:00"),
            ("UTC", "2021-10-17T06:10:56+00:00"),
        ],
    )
    def test_should_read_z_as_utc_when_milliseconds_are_absent(self, timezone, expected):
        assert parse_datetime("2021-10-17T06:10:56Z", timezone).isoformat() == expected

    def test_should_agree_with_the_millisecond_form_when_both_name_one_instant(self):
        assert parse_datetime("2021-10-17T06:10:56Z") == parse_datetime("2021-10-17T06:10:56.000Z")


class TestParseDateTimeEpochAmbiguity:
    """A 13-digit epoch must never be read as an ISO 8601 basic-format datetime.

    ``dateutil.parser.isoparse`` accepts basic format, so a minority of 13-digit epochs
    are also readable as ``YYYYMMDD`` + a time -- ``1591101132141`` parses as year 1591.
    The epoch shape is fully specified (13 ASCII digits), so it is settled before the ISO
    reader ever sees the string.
    """

    @pytest.mark.parametrize(
        ("rec_time", "expected"),
        [
            ("1591101132141", "2020-06-02T20:32:12.100000+08:00"),
            ("1088102231633", "2004-06-25T02:37:11.600000+08:00"),
        ],
    )
    def test_should_read_as_epoch_when_the_digits_also_form_a_basic_iso_date(
        self, rec_time, expected
    ):
        assert parse_datetime(rec_time).isoformat() == expected


class TestParseDateTimeUnrecognised:
    def test_should_raise_value_error_when_the_string_is_not_a_timestamp(self):
        with pytest.raises(ValueError):
            parse_datetime("not a timestamp")
