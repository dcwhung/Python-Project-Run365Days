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

# Both constants come from one real record in data/raw/garmin/summarized_activities.json
# (activityId 7669951076, "Kowloon Running"): "beginTimestamp" == "startTimeGmt" ==
# 1634422256000, while "startTimeLocal" == 1634451056000. Garmin's *Local field is the
# HK wall clock encoded as if it were UTC, which is exactly the 8-hour mix-up AU-048
# fixed -- the epoch-ms path is fed real epochs, never *Local values.
GARMIN_BEGIN_TIMESTAMP_MS = "1634422256000"
GARMIN_START_TIME_LOCAL_MS = "1634451056000"


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
        dt = parse_datetime(GARMIN_BEGIN_TIMESTAMP_MS)
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
        dt = parse_datetime(GARMIN_BEGIN_TIMESTAMP_MS)
        assert dt.utcoffset() == timedelta(hours=8)

    def test_naive_local_returns_plus_eight_offset(self):
        dt = parse_datetime("2021-10-17 06:10:56")
        assert dt.utcoffset() == timedelta(hours=8)

    def test_explicit_timezone_argument_returns_plus_eight_offset(self):
        dt = parse_datetime("2021-10-17 06:10:56", timezone="Asia/Hong_Kong")
        assert dt.utcoffset() == timedelta(hours=8)


class TestParseDateTimeWallClockOfZonedInputs:
    """Inputs that already carry the target zone's wall clock must keep it.

    Both inputs below are the HK wall clock: one states the ``+08:00`` offset, the
    other is naive and is read as local by contract. Parsing must not move either.
    Absolute-instant inputs (UTC ``Z``, epoch milliseconds) are a different contract
    and live in :class:`TestParseDateTimeAbsoluteInstantInputs`.
    """

    @pytest.mark.parametrize(
        "rec_time",
        ["2021-10-17T06:10:56+08:00", "2021-10-17 06:10:56"],
    )
    def test_wall_clock_is_preserved(self, rec_time):
        dt = parse_datetime(rec_time)
        assert (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) == (
            2021,
            10,
            17,
            6,
            10,
            56,
        )


class TestParseDateTimeIsoOffsetIsConvertedToTimezone:
    """An ISO string with an offset names an instant and is converted into *timezone*.

    CUI-0006, and the class that replaces ``TestParseDateTimeOffsetIsReturnedAsWritten``.
    That one pinned the opposite contract on purpose -- the branch returned
    ``dateutil.parser.parse(rec_time)`` untouched, so the offset came back exactly as
    written and the *timezone* argument was dead on this one path of four (W-019) --
    and said in as many words that a future ``.astimezone()`` was what would turn it
    red. This is that ticket, so the pin is rewritten rather than worked around.

    Two halves of one fault, both fixed here: the missing conversion, and the
    ``elif "+" in rec_time`` test that made a *negative* offset miss the branch and
    die in the naive ``strptime`` below it. Both came of writing the branch around
    the only offset the sample data carries.

    Why it matters despite that data: the three Garmin exports of a single run
    disagree about format. GPX and TCX write UTC ``Z``, which is converted; KML
    writes a local offset, which was not. At ``+08:00`` the two agree, so a run
    recorded outside Hong Kong (KML ``+09:00``) would have silently landed an hour
    off its own GPX and TCX twins.
    """

    @pytest.mark.parametrize(
        ("timezone", "expected"),
        [
            ("Asia/Hong_Kong", "2021-10-17T05:10:56+08:00"),
            ("America/New_York", "2021-10-16T17:10:56-04:00"),
            ("UTC", "2021-10-16T21:10:56+00:00"),
        ],
    )
    def test_a_foreign_positive_offset_is_converted_to_the_requested_zone(self, timezone, expected):
        # +09:00 is what a Japanese run's KML would carry. The wall clock moves by
        # one hour into HK, which is exactly what the old behaviour did not do.
        assert (
            parse_datetime("2021-10-17T06:10:56+09:00", timezone=timezone).isoformat() == expected
        )

    @pytest.mark.parametrize(
        ("timezone", "expected"),
        [
            ("Asia/Hong_Kong", "2021-10-17T19:10:56+08:00"),
            ("America/New_York", "2021-10-17T07:10:56-04:00"),
            ("UTC", "2021-10-17T11:10:56+00:00"),
        ],
    )
    def test_a_negative_offset_is_parsed_rather_than_rejected(self, timezone, expected):
        # Previously a ValueError from the naive strptime: the branch tested for
        # "+", and a bare `"-" in rec_time` could not replace it because every
        # timestamp's date separators are minus signs. The offset has to be
        # recognised at the end of the string or not at all.
        assert (
            parse_datetime("2021-10-17T06:10:56-05:00", timezone=timezone).isoformat() == expected
        )

    @pytest.mark.parametrize(
        "rec_time",
        ["2021-10-17T06:10:56+09:00", "2021-10-17T06:10:56-05:00", "2021-10-17T06:10:56+00:00"],
        ids=["positive", "negative", "zero"],
    )
    def test_the_instant_is_the_same_whatever_timezone_asks_for(self, rec_time):
        # A conversion, not a reinterpretation: the argument decides how the
        # instant is *displayed*, never which instant it is.
        instants = {
            parse_datetime(rec_time, timezone=tz).timestamp()
            for tz in ("Asia/Hong_Kong", "America/New_York", "UTC")
        }
        assert len(instants) == 1
        assert instants == {parse_datetime(rec_time, timezone="UTC").timestamp()}

    def test_the_offset_follows_the_argument_not_the_input(self):
        # The inverse of the superseded test_the_timezone_argument_does_not_reach
        # _this_branch: two zones eight hours apart must now separate, where
        # before they returned byte-identical strings.
        hong_kong = parse_datetime("2021-10-17T06:10:56+09:00", timezone="Asia/Hong_Kong")
        new_york = parse_datetime("2021-10-17T06:10:56+09:00", timezone="America/New_York")

        assert hong_kong.utcoffset() == timedelta(hours=8)
        assert new_york.utcoffset() == timedelta(hours=-4)
        assert hong_kong.isoformat() != new_york.isoformat()

    def test_the_offset_the_callers_actually_feed_is_left_where_it_was(self):
        # The whole production data set is +08:00 under the Hong_Kong default,
        # where converting and not converting agree -- which is why this fix
        # rewrites 194,008 KML timestamps into the same bytes. Kept from the
        # superseded class, where it explained why the old defect was invisible;
        # here it is the regression guard for the 365 tracked activities.
        as_written = parse_datetime("2021-10-17T06:10:56+08:00")
        converted = parse_datetime("2021-10-16T22:10:56.000Z")

        assert as_written.isoformat() == "2021-10-17T06:10:56+08:00"
        assert as_written.timestamp() == converted.timestamp()
        assert as_written.utcoffset() == converted.utcoffset() == timedelta(hours=8)

    def test_a_utc_z_string_without_milliseconds_is_still_not_supported(self):
        # Deliberately left alone by CUI-0006. The Z branch requires a ".", so
        # this shape falls through to the naive strptime and raises -- the same
        # kind of sample-data-shaped condition as the "+" test fixed above, but a
        # different branch, absent from the data (every Z timestamp in the 365
        # GPX and TCX files carries milliseconds) and out of this ticket's scope.
        # The docstring says "not supported" and this keeps that honest.
        with pytest.raises(ValueError):
            parse_datetime("2021-10-16T22:10:56Z")


class TestParseDateTimeAbsoluteInstantInputs:
    """UTC ``Z`` strings and epoch milliseconds name an instant, not a wall clock.

    Both encode the same instant as ``2021-10-17 06:10:56+08:00``, so the wall clock
    *must* move by the target zone's offset while the instant stays put. Superseded
    AU-003's ``TestParseDateTimeWallClockUnchanged``, which pinned the epoch-ms case
    to the UTC wall clock relabelled ``+08:00`` -- a known-incorrect behaviour that
    AU-048 replaced with a real conversion.
    """

    @pytest.mark.parametrize("rec_time", ["2021-10-16T22:10:56.000Z", GARMIN_BEGIN_TIMESTAMP_MS])
    def test_converts_to_hong_kong_wall_clock(self, rec_time):
        dt = parse_datetime(rec_time)
        assert (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) == (
            2021,
            10,
            17,
            6,
            10,
            56,
        )
        assert dt.utcoffset() == timedelta(hours=8)

    @pytest.mark.parametrize("rec_time", ["2021-10-16T22:10:56.000Z", GARMIN_BEGIN_TIMESTAMP_MS])
    def test_instant_is_independent_of_the_timezone_argument(self, rec_time):
        hong_kong = parse_datetime(rec_time, timezone="Asia/Hong_Kong")
        new_york = parse_datetime(rec_time, timezone="America/New_York")

        # Same instant, different wall clocks -- the point of an absolute timestamp.
        assert hong_kong.timestamp() == new_york.timestamp() == 1634422256.0
        assert (hong_kong.hour, hong_kong.day) == (6, 17)
        assert (new_york.hour, new_york.day) == (18, 16)
        assert new_york.utcoffset() == timedelta(hours=-4)


class TestParseDateTimeEpochMilliseconds:
    """The 13-digit epoch-ms path (AU-048)."""

    def test_garmin_begin_timestamp_matches_its_documented_local_start(self):
        # summarized_activities.json pairs beginTimestamp 1634422256000 with
        # startTimeLocal 1634451056000, i.e. HK 2021-10-17 06:10:56.
        dt = parse_datetime(GARMIN_BEGIN_TIMESTAMP_MS)
        assert dt.isoformat() == "2021-10-17T06:10:56+08:00"

    def test_start_time_local_value_is_eight_hours_later_than_the_real_epoch(self):
        # Guards the original defect: startTimeLocal is the HK wall clock encoded as
        # UTC, so feeding it in must land 8 hours late rather than look correct.
        dt = parse_datetime(GARMIN_START_TIME_LOCAL_MS)
        assert dt.isoformat() == "2021-10-17T14:10:56+08:00"
        assert dt - parse_datetime(GARMIN_BEGIN_TIMESTAMP_MS) == timedelta(hours=8)

    def test_a_millisecond_residue_is_rounded_to_a_tenth_of_a_second(self):
        # The `round(..., 1)` in the epoch branch, which had no comment and no
        # test (S-031). It is coarser than its own input: 092ms becomes .100000,
        # 8ms this timestamp never carried. Pinned rather than corrected --
        # every value that actually reaches this path is a whole second, so
        # dropping the round changes nothing in the data and would still change
        # a parser three callers share. This is what makes that a decision.
        assert parse_datetime("1634422256092").isoformat() == "2021-10-17T06:10:56.100000+08:00"
        # A residue under 50ms goes the other way, to no sub-second part at all.
        assert parse_datetime("1634422256040").isoformat() == "2021-10-17T06:10:56+08:00"
        # And the whole-second case every tracked activity but one actually has.
        assert parse_datetime(GARMIN_BEGIN_TIMESTAMP_MS).microsecond == 0

    def test_ten_digit_epoch_seconds_are_not_treated_as_epoch_milliseconds(self):
        # Only 13-digit strings take the epoch path; anything else falls through to
        # the naive "%Y-%m-%d %H:%M:%S" parse and is rejected.
        with pytest.raises(ValueError):
            parse_datetime("1634422256")

    def test_float_formatted_epoch_is_not_treated_as_epoch_milliseconds(self):
        # Garmin renders startTimeGmt as 1634422256000.0; str.isdigit() is False for
        # it, so it must be rejected loudly instead of silently mis-parsed.
        with pytest.raises(ValueError):
            parse_datetime("1634422256000.0")

    def test_non_numeric_strings_still_take_the_iso_path(self):
        assert (
            parse_datetime("2021-10-17T06:10:56+08:00").isoformat() == "2021-10-17T06:10:56+08:00"
        )


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
