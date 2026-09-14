import logging
import xml.etree.ElementTree as ET

import pytest

from run365days.activities.parsers.base import (
    ActivityParseError,
    ActivitySkipped,
    optional_float,
    optional_int,
)
from run365days.activities.parsers.gpx import GPXParser
from run365days.activities.parsers.kml import KMLParser
from run365days.activities.parsers.tcx import TCXParser

CHALLENGE_YEAR = 2021


class TestTCXParser:
    def test_golden_file_yields_device_totals(self, fixtures_dir):
        act = TCXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_1001.tcx")
        assert act.activity_id == "1001"
        assert act.date == "2021-01-08 12:04:52"
        assert act.total_sec == 600.0
        assert act.total_time == "0:10:00"
        assert act.distance_km == 2.0
        assert act.pacing == "0:05:00"
        assert act.calories == 150

    def test_golden_file_yields_full_track_points(self, fixtures_dir):
        act = TCXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_1001.tcx")
        assert len(act.track_points) == 2
        first = act.track_points[0]
        assert (first.lat, first.lon) == (22.3278, 114.2019)
        assert first.time == "2021-01-08 12:04:52"
        assert first.elevation == 329.8
        assert first.speed == 2.87
        assert first.cadence == 85

    def test_should_keep_activity_when_altitude_meters_missing(self, fixtures_dir):
        act = TCXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_1002.tcx")
        assert act.activity_id == "1002"
        assert act.distance_km == 1.0
        assert len(act.track_points) == 2
        assert all(tp.elevation is None for tp in act.track_points)

    def test_should_keep_activity_when_position_missing(self, fixtures_dir):
        act = TCXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_1002.tcx")
        assert all(tp.lat is None and tp.lon is None for tp in act.track_points)
        assert act.distance_by_coord_km == 0.0

    def test_should_raise_parse_error_when_xml_malformed(self, fixtures_dir):
        with pytest.raises(ET.ParseError):
            TCXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_1003.tcx")

    def test_should_raise_activity_skipped_when_activity_predates_challenge_year(
        self, fixtures_dir
    ):
        with pytest.raises(ActivitySkipped, match="old activity"):
            TCXParser(CHALLENGE_YEAR + 1).parse(fixtures_dir / "activity_1001.tcx")


class TestGPXParser:
    def test_golden_file_yields_track_statistics(self, fixtures_dir):
        act = GPXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_2001.gpx")
        assert act.activity_id == "2001"
        assert act.date == "2021-01-08 12:04:52"
        assert act.total_sec == 600.0
        assert act.distance_km == 0.0
        assert act.distance_by_coord_km > 0
        assert act.avg_temp == 22.0
        assert act.min_elevation == 329.8
        assert act.max_elevation == 331.0
        assert act.avg_cadence == 148.0

    def test_should_keep_activity_when_elevation_and_extensions_missing(self, fixtures_dir):
        act = GPXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_2002.gpx")
        assert act.activity_id == "2002"
        assert len(act.track_points) == 2
        second = act.track_points[1]
        assert second.elevation is None
        assert second.temperature is None
        assert second.cadence is None
        assert act.avg_temp == 21.0

    def test_should_raise_parse_error_when_xml_malformed(self, fixtures_dir):
        with pytest.raises(ET.ParseError):
            GPXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_2003.gpx")


class TestKMLParser:
    def test_golden_file_yields_lap_totals_and_track(self, fixtures_dir):
        act = KMLParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_3001.kml")
        assert act.activity_id == "3001"
        assert act.date == "2021-01-08 12:04:52"
        assert act.total_sec == 600.0
        assert act.distance_km == 2.0
        assert len(act.track_points) == 2
        assert act.track_points[0].lat == 22.3278
        assert act.track_points[0].lon == 114.2019

    def test_should_keep_activity_when_lap_table_and_point_incomplete(self, fixtures_dir):
        act = KMLParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_3002.kml")
        assert act.activity_id == "3002"
        assert act.total_sec == 600.0
        assert act.distance_km == 2.0
        assert len(act.track_points) == 1

    def test_should_raise_parse_error_when_xml_malformed(self, fixtures_dir):
        with pytest.raises(ET.ParseError):
            KMLParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_3003.kml")

    def test_should_raise_parse_error_when_no_track_points(self, fixtures_dir):
        # W-004: an indoor run carries lap data but no timestamp of any kind,
        # so it is a data problem to report, not a deliberate year filter.
        with pytest.raises(ActivityParseError, match="no track points"):
            KMLParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_3004.kml")


class TestParseAllDoesNotSilentlyDrop:
    def test_should_keep_activity_missing_optional_tag(self, fixtures_dir):
        ids = [a.activity_id for a in TCXParser(CHALLENGE_YEAR).parse_all(fixtures_dir)]
        assert ids == ["1001", "1002", "1004"]

    def test_should_keep_gpx_activity_missing_optional_tag(self, fixtures_dir):
        ids = [a.activity_id for a in GPXParser(CHALLENGE_YEAR).parse_all(fixtures_dir)]
        assert ids == ["2001", "2002"]

    def test_should_keep_kml_activity_missing_optional_tag(self, fixtures_dir):
        ids = [a.activity_id for a in KMLParser(CHALLENGE_YEAR).parse_all(fixtures_dir)]
        assert ids == ["3001", "3002"]

    def test_should_log_warning_naming_the_skipped_file(self, fixtures_dir, caplog):
        with caplog.at_level(logging.WARNING, logger="run365days.activities.parsers.base"):
            TCXParser(CHALLENGE_YEAR).parse_all(fixtures_dir)
        assert any("activity_1003.tcx" in record.getMessage() for record in caplog.records)


class TestParseAllLogLevels:
    """W-004: the log level must follow the exception type, not the other way round."""

    @staticmethod
    def _records_for(caplog, name: str) -> list[logging.LogRecord]:
        return [r for r in caplog.records if name in r.getMessage()]

    def test_should_log_debug_when_activity_deliberately_skipped(self, fixtures_dir, caplog):
        with caplog.at_level(logging.DEBUG, logger="run365days.activities.parsers.base"):
            TCXParser(CHALLENGE_YEAR + 1).parse_all(fixtures_dir)
        records = self._records_for(caplog, "activity_1001.tcx")
        assert records
        assert all(r.levelno == logging.DEBUG for r in records)

    def test_should_log_warning_when_kml_has_no_track_points(self, fixtures_dir, caplog):
        with caplog.at_level(logging.WARNING, logger="run365days.activities.parsers.base"):
            KMLParser(CHALLENGE_YEAR).parse_all(fixtures_dir)
        records = self._records_for(caplog, "activity_3004.kml")
        assert records
        assert all(r.levelno == logging.WARNING for r in records)
        assert "no track points" in records[0].getMessage()

    def test_should_log_warning_when_a_generic_value_error_escapes_a_parser(
        self, fixtures_dir, caplog, monkeypatch
    ):
        # A bad float() anywhere in a parser is a data problem, not a skip, so it
        # must stay visible at the default log level.
        parser = TCXParser(CHALLENGE_YEAR)
        monkeypatch.setattr(
            parser, "parse", lambda fp: (_ for _ in ()).throw(ValueError("could not convert"))
        )
        with caplog.at_level(logging.DEBUG, logger="run365days.activities.parsers.base"):
            assert parser.parse_all(fixtures_dir) == []
        assert caplog.records
        assert all(r.levelno == logging.WARNING for r in caplog.records)

    def test_should_raise_activity_skipped_when_gpx_is_not_running(self, fixtures_dir):
        with pytest.raises(ActivitySkipped, match="Not a running activity"):
            GPXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_2004.gpx")

    def test_should_raise_activity_skipped_when_kml_is_not_running(self, fixtures_dir):
        with pytest.raises(ActivitySkipped, match="Not a running activity"):
            KMLParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_3005.kml")


class TestExportDataZeroRecordGuard:
    def test_should_exit_non_zero_when_no_activity_parsed(self, tmp_path, monkeypatch):
        from run365days.cli import export_data

        empty_tcx = tmp_path / "tcx"
        empty_tcx.mkdir()
        monkeypatch.setattr(
            "sys.argv",
            [
                "run365-export",
                "--tcx-dir",
                str(empty_tcx),
                "--gpx-dir",
                str(tmp_path / "gpx"),
                "--db",
                str(tmp_path / "run365.db"),
                "--static-dir",
                str(tmp_path / "static"),
            ],
        )

        with pytest.raises(SystemExit) as exc:
            export_data.main()

        assert exc.value.code != 0
        assert not (tmp_path / "static").exists()


class TestGuardedHelpers:
    """W-005: nan / inf are valid floats but not valid ints, and must not escape."""

    @staticmethod
    def _element(text: str) -> ET.Element:
        parent = ET.Element("parent")
        child = ET.SubElement(parent, "value")
        child.text = text
        return parent

    @pytest.mark.parametrize("text", ["nan", "inf", "-inf", "Infinity", "NaN"])
    def test_should_return_none_when_optional_int_is_not_finite(self, text):
        assert optional_int(self._element(text), "value", {}) is None

    @pytest.mark.parametrize("text", ["nan", "inf", "-inf"])
    def test_should_return_none_when_optional_float_is_not_finite(self, text):
        assert optional_float(self._element(text), "value", {}) is None

    def test_should_return_truncated_int_when_value_is_finite(self):
        assert optional_int(self._element("12.7"), "value", {}) == 12

    def test_should_return_none_when_text_is_not_a_number(self):
        assert optional_float(self._element("abc"), "value", {}) is None
        assert optional_int(self._element("abc"), "value", {}) is None

    def test_should_return_none_when_element_absent(self):
        assert optional_float(ET.Element("parent"), "value", {}) is None
        assert optional_int(ET.Element("parent"), "value", {}) is None

    def test_should_not_crash_parse_all_when_a_reading_is_infinite(self, fixtures_dir):
        # int(float("inf")) raises OverflowError, which parse_all does not catch:
        # one corrupt file used to take the whole export down with it.
        ids = [a.activity_id for a in TCXParser(CHALLENGE_YEAR).parse_all(fixtures_dir)]
        assert "1004" in ids

    def test_should_drop_non_finite_readings_from_the_parsed_activity(self, fixtures_dir):
        act = TCXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_1004.tcx")
        assert act.calories == 0  # "nan" Calories reads as absent, not as poison
        assert all(tp.cadence is None for tp in act.track_points)
        assert act.track_points[0].speed is None
        assert act.track_points[0].elevation is None


class TestKMLLapTotalsAreMandatory:
    """W-006: Time and Distance feed the headline numbers, so KML matches TCX."""

    def test_should_raise_parse_error_when_a_lap_omits_distance(self, fixtures_dir):
        with pytest.raises(ActivityParseError, match="lap 2"):
            KMLParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_3006.kml")

    def test_should_raise_parse_error_when_a_lap_omits_time(self, fixtures_dir):
        with pytest.raises(ActivityParseError, match="lap 2"):
            KMLParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_3007.kml")

    def test_should_log_warning_rather_than_undercount_the_totals(self, fixtures_dir, caplog):
        with caplog.at_level(logging.WARNING, logger="run365days.activities.parsers.base"):
            KMLParser(CHALLENGE_YEAR).parse_all(fixtures_dir)
        messages = [r.getMessage() for r in caplog.records if "activity_3006.kml" in r.getMessage()]
        assert messages
        assert "Distance" in messages[0]

    def test_should_match_tcx_which_treats_the_same_pair_as_mandatory(self, fixtures_dir):
        # TCX reads TotalTimeSeconds / DistanceMeters through required_text; the
        # KML lap table's Time / Distance are the same two numbers.
        with pytest.raises(ActivityParseError):
            TCXParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_1005.tcx")
        with pytest.raises(ActivityParseError):
            KMLParser(CHALLENGE_YEAR).parse(fixtures_dir / "activity_3006.kml")
