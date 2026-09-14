"""Collector tests (CUI-0010) plus the AU-014 timeout and AU-013 logging guards.

Every test drives the collectors through a fake ``requests.get``. The autouse
``block_real_sockets`` fixture is the proof that none of them reaches the real
network: it severs ``socket.socket.connect`` for the whole module, so a test
that forgot to install a fake would raise instead of quietly scraping HKO.
"""

import json
import logging
import socket
from pathlib import Path

import pytest
import requests
from bs4 import BeautifulSoup

from run365days.cli.collect_weather import _write_jsonl
from run365days.common import config
from run365days.dashboard.builder import hourly_at, load_jsonl, warnings_by_date
from run365days.export.records import daily_weather_record, warning_record
from run365days.weather.collectors import hko_daily, hourly, warnings
from run365days.weather.collectors.html_reads import child_attr, child_string

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "weather"

# A collector loop is only as bounded as its slowest request: fetch_year() issues
# up to 13 and hourly.fetch_range() one per day, so anything looser than these
# ceilings lets a single stalled socket dominate a whole run.
MAX_ACCEPTABLE_CONNECT_SEC = 10
MAX_ACCEPTABLE_READ_SEC = 60

HKO_YEAR_URL = "https://www.weather.gov.hk/cis/dailyExtract/dailyExtract_2021.xml"
HKO_FEB_URL = "https://www.weather.gov.hk/cis/dailyExtract/dailyExtract_202102.xml"
HKO_MAR_URL = "https://www.weather.gov.hk/cis/dailyExtract/dailyExtract_202103.xml"

HKO_DAILY_LOGGER = "run365days.weather.collectors.hko_daily"
HOURLY_LOGGER = "run365days.weather.collectors.hourly"
WARNINGS_LOGGER = "run365days.weather.collectors.warnings"


def read_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def block_real_sockets(monkeypatch):
    """Make any unfaked outbound connection fail loudly instead of scraping."""

    def refuse(*args, **kwargs):
        raise AssertionError("a test tried to open a real network connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)


class FakeResponse:
    def __init__(self, text: str):
        self.text = text


class RequestRecorder:
    """A ``requests.get`` stand-in that answers from a URL -> body/exception map."""

    def __init__(self, routes: dict):
        self.routes = routes
        self.calls: list[dict] = []

    def __call__(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        answer = self.routes[url]
        if isinstance(answer, Exception):
            raise answer
        return FakeResponse(answer)

    @property
    def timeouts(self) -> list:
        return [call["timeout"] for call in self.calls]


def assert_bounded_timeout(timeout) -> None:
    """Assert *timeout* splits into a fast connect phase and a bounded read."""
    assert timeout is not None, "requests.get was called without a timeout"
    connect, read = timeout
    assert 0 < connect <= MAX_ACCEPTABLE_CONNECT_SEC
    assert 0 < read <= MAX_ACCEPTABLE_READ_SEC


def install_fake_get(monkeypatch, module, routes: dict) -> RequestRecorder:
    recorder = RequestRecorder(routes)
    monkeypatch.setattr(module.requests, "get", recorder)
    return recorder


def hko_routes(*, feb=None, mar=None) -> dict:
    year_body = read_fixture("hko_daily_year.json")
    month_body = read_fixture("hko_daily_month_02.json")
    return {
        HKO_YEAR_URL: year_body,
        HKO_FEB_URL: month_body if feb is None else feb,
        HKO_MAR_URL: "" if mar is None else mar,
    }


class TestGuardedHtmlReads:
    """The two guards CUI-0012 turned three chained ``.find()`` reads into.

    ``fetch_day`` only ever reaches the "element absent" branch, so the two
    "element present but unreadable" branches are pinned here directly -- both
    are real BeautifulSoup shapes, and both used to raise.
    """

    @staticmethod
    def cell(markup: str):
        return BeautifulSoup(markup, "html.parser")

    def test_child_string_returns_none_when_the_tag_is_absent(self):
        assert child_string(self.cell("<td>&nbsp;</td>"), "script") is None

    def test_child_string_returns_none_when_the_tag_is_present_but_empty(self):
        # An emitted-but-empty <script> is the second way this read used to
        # break: ``find()`` hands back a Tag, and ``.string`` on it is None.
        assert child_string(self.cell("<td><script></script></td>"), "script") is None

    def test_child_string_returns_the_tags_only_string(self):
        assert child_string(self.cell("<td><script>icon(7)</script></td>"), "script") == "icon(7)"

    def test_child_attr_returns_none_when_the_tag_is_absent(self):
        assert child_attr(self.cell("<td>&nbsp;</td>"), "img", "src") is None

    def test_child_attr_returns_none_when_the_tag_lacks_the_attribute(self):
        assert child_attr(self.cell('<td><img alt="cold" /></td>'), "img", "src") is None

    def test_child_attr_returns_the_attribute_value(self):
        cell = self.cell('<td><img src="/images_e/cold.gif" /></td>')

        assert child_attr(cell, "img", "src") == "/images_e/cold.gif"


class TestHkoDailyFetchYear:
    def test_parses_every_numeric_row_of_the_yearly_payload(self, monkeypatch):
        install_fake_get(monkeypatch, hko_daily, hko_routes())

        records = hko_daily.fetch_year("2021")

        january = [r for r in records if r.date.startswith("2021-01")]
        assert [r.date for r in january] == ["2021-01-01", "2021-01-02", "2021-01-03"]
        assert january[0].max_temp_c == 15.0
        assert january[0].mean_temp_c == 11.8
        assert january[0].min_temp_c == 8.6
        assert january[0].mean_humidity_pct == 40.0
        assert january[0].total_rainfall_mm == 0.0
        assert january[0].mean_wind_kmh == 25.6

    def test_skips_the_summary_row_whose_first_cell_is_not_a_day_number(self, monkeypatch):
        install_fake_get(monkeypatch, hko_daily, hko_routes())

        records = hko_daily.fetch_year("2021")

        assert all("Mean" not in r.date for r in records)
        assert len([r for r in records if r.date.startswith("2021-01")]) == 3

    def test_falls_back_to_the_per_month_endpoint_when_daydata_is_empty(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, hko_daily, hko_routes())

        records = hko_daily.fetch_year("2021")

        february = [r for r in records if r.date.startswith("2021-02")]
        assert [r.date for r in february] == ["2021-02-01", "2021-02-02"]
        assert february[0].max_temp_c == 25.1
        assert HKO_FEB_URL in [call["url"] for call in recorder.calls]

    def test_skips_the_month_when_the_fallback_payload_is_not_valid_json(self, monkeypatch):
        install_fake_get(monkeypatch, hko_daily, hko_routes(mar="<html>503 Service Unavailable"))

        records = hko_daily.fetch_year("2021")

        assert not [r for r in records if r.date.startswith("2021-03")]
        assert [r.date for r in records if r.date.startswith("2021-02")]

    def test_logs_the_month_and_the_reason_when_a_month_is_skipped(self, monkeypatch, caplog):
        install_fake_get(monkeypatch, hko_daily, hko_routes(mar="<html>503 Service Unavailable"))

        with caplog.at_level(logging.WARNING, logger=HKO_DAILY_LOGGER):
            hko_daily.fetch_year("2021")

        skipped = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(skipped) == 1
        message = skipped[0].getMessage()
        assert "2021" in message
        assert "03" in message
        assert "JSONDecodeError" in message

    def test_skips_the_month_when_the_fallback_payload_has_no_day_data(self, monkeypatch, caplog):
        install_fake_get(monkeypatch, hko_daily, hko_routes(mar=json.dumps({"stn": {"data": []}})))

        with caplog.at_level(logging.WARNING, logger=HKO_DAILY_LOGGER):
            records = hko_daily.fetch_year("2021")

        assert not [r for r in records if r.date.startswith("2021-03")]
        assert "IndexError" in caplog.records[0].getMessage()

    def test_every_request_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, hko_daily, hko_routes())

        hko_daily.fetch_year("2021")

        assert recorder.timeouts
        for timeout in recorder.timeouts:
            assert_bounded_timeout(timeout)

    def test_an_unreachable_host_is_not_reported_as_a_missing_month(self, monkeypatch):
        routes = hko_routes(feb=requests.ConnectionError("Name or service not known"))
        install_fake_get(monkeypatch, hko_daily, routes)

        with pytest.raises(requests.ConnectionError):
            hko_daily.fetch_year("2021")

    def test_a_stalled_endpoint_is_not_reported_as_a_missing_month(self, monkeypatch):
        routes = hko_routes(feb=requests.Timeout("read timed out"))
        install_fake_get(monkeypatch, hko_daily, routes)

        with pytest.raises(requests.Timeout):
            hko_daily.fetch_year("2021")


class TestHourlyFetchDay:
    def test_parses_each_observation_row(self, monkeypatch):
        install_fake_get(monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert [r.time for r in records] == ["00:00", "00:30", "01:00"]
        assert records[0].date == "2021-01-01"
        assert records[0].temperature_c == 11.0
        assert records[0].wind_kmh == 24.0
        assert records[0].humidity_pct == 24.0
        assert records[0].description == "Clear weather"

    def test_reads_the_wind_speed_from_the_bearing_form(self, monkeypatch):
        # "Northeast 50° 24 Km/h" -- the form that names a compass bearing.
        install_fake_get(monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert records[0].wind_kmh == 24.0

    def test_reads_the_wind_speed_from_the_variable_direction_form(self, monkeypatch):
        # Rewritten for CUI-0018. This test used to pass for the wrong reason:
        # "Variable at 20 Km/h" carries no degree sign, find("°") answered -1,
        # and the slice parsed only because -1 + 1 started it at the front. That
        # made the sentinel load-bearing, so any guard added to find() would
        # have broken this form while the test named nothing about it. The
        # assertion is now the speed the page states, for each form in its own
        # test, so the two forms stay pinned however the cell is read.
        install_fake_get(monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert records[1].wind_kmh == 20.0

    def test_maps_a_wind_cell_matching_neither_form_to_none(self, monkeypatch):
        # The fixture's first cell reads "Variable at 20 mph". The old read
        # stripped the prefix, then dropped five characters for "Km/h" -- which
        # ate one digit too many off "20 mph" and reported 2.0 Km/h. A speed
        # nobody wrote must not be reported as one.
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_unreadable_wind.html")}
        )

        records = hourly.fetch_day("2021-01-01")

        assert records[0].wind_kmh is None
        assert records[1].wind_kmh == 22.0

    def test_logs_a_warning_when_the_wind_cell_matches_neither_form(self, monkeypatch, caplog):
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_unreadable_wind.html")}
        )

        with caplog.at_level(logging.WARNING, logger=HOURLY_LOGGER):
            hourly.fetch_day("2021-01-01")

        unreadable = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(unreadable) == 1
        message = unreadable[0].getMessage()
        assert "2021-01-01" in message
        assert "00:00" in message

    def test_keeps_the_rest_of_a_row_whose_wind_cell_matches_neither_form(self, monkeypatch):
        # The page arrived and every other cell is readable, so the observation
        # is real data; only the one column is a gap. Dropping the row would
        # cost the hour its temperature, humidity and description as well.
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_unreadable_wind.html")}
        )

        records = hourly.fetch_day("2021-01-01")

        assert [r.time for r in records] == ["00:00", "00:30"]
        assert records[0].temperature_c == 11.0
        assert records[0].humidity_pct == 24.0
        assert records[0].description == "Clear weather"

    def test_maps_an_unreadable_temperature_to_none(self, monkeypatch):
        install_fake_get(monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert records[2].temperature_c is None

    def test_maps_an_unknown_description_code_to_unknown(self, monkeypatch):
        install_fake_get(monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert records[2].description == "Unknown"

    def test_skips_rows_whose_cell_count_does_not_match_the_header(self, monkeypatch):
        install_fake_get(monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert "Daily summary" not in [r.time for r in records]
        assert len(records) == 3

    def test_returns_empty_list_when_the_page_has_no_history_table(self, monkeypatch):
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_no_table.html")}
        )

        assert hourly.fetch_day("2021-01-01") == []

    def test_queries_the_requested_date(self, monkeypatch):
        recorder = install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")}
        )

        hourly.fetch_day("2021-01-01")

        assert recorder.calls[0]["params"]["date"] == "2021-01-01"

    def test_request_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")}
        )

        hourly.fetch_day("2021-01-01")

        assert_bounded_timeout(recorder.calls[0]["timeout"])

    def test_skips_a_row_without_a_weather_script_and_keeps_the_others(self, monkeypatch):
        # Replaces the CUI-0010 pinning test that nailed the AttributeError from
        # the unguarded ``tds[9].find("script").string``. One malformed cell used
        # to take the whole day's data down with it (CUI-0012).
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_no_script.html")}
        )

        records = hourly.fetch_day("2021-01-01")

        assert [r.time for r in records] == ["00:30"]
        assert records[0].description == "Cloudy skies"

    def test_logs_the_row_it_dropped_for_a_missing_weather_script(self, monkeypatch, caplog):
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_no_script.html")}
        )

        with caplog.at_level(logging.WARNING, logger=HOURLY_LOGGER):
            hourly.fetch_day("2021-01-01")

        dropped = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(dropped) == 1
        message = dropped[0].getMessage()
        assert "2021-01-01" in message
        assert "00:00" in message

    def test_reports_unknown_when_the_script_carries_no_icon_call(self, monkeypatch):
        # The fixture's first script has lost its writeWeatherIcon( name, so
        # ``find("n(")`` answered -1. -1 + 2 = 1 is a legal index, so the slice
        # ran from character 1 and handed back "7" -- a code that maps to "Rain".
        # A description nobody read out of the page must not be reported as one.
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_unreadable_script.html")}
        )

        records = hourly.fetch_day("2021-01-01")

        assert records[0].description == "Unknown"
        assert records[1].description == "Cloudy skies"

    def test_logs_a_warning_when_the_script_carries_no_icon_call(self, monkeypatch, caplog):
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_unreadable_script.html")}
        )

        with caplog.at_level(logging.WARNING, logger=HOURLY_LOGGER):
            hourly.fetch_day("2021-01-01")

        unreadable = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(unreadable) == 1
        message = unreadable[0].getMessage()
        assert "2021-01-01" in message
        assert "00:00" in message

    def test_keeps_the_rest_of_a_row_whose_script_carries_no_icon_call(self, monkeypatch):
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_unreadable_script.html")}
        )

        records = hourly.fetch_day("2021-01-01")

        assert [r.time for r in records] == ["00:00", "00:30"]
        assert records[0].temperature_c == 11.0
        assert records[0].wind_kmh == 24.0


class TestHourlyFetchRange:
    def test_fetches_one_page_per_day_in_the_inclusive_range(self, monkeypatch):
        recorder = install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")}
        )

        records = hourly.fetch_range("2021-01-01", "2021-01-03")

        assert [call["params"]["date"] for call in recorder.calls] == [
            "2021-01-01",
            "2021-01-02",
            "2021-01-03",
        ]
        assert len(records) == 9

    def test_every_request_in_the_range_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")}
        )

        hourly.fetch_range("2021-01-01", "2021-01-02")

        for timeout in recorder.timeouts:
            assert_bounded_timeout(timeout)


def warning_routes(
    day_fixture: str = "hko_warning_day.html",
    *,
    legend_fixture: str = "hko_warning_legend.html",
) -> dict:
    return {
        warnings._SIGNALS_URL: read_fixture(legend_fixture),
        warnings._HISTORY_URL: read_fixture(day_fixture),
    }


class TestWarningSignalMetadata:
    def test_maps_each_icon_alt_to_its_index_and_warning_type(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes())

        meta = warnings._load_signal_metadata()

        assert meta["Red Fire Danger Warning"] == {"Idx": "firer", "Type": "Fire Danger Warnings"}
        assert meta["Yellow Fire Danger Warning"]["Type"] == "Fire Danger Warnings"
        assert meta["Standby Signal No.1"]["Idx"] == "tc1"

    def test_request_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warnings, warning_routes())

        warnings._load_signal_metadata()

        assert_bounded_timeout(recorder.calls[0]["timeout"])

    def test_reads_the_icon_index_from_a_src_without_a_file_extension(self, monkeypatch):
        # ``rfind(".")`` answered -1 on an extensionless src, and src[start:-1]
        # is a legal slice, so the index quietly lost its last character.
        routes = warning_routes(legend_fixture="hko_warning_legend_no_extension.html")
        install_fake_get(monkeypatch, warnings, routes)

        meta = warnings._load_signal_metadata()

        assert meta["Cold Weather Warning"]["Idx"] == "cold"
        assert meta["Frost Warning"]["Idx"] == "frost"
        assert meta["Standby Signal No.1"]["Idx"] == "tc1"

    def test_skips_a_legend_table_that_does_not_carry_both_cells(self, monkeypatch):
        routes = warning_routes(legend_fixture="hko_warning_legend_short_row.html")
        install_fake_get(monkeypatch, warnings, routes)

        meta = warnings._load_signal_metadata()

        assert meta == {"Cold Weather Warning": {"Idx": "cold", "Type": "Cold Weather Warning"}}

    def test_logs_a_warning_for_each_legend_table_it_skipped(self, monkeypatch, caplog):
        routes = warning_routes(legend_fixture="hko_warning_legend_short_row.html")
        install_fake_get(monkeypatch, warnings, routes)

        with caplog.at_level(logging.WARNING, logger=WARNINGS_LOGGER):
            warnings._load_signal_metadata()

        skipped = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(skipped) == 2


class TestWarningsFetchDay:
    def test_parses_every_six_cell_warning_row(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes())
        meta = warnings._load_signal_metadata()

        records = warnings.fetch_day("2021-01-01", meta)

        assert [r.warning_signal for r in records] == [
            "RED FIRE DANGER WARNING",
            "COLD WEATHER WARNING",
            "FROST WARNING",
        ]
        assert records[0].date == "2021-01-01"
        assert records[0].warning_type == "Fire Danger Warnings"
        assert records[0].start_time == "2020-12-30 06:00:00"
        assert records[0].end_time == "2021-01-02 20:00:00"
        assert records[0].icon_url == "/images_e/firer.gif"

    def test_ignores_rows_before_the_tropical_cyclone_marker(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes())
        meta = warnings._load_signal_metadata()

        records = warnings.fetch_day("2021-01-01", meta)

        assert "SIGNAL BEFORE THE MARKER" not in [r.warning_signal for r in records]

    def test_skips_rows_that_do_not_have_six_cells(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes())
        meta = warnings._load_signal_metadata()

        records = warnings.fetch_day("2021-01-01", meta)

        assert len(records) == 3

    def test_maps_a_signal_missing_from_the_legend_to_unknown(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes())
        meta = warnings._load_signal_metadata()

        records = warnings.fetch_day("2021-01-01", meta)

        frost = next(r for r in records if r.warning_signal == "FROST WARNING")
        assert frost.warning_type == "Unknown"

    def test_returns_empty_list_when_no_warning_was_in_force(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes("hko_warning_day_empty.html"))
        meta = warnings._load_signal_metadata()

        assert warnings.fetch_day("2021-01-01", meta) == []

    def test_returns_no_records_when_the_tropical_cyclone_marker_is_absent(self, monkeypatch):
        # ``str.find`` answers -1, and -1 + len(marker) used to land the slice on
        # character 31 of the whole page -- close enough to parse, so a renamed
        # HKO heading quietly yielded rows scraped from the wrong table (CUI-0012).
        install_fake_get(monkeypatch, warnings, warning_routes("hko_warning_no_marker.html"))

        assert warnings.fetch_day("2021-01-01", {}) == []

    def test_logs_a_warning_when_the_tropical_cyclone_marker_is_absent(self, monkeypatch, caplog):
        install_fake_get(monkeypatch, warnings, warning_routes("hko_warning_no_marker.html"))

        with caplog.at_level(logging.WARNING, logger=WARNINGS_LOGGER):
            warnings.fetch_day("2021-01-01", {})

        skipped = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(skipped) == 1
        assert "2021-01-01" in skipped[0].getMessage()

    def test_keeps_a_warning_row_whose_signal_icon_is_missing(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes("hko_warning_no_icon.html"))

        records = warnings.fetch_day("2021-01-01", {})

        assert [r.warning_signal for r in records] == ["COLD WEATHER WARNING"]
        assert records[0].start_time == "2020-12-29 16:20:00"
        assert records[0].icon_url == ""

    def test_logs_a_warning_for_a_row_whose_signal_icon_is_missing(self, monkeypatch, caplog):
        install_fake_get(monkeypatch, warnings, warning_routes("hko_warning_no_icon.html"))

        with caplog.at_level(logging.WARNING, logger=WARNINGS_LOGGER):
            warnings.fetch_day("2021-01-01", {})

        iconless = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(iconless) == 1
        assert "COLD WEATHER WARNING" in iconless[0].getMessage()

    def test_queries_the_requested_day(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warnings, warning_routes())

        warnings.fetch_day("2021-01-01", {})

        assert recorder.calls[0]["params"] == {"start_ym": "20210101"}

    def test_request_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warnings, warning_routes())

        warnings.fetch_day("2021-01-01", {})

        assert_bounded_timeout(recorder.calls[0]["timeout"])


class TestWarningsFetchRange:
    def test_loads_the_signal_legend_once_for_the_whole_range(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warnings, warning_routes())

        warnings.fetch_range("2021-01-01", "2021-01-03")

        legend_calls = [c for c in recorder.calls if c["url"] == warnings._SIGNALS_URL]
        history_calls = [c for c in recorder.calls if c["url"] == warnings._HISTORY_URL]
        assert len(legend_calls) == 1
        assert len(history_calls) == 3

    def test_returns_every_days_warnings(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes())

        records = warnings.fetch_range("2021-01-01", "2021-01-02")

        assert [r.date for r in records] == ["2021-01-01"] * 3 + ["2021-01-02"] * 3

    def test_every_request_in_the_range_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warnings, warning_routes())

        warnings.fetch_range("2021-01-01", "2021-01-02")

        for timeout in recorder.timeouts:
            assert_bounded_timeout(timeout)


class TestCollectorTimeoutsHaveOneOwner:
    """The timeout pair is one value, not three copies that can drift apart.

    ``assert_bounded_timeout`` above states the *requirement* -- a bounded pair,
    a fast connect, a capped read -- and deliberately says nothing about the
    numbers. These tests state the other half: whatever the numbers are, all
    three collectors read them from the same place, so a future change to the
    ceiling cannot be applied to two scrapers and forgotten in the third.
    """

    def test_config_owns_the_pair_the_collectors_send(self):
        assert config.HTTP_TIMEOUT == (
            config.HTTP_CONNECT_TIMEOUT_SEC,
            config.HTTP_READ_TIMEOUT_SEC,
        )
        assert_bounded_timeout(config.HTTP_TIMEOUT)

    def test_hko_daily_sends_the_configured_pair(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, hko_daily, hko_routes())

        hko_daily.fetch_year("2021")

        assert recorder.timeouts
        # Identity, not equality: an equal tuple built locally would still be a
        # second copy of the numbers, which is the thing being removed.
        assert all(timeout is config.HTTP_TIMEOUT for timeout in recorder.timeouts)

    def test_hourly_sends_the_configured_pair(self, monkeypatch):
        recorder = install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")}
        )

        hourly.fetch_day("2021-01-01")

        assert recorder.timeouts
        assert all(timeout is config.HTTP_TIMEOUT for timeout in recorder.timeouts)

    def test_warnings_sends_the_configured_pair(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warnings, warning_routes())

        warnings.fetch_range("2021-01-01", "2021-01-02")

        assert recorder.timeouts
        assert all(timeout is config.HTTP_TIMEOUT for timeout in recorder.timeouts)


class TestCollectThenExport:
    """The full path the bug hid behind: collect -> _write_jsonl -> read -> export.

    Nobody had ever driven a freshly collected file back through the export
    layer, so the collectors and the readers were free to drift apart. Each test
    below writes with the collector's own writer and reads with the exporter's
    own reader, which is the only pairing that can catch it (CUI-0011).
    """

    def test_collected_daily_rows_export_with_every_reading_intact(self, monkeypatch, tmp_path):
        install_fake_get(monkeypatch, hko_daily, hko_routes())
        collected = hko_daily.fetch_year("2021")
        path = tmp_path / "hko_daily_weather_extract.json"

        _write_jsonl(collected, path)
        exported = [daily_weather_record(row) for row in load_jsonl(path)]

        assert [r["date"] for r in exported] == [r.date for r in collected]
        assert [r["max_temp_c"] for r in exported] == [r.max_temp_c for r in collected]
        assert [r["avg_temp_c"] for r in exported] == [r.mean_temp_c for r in collected]
        assert [r["min_temp_c"] for r in exported] == [r.min_temp_c for r in collected]
        assert [r["humidity_pct"] for r in exported] == [r.mean_humidity_pct for r in collected]
        assert [r["rainfall_mm"] for r in exported] == [r.total_rainfall_mm for r in collected]
        assert [r["wind_kmh"] for r in exported] == [r.mean_wind_kmh for r in collected]

    def test_collected_daily_rows_export_without_an_all_none_reading(self, monkeypatch, tmp_path):
        install_fake_get(monkeypatch, hko_daily, hko_routes())
        path = tmp_path / "hko_daily_weather_extract.json"

        _write_jsonl(hko_daily.fetch_year("2021"), path)
        first = [daily_weather_record(row) for row in load_jsonl(path)][0]

        readings = ("max_temp_c", "avg_temp_c", "min_temp_c", "humidity_pct", "wind_kmh")
        assert all(first[field] is not None for field in readings)

    def test_collected_hourly_rows_export_into_an_activity_weather_block(
        self, monkeypatch, tmp_path
    ):
        install_fake_get(monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")})
        collected = hourly.fetch_day("2021-01-08")
        path = tmp_path / "weather_history.json"

        _write_jsonl(collected, path)
        observed = hourly_at(load_jsonl(path), "2021-01-08", collected[0].time)

        assert observed == {
            "desc": collected[0].description,
            "temp": collected[0].temperature_c,
            "hum": collected[0].humidity_pct,
            "wind": collected[0].wind_kmh,
        }

    def test_collected_warnings_keep_their_signals_through_the_export(self, monkeypatch, tmp_path):
        install_fake_get(monkeypatch, warnings, warning_routes())
        collected = warnings.fetch_day("2021-01-01", {})
        path = tmp_path / "weather_warning_history.json"

        _write_jsonl(collected, path)
        rows = load_jsonl(path)

        assert warnings_by_date(rows) == {
            "2021-01-01": list(dict.fromkeys(r.warning_signal for r in collected))
        }
        assert warning_record(rows[0]) == {
            "date": collected[0].date,
            "type": collected[0].warning_type,
            "signal": collected[0].warning_signal,
            "start_time": collected[0].start_time,
            "end_time": collected[0].end_time,
        }
