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
from run365days.dashboard.builder import hourly_at, load_jsonl, warnings_by_date
from run365days.export.records import daily_weather_record, warning_record
from run365days.weather.collectors import hko_daily, hourly, warnings
from run365days.weather.collectors._parsing import WeatherPageStructureError

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

    def test_reads_the_wind_speed_from_the_variable_direction_form(self, monkeypatch):
        install_fake_get(monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert records[1].wind_kmh == 20.0
        assert records[1].description == "Cloudy skies"

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

    def test_the_column_map_matches_the_header_row_it_names(self):
        # Binds every column constant to the header text it claims to point at,
        # so renumbering one without moving it fails here rather than silently
        # reading humidity as a wind speed.
        soup = BeautifulSoup(read_fixture("freemeteo_day.html"), "html.parser")
        history = soup.find_all("table", {"class": "daily-history"})[0]
        headers = [th.text.strip() for th in history.find_all("th")]

        assert headers[hourly._TIME_COL] == "Time"
        assert headers[hourly._TEMPERATURE_COL] == "Temperature"
        assert headers[hourly._WIND_COL] == "Wind"
        assert headers[hourly._HUMIDITY_COL] == "Humidity"
        assert headers[hourly._WEATHER_COL] == "Weather"

    def test_a_row_without_a_weather_script_keeps_the_row(self, monkeypatch):
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_no_script.html")}
        )

        records = hourly.fetch_day("2021-01-01")

        assert [r.time for r in records] == ["00:00"]
        assert records[0].description == "Unknown"
        assert records[0].temperature_c == 11.0

    def test_a_script_that_is_not_the_documented_call_reads_unknown(self, monkeypatch):
        # The other half of the icon guard, and the half a missing-script test
        # cannot reach: this cell *has* a script, it just is not the documented
        # ``writeWeatherIcon(<code>, 'CurrentWeather', ...)`` call. Nothing says
        # drawIcon's argument is a current-weather code -- it could as easily be
        # tomorrow's forecast icon -- so the honest answer is Unknown.
        install_fake_get(
            monkeypatch, hourly, {hourly._URL: read_fixture("freemeteo_unexpected_script.html")}
        )

        records = hourly.fetch_day("2021-01-01")

        assert [r.description for r in records] == ["Unknown"]
        assert records[0].temperature_c == 11.0

    def test_the_old_icon_arithmetic_would_have_named_a_description(self):
        # Pins what the guard prevents. Both str.find() calls below can return
        # -1, and on this script the second one does; the old slice therefore
        # read s[start:-1], which lands exactly on "7" and named it Rain with
        # no evidence whatsoever. Without this the guard could be deleted and
        # every other test would stay green.
        script = "drawIcon(7)"
        old_reading = script[script.find("n(") + 2 : script.find(", 'CurrentWeather")]

        assert old_reading == "7"
        assert hourly._DESCRIPTION_MAP[old_reading] == "Rain"
        assert hourly._icon_code(script) is None

    def test_a_script_missing_the_call_prefix_is_not_read_as_a_code(self):
        # The mirror image of the test above, and the half of the guard nothing
        # held. There the *suffix* is missing; here the prefix is, so both
        # find() calls return -1 -- and `end < start` is then `-1 < -1`, which
        # is False. On its own it waves the slice through as s[1:-1], which on
        # this string lands on "26" and calls a snowfall out of a script that
        # never mentioned one.
        #
        # Measured: `start < 0` could be deleted and all 523 tests stayed green,
        # with the mutant cut from the real source so the two arms differ by
        # that clause alone and its answer read back as an oracle -- 'x26y'
        # gives None here and "26" without it. Not an equivalent mutant: it is
        # the same invented reading this class already refuses on the other
        # side.
        script = "x26y"
        unguarded_reading = script[script.find("n(") + 2 : script.find(", 'CurrentWeather")]

        assert unguarded_reading == "26"
        assert hourly._DESCRIPTION_MAP[unguarded_reading] == "Snow"
        assert hourly._icon_code(script) is None


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


def warning_routes(day_fixture: str = "hko_warning_day.html") -> dict:
    return {
        warnings._SIGNALS_URL: read_fixture("hko_warning_legend.html"),
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

    def test_the_column_map_matches_the_header_row_it_names(self):
        soup = BeautifulSoup(read_fixture("hko_warning_day.html"), "html.parser")
        headers = [th.text.strip() for th in soup.find_all("th")]

        assert len(headers) == warnings._WARNING_ROW_CELLS
        assert headers[warnings._ICON_COL] == "Signal"
        assert headers[warnings._SIGNAL_COL] == "Name"
        assert headers[warnings._START_TIME_COL] == "From"
        assert headers[warnings._START_DATE_COL] == "Date"
        assert headers[warnings._END_TIME_COL] == "To"
        assert headers[warnings._END_DATE_COL] == "Date"

    def test_a_page_without_the_marker_raises_instead_of_parsing(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes("hko_warning_day_no_marker.html"))

        with pytest.raises(WeatherPageStructureError):
            warnings.fetch_day("2021-01-01", {})

    def test_the_offset_a_missing_marker_used_to_yield_parses_the_wrong_table(self):
        # The trap the guard exists for, pinned so the guard cannot be removed
        # without this failing: str.find() answers "absent" with -1, and
        # -1 + len(marker) is a perfectly usable slice index. On this fixture
        # that index lands ahead of a complete six-cell table, so the unguarded
        # slice returned a full, plausible and entirely wrong record instead of
        # returning nothing. Asserting only that fetch_day no longer crashes
        # would not have told these two apart.
        page = read_fixture("hko_warning_day_no_marker.html")
        marker = warnings._WARNING_TABLE_MARKER
        assert page.find(marker) == -1

        bad_offset = page.find(marker) + len(marker)
        assert bad_offset == 31

        salvaged = BeautifulSoup(page[bad_offset:], "html.parser")
        wrong_rows = [
            tr
            for table in salvaged.find_all("table")
            for tr in table.find_all("tr")
            if len(tr.find_all("td")) == warnings._WARNING_ROW_CELLS
        ]
        assert len(wrong_rows) == 1
        assert "ROW FROM AN UNRELATED TABLE" in wrong_rows[0].text

    def test_a_day_with_no_warning_still_carries_the_marker(self):
        # Why a missing marker is an error and not an empty result: the heading
        # is page furniture rather than a consequence of the weather, so it is
        # there even on a day when nothing was in force. Its absence can only
        # mean the page changed shape, which is not the same thing as a quiet
        # day -- and the quiet day already has its own empty-list test below.
        assert warnings._WARNING_TABLE_MARKER in read_fixture("hko_warning_day_empty.html")

    def test_a_row_without_an_icon_keeps_the_row(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes("hko_warning_day_no_icon.html"))

        records = warnings.fetch_day("2021-01-01", {})

        assert [r.warning_signal for r in records] == ["COLD WEATHER WARNING"]
        assert records[0].icon_url == ""

    def test_returns_empty_list_when_no_warning_was_in_force(self, monkeypatch):
        install_fake_get(monkeypatch, warnings, warning_routes("hko_warning_day_empty.html"))
        meta = warnings._load_signal_metadata()

        assert warnings.fetch_day("2021-01-01", meta) == []

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
