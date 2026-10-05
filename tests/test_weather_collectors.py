"""Collector tests (CUI-0010) plus the AU-014 timeout and AU-013 logging guards.

Every test drives the collectors through a fake ``requests.get``. The autouse
``block_real_sockets`` fixture is the proof that none of them reaches the real
network: it severs ``socket.socket.connect`` for the whole module, so a test
that forgot to install a fake would raise instead of quietly scraping HKO.
"""

import importlib
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
from run365days.weather.collectors import (
    WeatherPageStructureError,
    _parsing,
    hko_daily,
    hourly,
    sun_moon,
    warnings,
)
from run365days.weather.models import SunMoon

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
SUN_MOON_LOGGER = "run365days.weather.collectors.sun_moon"

# S-137: what a refusing host actually serves. It parses as HTML, carries none
# of the landmarks a collector navigates by, and is not JSON -- so before the
# status check every collector read it as something other than an outage.
ERROR_PAGE = "<html><body>403 Forbidden</body></html>"


def error_page(status_code: int = 403) -> "FakeResponse":
    return FakeResponse(ERROR_PAGE, status_code=status_code)


# The four January days the sun and moon fixtures carry. Each is a real row out
# of data/raw/weather/sun_moon_rise_set_history.json (raw_sun_moon_sample.json
# holds them verbatim), so the collector is checked against the file the legacy
# scraper actually produced rather than an expectation written beside it.
COMMITTED_JANUARY = [
    SunMoon(
        date="2021-01-01",
        sunrise="07:02",
        sunset="17:50",
        solar_noon="12:26",
        day_length="10:47:58",
        moonrise="19:51",
        moon_transit="01:50",
        moonset="08:44",
        moon_illumination_pct=97.2,
    ),
    SunMoon(
        date="2021-01-06",
        sunrise="07:04",
        sunset="17:54",
        solar_noon="12:29",
        day_length="10:49:56",
        moonrise=None,
        moon_transit="06:03",
        moonset="12:13",
        moon_illumination_pct=55.6,
    ),
    SunMoon(
        date="2021-01-20",
        sunrise="07:04",
        sunset="18:03",
        solar_noon="12:34",
        day_length="10:58:53",
        moonrise="11:46",
        moon_transit="18:04",
        moonset=None,
        moon_illumination_pct=45.8,
    ),
    SunMoon(
        date="2021-01-28",
        sunrise="07:03",
        sunset="18:09",
        solar_noon="12:36",
        day_length="11:05:55",
        moonrise="17:40",
        moon_transit=None,
        moonset="06:36",
        moon_illumination_pct=100.0,
    ),
]


def read_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def one_row_history_page(cells: dict[int, str]) -> str:
    """A daily-history page holding a single data row, cells given by column index.

    Built here rather than added to ``freemeteo_day.html`` so the shared fixture
    keeps saying one thing. Every cell freemeteo sends carries its unit, so a
    unit-less cell cannot be expressed in that fixture without making it
    unrepresentative of the page it stands for.
    """
    header = "".join("<th></th>" for _ in range(hourly._WEATHER_COL + 1))
    row = "".join(f"<td>{cells.get(col, '')}</td>" for col in range(hourly._WEATHER_COL + 1))
    return f'<table class="daily-history"><tr>{header}</tr><tr>{row}</tr></table>'


@pytest.fixture(autouse=True)
def block_real_sockets(monkeypatch):
    """Make any unfaked outbound connection fail loudly instead of scraping."""

    def refuse(*args, **kwargs):
        raise AssertionError("a test tried to open a real network connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)


class FakeResponse:
    """The slice of ``requests.Response`` the collectors use.

    ``status_code`` defaults to 200 so every route written as a bare body keeps
    meaning "served"; a route that is a ``FakeResponse`` itself can say
    otherwise, which is how a test serves an error page.
    """

    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error", response=self)


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
        return answer if isinstance(answer, FakeResponse) else FakeResponse(answer)

    @property
    def timeouts(self) -> list:
        return [call["timeout"] for call in self.calls]


def assert_bounded_timeout(timeout) -> None:
    """Assert *timeout* is the shared constant, and that it is bounded.

    Identity, not equality (S-090). The range check alone says every request is
    bounded, which was never the whole claim: ``config.py`` says the timeouts
    live there "rather than once per collector so retiming them cannot leave
    one behind (CUI-0013)". A collector that reintroduced a local
    ``_REQUEST_TIMEOUT = (3, 20)`` would be bounded, would pass every range
    assertion, and would be exactly the thing centralising them was meant to
    prevent -- measured: all six timeout tests stayed green under that
    mutant. ``is`` catches a value that is right but comes from the wrong
    place, which is the property actually being bought here.
    """
    assert timeout is not None, "requests.get was called without a timeout"
    assert timeout is config.HTTP_REQUEST_TIMEOUT, (
        "this request carries its own timeout rather than the shared one, so retiming "
        "config.HTTP_REQUEST_TIMEOUT would leave it behind"
    )
    connect, read = timeout
    assert 0 < connect <= MAX_ACCEPTABLE_CONNECT_SEC
    assert 0 < read <= MAX_ACCEPTABLE_READ_SEC


def install_fake_get(monkeypatch, routes: dict) -> RequestRecorder:
    """Answer every ``requests.get`` from *routes* for the rest of the test.

    Patched on the ``requests`` module itself rather than on one collector's
    reference to it: every collector reaches the network through the shared
    ``_parsing.fetch_text`` (S-137), so the lookup that has to be intercepted
    is ``requests.get``, wherever the call is written.
    """
    recorder = RequestRecorder(routes)
    monkeypatch.setattr(requests, "get", recorder)
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
        install_fake_get(monkeypatch, hko_routes())

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
        install_fake_get(monkeypatch, hko_routes())

        records = hko_daily.fetch_year("2021")

        assert all("Mean" not in r.date for r in records)
        assert len([r for r in records if r.date.startswith("2021-01")]) == 3

    def test_falls_back_to_the_per_month_endpoint_when_daydata_is_empty(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, hko_routes())

        records = hko_daily.fetch_year("2021")

        february = [r for r in records if r.date.startswith("2021-02")]
        assert [r.date for r in february] == ["2021-02-01", "2021-02-02"]
        assert february[0].max_temp_c == 25.1
        assert HKO_FEB_URL in [call["url"] for call in recorder.calls]

    def test_skips_the_month_when_the_fallback_payload_is_not_valid_json(self, monkeypatch):
        install_fake_get(monkeypatch, hko_routes(mar="<html>503 Service Unavailable"))

        records = hko_daily.fetch_year("2021")

        assert not [r for r in records if r.date.startswith("2021-03")]
        assert [r.date for r in records if r.date.startswith("2021-02")]

    def test_logs_the_month_and_the_reason_when_a_month_is_skipped(self, monkeypatch, caplog):
        install_fake_get(monkeypatch, hko_routes(mar="<html>503 Service Unavailable"))

        with caplog.at_level(logging.WARNING, logger=HKO_DAILY_LOGGER):
            hko_daily.fetch_year("2021")

        skipped = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(skipped) == 1
        message = skipped[0].getMessage()
        assert "2021" in message
        assert "03" in message
        assert "JSONDecodeError" in message

    def test_skips_the_month_when_the_fallback_payload_has_no_day_data(self, monkeypatch, caplog):
        install_fake_get(monkeypatch, hko_routes(mar=json.dumps({"stn": {"data": []}})))

        with caplog.at_level(logging.WARNING, logger=HKO_DAILY_LOGGER):
            records = hko_daily.fetch_year("2021")

        assert not [r for r in records if r.date.startswith("2021-03")]
        assert "IndexError" in caplog.records[0].getMessage()

    def test_every_request_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, hko_routes())

        hko_daily.fetch_year("2021")

        assert recorder.timeouts
        for timeout in recorder.timeouts:
            assert_bounded_timeout(timeout)

    def test_an_unreachable_host_is_not_reported_as_a_missing_month(self, monkeypatch):
        routes = hko_routes(feb=requests.ConnectionError("Name or service not known"))
        install_fake_get(monkeypatch, routes)

        with pytest.raises(requests.ConnectionError):
            hko_daily.fetch_year("2021")

    def test_a_stalled_endpoint_is_not_reported_as_a_missing_month(self, monkeypatch):
        routes = hko_routes(feb=requests.Timeout("read timed out"))
        install_fake_get(monkeypatch, routes)

        with pytest.raises(requests.Timeout):
            hko_daily.fetch_year("2021")

    def test_an_error_status_on_the_yearly_endpoint_raises_http_error(self, monkeypatch):
        # S-137: the 403 body used to reach json.loads and leak JSONDecodeError,
        # which collect_weather does not catch, so `run("all")` crashed.
        routes = hko_routes()
        routes[HKO_YEAR_URL] = error_page()
        install_fake_get(monkeypatch, routes)

        with pytest.raises(requests.HTTPError):
            hko_daily.fetch_year("2021")

    def test_a_yearly_payload_that_is_not_json_is_a_page_structure_error(self, monkeypatch):
        # S-137: served with a 200, so the status check lets it through, and
        # json.loads used to raise JSONDecodeError -- a ValueError that
        # collect_weather does not catch, so it crashed the whole run and every
        # source after this one went uncollected.
        routes = hko_routes()
        routes[HKO_YEAR_URL] = "<html><body>Scheduled maintenance</body></html>"
        install_fake_get(monkeypatch, routes)

        with pytest.raises(WeatherPageStructureError, match="2021") as raised:
            hko_daily.fetch_year("2021")

        assert "JSON" in str(raised.value)
        assert isinstance(raised.value.__cause__, json.JSONDecodeError)

    @pytest.mark.parametrize("status_code", [403, 500])
    def test_an_error_status_on_the_per_month_endpoint_is_not_a_missing_month(
        self, monkeypatch, status_code
    ):
        # S-137: a refused per-month request is an outage, not a month the
        # extract has no data for, so it must not be logged and skipped. W-066
        # carves 404 out of this; a refusal or a server fault stays a fault.
        install_fake_get(monkeypatch, hko_routes(feb=error_page(status_code)))

        with pytest.raises(requests.HTTPError):
            hko_daily.fetch_year("2021")

    def test_a_per_month_404_skips_that_month_and_keeps_the_rest(self, monkeypatch, caplog):
        # W-066: the per-month endpoint is only asked for months the yearly file
        # left empty, i.e. recent ones. If HKO answers 404 for a month it has
        # not published yet, raising here failed the whole source every time
        # the current year was collected.
        install_fake_get(monkeypatch, hko_routes(feb=error_page(404)))

        with caplog.at_level(logging.WARNING, logger=HKO_DAILY_LOGGER):
            records = hko_daily.fetch_year("2021")

        assert not [r for r in records if r.date.startswith("2021-02")]
        assert [r.date for r in records if r.date.startswith("2021-01")]
        february = [r.getMessage() for r in caplog.records if "2021-02" in r.getMessage()]
        assert len(february) == 1
        assert "404" in february[0]

    def test_a_per_month_http_error_without_a_response_is_not_a_missing_month(self, monkeypatch):
        # W-066: only a response that says 404 is read as "no extract"; an
        # HTTPError that carries no response says nothing about the month.
        install_fake_get(monkeypatch, hko_routes(feb=requests.HTTPError("no response")))

        with pytest.raises(requests.HTTPError):
            hko_daily.fetch_year("2021")

    def test_a_yearly_404_still_raises(self, monkeypatch):
        # W-066 carves 404 out of the per-month fallback only: without the
        # yearly file there is no year to read, so its 404 is a fault.
        routes = hko_routes()
        routes[HKO_YEAR_URL] = error_page(404)
        install_fake_get(monkeypatch, routes)

        with pytest.raises(requests.HTTPError):
            hko_daily.fetch_year("2021")


class TestHourlyFetchDay:
    def test_parses_each_observation_row(self, monkeypatch):
        install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert [r.time for r in records] == ["00:00", "00:30", "01:00"]
        assert records[0].date == "2021-01-01"
        assert records[0].temperature_c == 11.0
        assert records[0].wind_kmh == 24.0
        assert records[0].humidity_pct == 24.0
        assert records[0].description == "Clear weather"

    def test_reads_the_wind_speed_from_the_variable_direction_form(self, monkeypatch):
        install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert records[1].wind_kmh == 20.0
        assert records[1].description == "Cloudy skies"

    def test_maps_an_unreadable_temperature_to_none(self, monkeypatch):
        install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert records[2].temperature_c is None

    def test_maps_an_unknown_description_code_to_unknown(self, monkeypatch):
        install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert records[2].description == "Unknown"

    def test_skips_rows_whose_cell_count_does_not_match_the_header(self, monkeypatch):
        install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_day("2021-01-01")

        assert "Daily summary" not in [r.time for r in records]
        assert len(records) == 3

    def test_returns_empty_list_when_the_page_has_no_history_table(self, monkeypatch):
        install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_no_table.html")})

        assert hourly.fetch_day("2021-01-01") == []

    def test_an_error_status_raises_rather_than_reading_as_a_day_without_a_table(self, monkeypatch):
        # S-137: the error page has no daily-history table either, so without
        # the status check a refusal read exactly like a day with no data.
        install_fake_get(monkeypatch, {hourly._URL: error_page()})

        with pytest.raises(requests.HTTPError):
            hourly.fetch_day("2021-01-01")

    def test_queries_the_requested_date(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})

        hourly.fetch_day("2021-01-01")

        assert recorder.calls[0]["params"]["date"] == "2021-01-01"

    def test_request_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})

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
        install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_no_script.html")})

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
            monkeypatch, {hourly._URL: read_fixture("freemeteo_unexpected_script.html")}
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

    def test_a_wind_cell_without_its_unit_keeps_its_digits(self):
        # hourly.py states in a comment that the units are "stripped by name so
        # that a cell which arrives without its unit keeps its digits instead of
        # losing its last few". Nothing held it: swapping `removesuffix` back
        # for the old fixed-length `[:-5]` slice left all 525 tests green,
        # because every wind cell in the fixtures carries " Km/h" and so never
        # walks the path the sentence is about.
        #
        # Both cell shapes, since they reach the suffix by different routes --
        # one splits on the bearing separator, the other strips a prefix.
        assert hourly._wind_speed_kmh("Northeast 50° 24") == 24.0
        assert hourly._wind_speed_kmh("Variable at 20") == 20.0
        assert hourly._wind_speed_kmh("Northeast 50° 24 Km/h") == 24.0

    def test_a_row_whose_cells_carry_no_units_is_read_at_full_precision(self, monkeypatch):
        # The same claim for temperature and humidity, which are stripped inline
        # in fetch_day rather than through a helper, so a pure-function test
        # cannot reach them. Measured: `[:-2]` for °C and `[:-1]` for % each
        # left all 525 tests green, turning 11 into None and 40 into 4 on a
        # unit-less row -- a wrong number, not a missing one, for humidity.
        page = one_row_history_page(
            {
                hourly._TIME_COL: "09:00",
                hourly._TEMPERATURE_COL: "11",
                hourly._WIND_COL: "Northeast 50° 24",
                hourly._HUMIDITY_COL: "40",
            }
        )
        install_fake_get(monkeypatch, {hourly._URL: page})

        record = hourly.fetch_day("2021-01-01")[0]

        assert record.temperature_c == 11.0
        assert record.humidity_pct == 40.0
        assert record.wind_kmh == 24.0

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
        recorder = install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})

        records = hourly.fetch_range("2021-01-01", "2021-01-03")

        assert [call["params"]["date"] for call in recorder.calls] == [
            "2021-01-01",
            "2021-01-02",
            "2021-01-03",
        ]
        assert len(records) == 9

    def test_a_range_with_no_readable_day_raises_rather_than_returning_nothing(self, monkeypatch):
        # S-137: an empty list here was written by collect_weather over the
        # existing hourly history -- a whole range of pages without the table
        # is a page that changed shape, the same rule sun_moon follows (W-063).
        install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_no_table.html")})

        with pytest.raises(WeatherPageStructureError, match="2021-01-01"):
            hourly.fetch_range("2021-01-01", "2021-01-03")

    def test_one_day_without_a_table_inside_a_readable_range_is_kept_quiet(self, monkeypatch):
        # The zero-records rule is about the whole range: one empty day among
        # readable ones is a gap, not a changed page, and must not cost the rest.
        pages = {
            "2021-01-01": read_fixture("freemeteo_day.html"),
            "2021-01-02": read_fixture("freemeteo_no_table.html"),
            "2021-01-03": read_fixture("freemeteo_day.html"),
        }
        monkeypatch.setattr(
            requests,
            "get",
            lambda url, params=None, timeout=None: FakeResponse(pages[params["date"]]),
        )

        records = hourly.fetch_range("2021-01-01", "2021-01-03")

        assert {r.date for r in records} == {"2021-01-01", "2021-01-03"}

    def test_every_request_in_the_range_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})

        hourly.fetch_range("2021-01-01", "2021-01-02")

        for timeout in recorder.timeouts:
            assert_bounded_timeout(timeout)


def warning_routes(day_fixture: str = "hko_warning_day.html") -> dict:
    return {
        warnings._SIGNALS_URL: read_fixture("hko_warning_legend.html"),
        warnings._HISTORY_URL: read_fixture(day_fixture),
    }


class TestParsingFallbacks:
    """The two defensive branches in `_parsing`, which line coverage cannot see.

    `_parsing.py` reports 100% of its statements covered, and both guards below
    could still be deleted with the whole suite green: the lines are executed
    on every page, it is only the `else` side of each that no fixture reaches.
    A worked example of what a line-coverage number does not promise (S-088).
    """

    def test_a_multi_valued_attribute_falls_back_instead_of_returning_a_list(self):
        # bs4 answers with a list for attributes HTML defines as multi-valued.
        # None of the attributes actually read here is one, so the isinstance
        # check is what keeps a list from being handed to a caller annotated
        # `str`. Measured: deleting it left all 527 tests green, because no
        # fixture asks for such an attribute.
        cell = BeautifulSoup('<td><img class="a b" src="x.png"/></td>', "html.parser").td

        assert _parsing.child_attr(cell, "img", "class", "fallback") == "fallback"
        assert _parsing.child_attr(cell, "img", "src", "fallback") == "x.png"

    def test_a_child_holding_no_single_string_reads_as_absent(self):
        # An empty <script> is the shape this hits in practice, and `.string`
        # is None for it. Without the check, `str(child.string)` returns the
        # four-character string "None" -- a value that reads as content.
        # Measured: deleting it left all 527 tests green, because today
        # `_icon_code` refuses "None" anyway. That is a guard leaning on
        # another guard, and the one it leans on is itself only pinned as of
        # W-031, so this says it directly rather than through the collector.
        cell = BeautifulSoup("<td><script></script></td>", "html.parser").td

        assert _parsing.child_string(cell, "script") is None
        assert _parsing.child_string(cell, "img") is None


class TestWarningSignalMetadata:
    def test_maps_each_icon_alt_to_its_index_and_warning_type(self, monkeypatch):
        install_fake_get(monkeypatch, warning_routes())

        meta = warnings._load_signal_metadata()

        assert meta["Red Fire Danger Warning"] == {"Idx": "firer", "Type": "Fire Danger Warnings"}
        assert meta["Yellow Fire Danger Warning"]["Type"] == "Fire Danger Warnings"
        assert meta["Standby Signal No.1"]["Idx"] == "tc1"

    def test_request_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warning_routes())

        warnings._load_signal_metadata()

        assert_bounded_timeout(recorder.calls[0]["timeout"])

    def test_an_error_status_raises_rather_than_reading_an_empty_legend(self, monkeypatch):
        routes = warning_routes()
        routes[warnings._SIGNALS_URL] = error_page()
        install_fake_get(monkeypatch, routes)

        with pytest.raises(requests.HTTPError):
            warnings._load_signal_metadata()


class TestWarningsFetchDay:
    def test_parses_every_six_cell_warning_row(self, monkeypatch):
        install_fake_get(monkeypatch, warning_routes())
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
        install_fake_get(monkeypatch, warning_routes())
        meta = warnings._load_signal_metadata()

        records = warnings.fetch_day("2021-01-01", meta)

        assert "SIGNAL BEFORE THE MARKER" not in [r.warning_signal for r in records]

    def test_skips_rows_that_do_not_have_six_cells(self, monkeypatch):
        install_fake_get(monkeypatch, warning_routes())
        meta = warnings._load_signal_metadata()

        records = warnings.fetch_day("2021-01-01", meta)

        assert len(records) == 3

    def test_maps_a_signal_missing_from_the_legend_to_unknown(self, monkeypatch):
        install_fake_get(monkeypatch, warning_routes())
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
        install_fake_get(monkeypatch, warning_routes("hko_warning_day_no_marker.html"))

        with pytest.raises(WeatherPageStructureError):
            warnings.fetch_day("2021-01-01", {})

    def test_an_error_status_is_reported_as_an_http_error_not_a_page_change(self, monkeypatch):
        # S-137: the error page lacks the marker too, so it used to surface as
        # WeatherPageStructureError -- caught, but blaming a layout change for
        # what was a refused request.
        routes = warning_routes()
        routes[warnings._HISTORY_URL] = error_page()
        install_fake_get(monkeypatch, routes)

        with pytest.raises(requests.HTTPError):
            warnings.fetch_day("2021-01-01", {})

    def test_the_documented_failure_mode_is_reachable_without_a_private_import(self):
        # `fetch_day` and `fetch_range` both name this class in their public
        # `Raises:`, so writing it down is part of using them -- and until
        # W-032 the only place to import it from was `_parsing`, whose leading
        # underscore says the opposite: rename or split it at will. A caller
        # forced to reach into a private module to honour a public contract is
        # a caller the next refactor breaks silently.
        #
        # Imported by name here rather than at the top of this module so the
        # failure reads as this assertion. A top-level import would break
        # collection instead, which reds the file without saying which promise
        # was withdrawn.
        collectors = importlib.import_module("run365days.weather.collectors")
        published = getattr(collectors, "WeatherPageStructureError", None)

        assert published is not None, "the documented Raises: type left the public surface"
        assert published is _parsing.WeatherPageStructureError, (
            "the public name must be the class the collectors actually raise, not a copy"
        )
        assert "WeatherPageStructureError" in collectors.__all__

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
        install_fake_get(monkeypatch, warning_routes("hko_warning_day_no_icon.html"))

        records = warnings.fetch_day("2021-01-01", {})

        assert [r.warning_signal for r in records] == ["COLD WEATHER WARNING"]
        assert records[0].icon_url == ""

    def test_returns_empty_list_when_no_warning_was_in_force(self, monkeypatch):
        install_fake_get(monkeypatch, warning_routes("hko_warning_day_empty.html"))
        meta = warnings._load_signal_metadata()

        assert warnings.fetch_day("2021-01-01", meta) == []

    def test_queries_the_requested_day(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warning_routes())

        warnings.fetch_day("2021-01-01", {})

        assert recorder.calls[0]["params"] == {"start_ym": "20210101"}

    def test_request_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warning_routes())

        warnings.fetch_day("2021-01-01", {})

        assert_bounded_timeout(recorder.calls[0]["timeout"])


class TestWarningsFetchRange:
    def test_loads_the_signal_legend_once_for_the_whole_range(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warning_routes())

        warnings.fetch_range("2021-01-01", "2021-01-03")

        legend_calls = [c for c in recorder.calls if c["url"] == warnings._SIGNALS_URL]
        history_calls = [c for c in recorder.calls if c["url"] == warnings._HISTORY_URL]
        assert len(legend_calls) == 1
        assert len(history_calls) == 3

    def test_returns_every_days_warnings(self, monkeypatch):
        install_fake_get(monkeypatch, warning_routes())

        records = warnings.fetch_range("2021-01-01", "2021-01-02")

        assert [r.date for r in records] == ["2021-01-01"] * 3 + ["2021-01-02"] * 3

    def test_every_request_in_the_range_carries_a_bounded_timeout(self, monkeypatch):
        recorder = install_fake_get(monkeypatch, warning_routes())

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
        install_fake_get(monkeypatch, hko_routes())
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
        install_fake_get(monkeypatch, hko_routes())
        path = tmp_path / "hko_daily_weather_extract.json"

        _write_jsonl(hko_daily.fetch_year("2021"), path)
        first = [daily_weather_record(row) for row in load_jsonl(path)][0]

        readings = ("max_temp_c", "avg_temp_c", "min_temp_c", "humidity_pct", "wind_kmh")
        assert all(first[field] is not None for field in readings)

    def test_collected_hourly_rows_export_into_an_activity_weather_block(
        self, monkeypatch, tmp_path
    ):
        install_fake_get(monkeypatch, {hourly._URL: read_fixture("freemeteo_day.html")})
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
        install_fake_get(monkeypatch, warning_routes())
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


class MonthlyRequestRecorder(RequestRecorder):
    """A ``requests.get`` stand-in keyed by ``(url, month)``.

    The sun and moon pages are one URL apiece for the whole year and differ
    only by query string, so a URL-keyed map cannot tell January from February.
    """

    def __call__(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        answer = self.routes[(url, params["month"])]
        if isinstance(answer, Exception):
            raise answer
        return answer if isinstance(answer, FakeResponse) else FakeResponse(answer)


def install_fake_monthly_get(monkeypatch, routes: dict) -> MonthlyRequestRecorder:
    recorder = MonthlyRequestRecorder(routes)
    monkeypatch.setattr(requests, "get", recorder)
    return recorder


def sun_moon_routes(*, sun=None, moon=None, blank_months=True) -> dict:
    """Answer January from the fixtures and, by default, the rest with no table."""
    routes = {
        (sun_moon._SUN_URL, "01"): read_fixture(sun or "timeanddate_sun_202101.html"),
        (sun_moon._MOON_URL, "01"): read_fixture(moon or "timeanddate_moon_202101.html"),
    }
    if blank_months:
        empty = read_fixture("timeanddate_no_table.html")
        for month in range(2, 13):
            routes[(sun_moon._SUN_URL, f"{month:02d}")] = empty
            routes[(sun_moon._MOON_URL, f"{month:02d}")] = empty
    return routes


def january_by_date() -> dict[str, SunMoon]:
    return {record.date: record for record in sun_moon.fetch_month("2021", "01")}


# One cell of a moon row, written the way the fixture pages write them.
BEARING = "<td>&uarr; (65&deg;)</td>"
EMPTY_SLOT = '<td colspan="2">-</td>'
TRAILING_BLOCK = (
    "<td>01:50</td><td>(63.0&deg;)</td><td>384,000 km</td><td>97.2%</td>"  # transit .. illum
)


def moon_page(rows: dict[str, str]) -> str:
    """A moon page whose day rows are given as ``{day header: <td>... html}``.

    Built inline rather than added to ``timeanddate_moon_202101.html`` so that
    fixture keeps standing for the page as served, and each malformed row sits
    beside the test that says what is wrong with it.
    """
    body = "".join(f"<tr><th>{day}</th>{tds}</tr>" for day, tds in rows.items())
    return f'<table id="tb-7dmn"><tr><th>Jan 2021</th><th>Moonrise</th></tr>{body}</table>'


def moon_month_from(monkeypatch, rows: dict[str, str]) -> dict[str, dict]:
    install_fake_monthly_get(monkeypatch, {(sun_moon._MOON_URL, "01"): moon_page(rows)})
    return sun_moon._moon_month("2021", "01")


class TestSunMoonFetchMonth:
    """The port of ``legacy/03_GetSunMoonRiseSetHistory.py`` (AU-037)."""

    def test_reads_a_month_into_the_rows_the_committed_file_already_holds(self, monkeypatch):
        install_fake_monthly_get(monkeypatch, sun_moon_routes())

        assert sun_moon.fetch_month("2021", "01") == COMMITTED_JANUARY

    def test_reads_a_day_the_moon_never_rises_on(self, monkeypatch):
        install_fake_monthly_get(monkeypatch, sun_moon_routes())

        day = january_by_date()["2021-01-06"]

        assert day.moonrise is None
        assert day.moonset == "12:13"

    def test_reads_a_day_the_moon_never_sets_on(self, monkeypatch):
        install_fake_monthly_get(monkeypatch, sun_moon_routes())

        day = january_by_date()["2021-01-20"]

        assert day.moonset is None
        assert day.moonrise == "11:46"

    def test_reads_a_full_moon_with_no_meridian_passing(self, monkeypatch):
        # Grounded in the committed file, not in the legacy script alone:
        # TestTheCommittedSunMoonFile pins that every day without a meridian
        # passing carries exactly 100.0%.
        install_fake_monthly_get(monkeypatch, sun_moon_routes())

        day = january_by_date()["2021-01-28"]

        assert day.moon_transit is None
        assert day.moon_illumination_pct == 100.0

    def test_drops_a_day_whose_moon_row_runs_out_mid_walk(self, monkeypatch, caplog):
        # Day 30 is a full sun row whose moon row ends before the meridian
        # block. The merge is an inner join, so the day is dropped rather than
        # published with the moon columns silently blank.
        install_fake_monthly_get(monkeypatch, sun_moon_routes())

        with caplog.at_level(logging.WARNING, logger=SUN_MOON_LOGGER):
            collected = sun_moon.fetch_month("2021", "01")

        assert "2021-01-30" not in {r.date for r in collected}
        assert "2021-01-30" in caplog.text

    @pytest.mark.parametrize(
        "tds",
        [
            # W-062's repro: three filled slots and nothing after them. Read from
            # the end, the "meridian passing" was the 13:00 moonset.
            f"<td>01:00</td>{BEARING}<td>13:00</td>{BEARING}<td>23:50</td>{BEARING}",
            # A page that grew a column between the slots and the trailing
            # block: read from the end, every trailing field shifts by one.
            f"<td>01:00</td>{BEARING}<td>13:00</td>{BEARING}{EMPTY_SLOT}<td>x</td>"
            + TRAILING_BLOCK,
            # A merged trailing cell that is not the last cell of the row.
            f"<td>01:00</td>{BEARING}<td>13:00</td>{BEARING}{EMPTY_SLOT}"
            '<td colspan="4">-</td><td>x</td>',
            # W-065 (b): four cells remain, so the width alone fits, but the
            # last of them is the merged full-moon cell -- read as a full moon,
            # the three cells in front of it would never be looked at.
            f"<td>01:00</td>{BEARING}{EMPTY_SLOT}{EMPTY_SLOT}<td>x</td><td>y</td><td>z</td>"
            '<td colspan="4">-</td>',
            # W-065 (a): one cell remains, but it spans one column, not the
            # merged block's four. A guard that checks only the count reads the
            # 13:00 moonset as the meridian passing again (W-062).
            f"<td>01:00</td>{BEARING}<td>13:00</td>{BEARING}{EMPTY_SLOT}<td>x</td>",
        ],
        ids=[
            "nothing-after-the-slots",
            "one-cell-too-many",
            "merged-block-not-last",
            "merged-in-a-four-cell-tail",
            "single-plain-cell-after-slots",
        ],
    )
    def test_drops_a_day_whose_trailing_block_is_not_the_width_read_from_the_end(
        self, monkeypatch, caplog, tds
    ):
        with caplog.at_level(logging.WARNING, logger=SUN_MOON_LOGGER):
            days = moon_month_from(monkeypatch, {"3": tds})

        assert "2021-01-03" not in days
        assert "2021-01-03" in caplog.text

    def test_reads_the_normal_and_the_merged_trailing_block_beside_a_malformed_row(
        self, monkeypatch
    ):
        days = moon_month_from(
            monkeypatch,
            {
                "1": f"<td>19:51</td>{BEARING}<td>08:44</td>{BEARING}{EMPTY_SLOT}" + TRAILING_BLOCK,
                "3": f"<td>01:00</td>{BEARING}<td>13:00</td>{BEARING}<td>23:50</td>{BEARING}",
                "28": f"<td>17:40</td>{BEARING}<td>06:36</td>{BEARING}{EMPTY_SLOT}"
                '<td colspan="4">-</td>',
            },
        )

        assert days == {
            "2021-01-01": {
                "moonrise": "19:51",
                "moonset": "08:44",
                "moon_transit": "01:50",
                "moon_illumination_pct": 97.2,
            },
            "2021-01-28": {
                "moonrise": "17:40",
                "moonset": "06:36",
                "moon_transit": None,
                "moon_illumination_pct": 100.0,
            },
        }

    # "\u00b2" (superscript two) is a str.isdigit() digit that int() refuses
    # with a ValueError, which no collection-failure handler catches (S-141).
    @pytest.mark.parametrize("header", ["Note", "0", "32", "1a", "\u00b2"])
    def test_skips_a_row_whose_header_is_not_a_day_of_the_month(self, monkeypatch, caplog, header):
        # S-133: a lone <th> is how a day row is told apart, so a footnote row
        # with one header used to be published as the date "2021-01-Note".
        row = f"<td>19:51</td>{BEARING}<td>08:44</td>{BEARING}{EMPTY_SLOT}" + TRAILING_BLOCK

        with caplog.at_level(logging.WARNING, logger=SUN_MOON_LOGGER):
            days = moon_month_from(monkeypatch, {"1": row, header: row})

        assert list(days) == ["2021-01-01"]
        assert repr(header) in caplog.text

    def test_drops_a_row_too_short_to_read_by_position(self, monkeypatch, caplog):
        install_fake_monthly_get(monkeypatch, sun_moon_routes())

        with caplog.at_level(logging.WARNING, logger=SUN_MOON_LOGGER):
            collected = sun_moon.fetch_month("2021", "01")

        assert "2021-01-29" not in {r.date for r in collected}
        assert "2021-01-29" in caplog.text

    def test_reports_a_cell_that_states_no_time_rather_than_slicing_one_out(
        self, monkeypatch, caplog
    ):
        # "7:05 pm" is a real time in the wrong clock, and the legacy character
        # slice would have reported it as 07:05 -- twelve hours out, silently.
        # The column becomes a gap and the rest of the row survives (CUI-0018).
        install_fake_monthly_get(
            monkeypatch,
            sun_moon_routes(sun="timeanddate_sun_unreadable.html", blank_months=False),
        )

        with caplog.at_level(logging.WARNING, logger=SUN_MOON_LOGGER):
            days = sun_moon._sun_month("2021", "01")

        assert days["2021-01-01"] == {
            "sunrise": "",
            "sunset": "",
            "solar_noon": "12:21",
            "day_length": "",
        }
        assert "7:05 pm" in caplog.text

    def test_reports_an_illumination_cell_that_states_no_percentage(self, monkeypatch, caplog):
        install_fake_monthly_get(
            monkeypatch,
            sun_moon_routes(moon="timeanddate_moon_unreadable.html", blank_months=False),
        )

        with caplog.at_level(logging.WARNING, logger=SUN_MOON_LOGGER):
            days = sun_moon._moon_month("2021", "01")

        assert days["2021-01-01"]["moon_illumination_pct"] is None
        assert days["2021-01-01"]["moon_transit"] == "01:50"
        assert "not available" in caplog.text

    def test_a_slot_whose_colspan_is_not_a_number_is_read_as_a_filled_slot(self, monkeypatch):
        # colspan="two" used to raise out of the whole month. Reading an
        # unreadable span as "this slot holds an event" keeps the walk aligned
        # with the cells that follow it.
        install_fake_monthly_get(
            monkeypatch,
            sun_moon_routes(moon="timeanddate_moon_unreadable.html", blank_months=False),
        )

        day = sun_moon._moon_month("2021", "01")["2021-01-02"]

        assert day["moonrise"] == "20:30"
        assert day["moonset"] == "09:20"
        assert day["moon_illumination_pct"] == 92.1

    @pytest.mark.parametrize(
        ("page", "table_id"),
        [("sun", "as-monthsun"), ("moon", "tb-7dmn")],
    )
    def test_skips_a_month_whose_page_carries_no_table(self, monkeypatch, caplog, page, table_id):
        install_fake_monthly_get(
            monkeypatch, sun_moon_routes(**{page: "timeanddate_no_table.html"})
        )

        with caplog.at_level(logging.WARNING, logger=SUN_MOON_LOGGER):
            collected = sun_moon.fetch_month("2021", "01")

        assert collected == []
        assert table_id in caplog.text

    def test_sends_the_shared_bounded_timeout(self, monkeypatch):
        recorder = install_fake_monthly_get(monkeypatch, sun_moon_routes())

        sun_moon.fetch_month("2021", "01")

        assert recorder.timeouts
        for timeout in recorder.timeouts:
            assert_bounded_timeout(timeout)

    def test_an_error_status_is_raised_rather_than_read_as_an_empty_month(self, monkeypatch):
        # W-063: a 403 page carries no table, so without the status check it
        # read exactly like a month timeanddate had no data for.
        routes = sun_moon_routes()
        routes[(sun_moon._SUN_URL, "01")] = FakeResponse(
            read_fixture("timeanddate_no_table.html"), status_code=403
        )
        install_fake_monthly_get(monkeypatch, routes)

        with pytest.raises(requests.HTTPError):
            sun_moon.fetch_month("2021", "01")

    def test_an_unreachable_page_is_raised_rather_than_read_as_an_empty_month(self, monkeypatch):
        # A host that cannot be reached says nothing about that month, so
        # swallowing it would turn one outage into an empty month (AU-013).
        routes = sun_moon_routes()
        routes[(sun_moon._SUN_URL, "01")] = requests.ConnectionError("boom")
        install_fake_monthly_get(monkeypatch, routes)

        with pytest.raises(requests.ConnectionError):
            sun_moon.fetch_month("2021", "01")


def td(text: str):
    return BeautifulSoup(f"<table><tr><td>{text}</td></tr></table>", "html.parser").td


class TestSunMoonTimeCell:
    """W-064: a cell that states no 24-hour HH:MM must read as a gap, not a time."""

    @pytest.mark.parametrize(
        "text",
        [
            "10:47:58",  # a daylength: its tail "47:58" is not a time
            # A daylength whose tail is a valid time, so only the lookbehind can
            # refuse it -- "47:58" above is also out of range, which hides it.
            "11:05:55",
            "25:99",  # shaped like a time, but no clock reads it
            "7:05 p.m.",  # a 12-hour clock, dotted: twelve hours out if read
            # S-138: a third minute digit. Without a digit guard on the right
            # these read as 07:05 and 12:34, cut off the front of a longer number.
            "07:051",
            "12:345",
        ],
    )
    def test_a_cell_that_is_not_a_24_hour_time_is_a_logged_gap(self, caplog, text):
        with caplog.at_level(logging.WARNING, logger=SUN_MOON_LOGGER):
            read = sun_moon._time(td(text), "2021-01-01", "sunrise")

        assert read == ""
        assert text in caplog.text

    @pytest.mark.parametrize(
        ("text", "expected"),
        [("07:02 &uarr; (114&deg;)", "07:02"), ("7:05", "07:05"), ("23:59", "23:59")],
    )
    def test_a_24_hour_time_still_reads(self, text, expected):
        assert sun_moon._time(td(text), "2021-01-01", "sunrise") == expected


class TestSunMoonFetchYear:
    def test_a_year_with_no_readable_table_raises_rather_than_returning_nothing(self, monkeypatch):
        # W-063: twelve months of "no table" used to come back as an empty
        # list, which collect_weather then wrote over the committed history.
        install_fake_monthly_get(
            monkeypatch,
            sun_moon_routes(sun="timeanddate_no_table.html", moon="timeanddate_no_table.html"),
        )

        with pytest.raises(WeatherPageStructureError, match="2021"):
            sun_moon.fetch_year("2021")

    def test_walks_every_month_in_date_order(self, monkeypatch):
        recorder = install_fake_monthly_get(monkeypatch, sun_moon_routes())

        collected = sun_moon.fetch_year("2021")

        requested = sorted({call["params"]["month"] for call in recorder.calls})
        assert requested == [f"{month:02d}" for month in range(1, 13)]
        assert {call["params"]["year"] for call in recorder.calls} == {"2021"}
        assert [r.date for r in collected] == [r.date for r in COMMITTED_JANUARY]

    def test_the_socket_block_refuses_an_unfaked_call(self):
        # The one sun/moon test with no fake installed, so the guard this module
        # relies on is shown to bite for this collector rather than assumed.
        with pytest.raises(AssertionError, match="real network connection"):
            sun_moon.fetch_year("2021")


class TestSunMoonCollectThenWrite:
    """Collect -> _write_jsonl -> read back, the pairing CUI-0011 was found by."""

    def test_collected_rows_write_the_committed_rows_back(self, monkeypatch, tmp_path):
        install_fake_monthly_get(monkeypatch, sun_moon_routes())
        path = tmp_path / "sun_moon_rise_set_history.json"

        _write_jsonl(sun_moon.fetch_month("2021", "01"), path)

        committed = load_jsonl(FIXTURE_DIR / "raw_sun_moon_sample.json")
        assert load_jsonl(path) == committed

    def test_collected_rows_read_back_as_the_records_they_were_written_from(
        self, monkeypatch, tmp_path
    ):
        install_fake_monthly_get(monkeypatch, sun_moon_routes())
        collected = sun_moon.fetch_month("2021", "01")
        path = tmp_path / "sun_moon_rise_set_history.json"

        _write_jsonl(collected, path)

        assert [SunMoon.from_raw_row(row) for row in load_jsonl(path)] == collected
