"""Tests for the weather collection entry point (CUI-0013).

The three collectors reached 100% coverage in CUI-0010 while the command that
drives them stayed largely untested, so the part that decides *what happens when
a source fails* had never been run. Every test here fakes the collectors: none
of them may reach the network, and the autouse fixture below is what proves it.
"""

import json
import logging
import socket
import sys

import pytest
import requests

from run365days.cli import collect_weather
from run365days.common import config
from run365days.weather.collectors import (
    WeatherPageStructureError,
    hko_daily,
    hourly,
    sun_moon,
    warnings,
)
from run365days.weather.models import DailyWeather, HourlyWeather, SunMoon, WeatherWarning

COLLECT_WEATHER_LOGGER = "run365days.cli.collect_weather"

# The real entry point, captured before any test replaces it, for the test that
# drives the HKO collector end to end behind a faked requests.get.
REAL_HKO_FETCH_YEAR = hko_daily.fetch_year


class FakeResponse:
    """The slice of ``requests.Response`` the collectors read."""

    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error", response=self)


@pytest.fixture(autouse=True)
def block_real_sockets(monkeypatch):
    """Make any unfaked outbound connection fail loudly instead of scraping."""

    def refuse(*args, **kwargs):
        raise AssertionError("a test tried to open a real network connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)


@pytest.fixture
def out_paths(monkeypatch, tmp_path):
    """Point every output file at a temporary directory."""
    paths = {
        "hourly": tmp_path / "weather_history.json",
        "warnings": tmp_path / "weather_warning_history.json",
        "hko-daily": tmp_path / "hko_daily_weather_extract.json",
        "sun-moon": tmp_path / "sun_moon_rise_set_history.json",
    }
    monkeypatch.setattr(config, "WEATHER_HISTORY_JSON", paths["hourly"])
    monkeypatch.setattr(config, "WEATHER_WARNING_JSON", paths["warnings"])
    monkeypatch.setattr(config, "HKO_DAILY_JSON", paths["hko-daily"])
    monkeypatch.setattr(config, "SUN_MOON_JSON", paths["sun-moon"])
    return paths


def an_hourly_record() -> HourlyWeather:
    return HourlyWeather(
        date="2021-01-01",
        time="00:00",
        temperature_c=11.0,
        wind_kmh=24.0,
        humidity_pct=24.0,
        description="Clear weather",
    )


def a_warning_record() -> WeatherWarning:
    return WeatherWarning(
        date="2021-01-01",
        warning_type="Fire Danger Warnings",
        warning_signal="RED FIRE DANGER WARNING",
        start_time="2020-12-30 06:00:00",
        end_time="2021-01-02 20:00:00",
        icon_url="/images_e/firer.gif",
    )


def a_daily_record() -> DailyWeather:
    return DailyWeather(
        date="2021-01-01",
        max_temp_c=15.0,
        mean_temp_c=11.8,
        min_temp_c=8.6,
        mean_humidity_pct=40.0,
        total_rainfall_mm=0.0,
        mean_wind_kmh=25.6,
    )


def a_sun_moon_record() -> SunMoon:
    return SunMoon(
        date="2021-01-01",
        sunrise="07:02",
        sunset="17:50",
        solar_noon="12:26",
        day_length="10:47:58",
        moonrise="19:51",
        moon_transit="01:50",
        moonset="08:44",
        moon_illumination_pct=97.2,
    )


def install_fake_collectors(
    monkeypatch, *, hourly_answer=None, warnings_answer=None, hko=None, sun_moon_answer=None
):
    """Replace each collector's fetch entry point with a canned answer.

    An answer that is an exception is raised instead of returned, which is how
    a test asks for one source to fail while the others succeed.
    """

    def answer_with(value):
        def fetch(*args, **kwargs):
            if isinstance(value, Exception):
                raise value
            return value

        return fetch

    monkeypatch.setattr(
        hourly,
        "fetch_range",
        answer_with([an_hourly_record()] if hourly_answer is None else hourly_answer),
    )
    monkeypatch.setattr(
        warnings,
        "fetch_range",
        answer_with([a_warning_record()] if warnings_answer is None else warnings_answer),
    )
    monkeypatch.setattr(
        hko_daily, "fetch_year", answer_with([a_daily_record()] if hko is None else hko)
    )
    monkeypatch.setattr(
        sun_moon,
        "fetch_year",
        answer_with([a_sun_moon_record()] if sun_moon_answer is None else sun_moon_answer),
    )


class TestRun:
    def test_collects_every_source_when_asked_for_all(self, monkeypatch, out_paths):
        install_fake_collectors(monkeypatch)

        failed = collect_weather.run("all", "2021-01-01", "2021-01-02", 2021)

        assert failed == 0
        assert all(path.exists() for path in out_paths.values())

    def test_collects_only_the_named_source(self, monkeypatch, out_paths):
        install_fake_collectors(monkeypatch)

        collect_weather.run("warnings", "2021-01-01", "2021-01-02", 2021)

        assert out_paths["warnings"].exists()
        assert not out_paths["hourly"].exists()
        assert not out_paths["hko-daily"].exists()

    def test_writes_the_records_in_their_raw_scraper_column_names(self, monkeypatch, out_paths):
        install_fake_collectors(monkeypatch)

        collect_weather.run("hourly", "2021-01-01", "2021-01-02", 2021)

        row = json.loads(out_paths["hourly"].read_text().splitlines()[0])
        assert row == an_hourly_record().to_raw_row()

    def test_passes_the_requested_range_to_the_ranged_collectors(self, monkeypatch, out_paths):
        seen = []
        monkeypatch.setattr(hourly, "fetch_range", lambda s, e: seen.append((s, e)) or [])
        monkeypatch.setattr(warnings, "fetch_range", lambda s, e: seen.append((s, e)) or [])
        monkeypatch.setattr(hko_daily, "fetch_year", lambda y: [])
        monkeypatch.setattr(sun_moon, "fetch_year", lambda y: [])

        collect_weather.run("all", "2021-03-01", "2021-03-09", 2021)

        assert seen == [("2021-03-01", "2021-03-09"), ("2021-03-01", "2021-03-09")]

    def test_passes_the_requested_year_to_the_daily_extract(self, monkeypatch, out_paths):
        seen = []
        monkeypatch.setattr(hko_daily, "fetch_year", lambda y: seen.append(y) or [])

        collect_weather.run("hko-daily", "2021-01-01", "2021-01-02", 2019)

        assert seen == ["2019"]

    def test_passes_the_requested_year_to_the_sun_moon_history(self, monkeypatch, out_paths):
        seen = []
        monkeypatch.setattr(sun_moon, "fetch_year", lambda y: seen.append(y) or [])

        collect_weather.run("sun-moon", "2021-01-01", "2021-01-02", 2019)

        assert seen == ["2019"]

    def test_writes_the_sun_moon_history_where_config_points(self, monkeypatch, out_paths):
        # AU-037: SunMoon had no writer, so config.SUN_MOON_JSON named a file
        # nothing in the package could produce.
        install_fake_collectors(monkeypatch)

        collect_weather.run("sun-moon", "2021-01-01", "2021-01-02", 2021)

        rows = [json.loads(line) for line in out_paths["sun-moon"].read_text().splitlines()]
        assert rows == [a_sun_moon_record().to_raw_row()]
        assert not out_paths["hko-daily"].exists()

    def test_an_unreachable_source_does_not_stop_the_others(self, monkeypatch, out_paths):
        install_fake_collectors(
            monkeypatch, hourly_answer=requests.ConnectionError("Name or service not known")
        )

        failed = collect_weather.run("all", "2021-01-01", "2021-01-02", 2021)

        assert failed == 1
        assert not out_paths["hourly"].exists()
        assert out_paths["warnings"].exists()
        assert out_paths["hko-daily"].exists()

    def test_a_page_that_changed_shape_does_not_stop_the_others(self, monkeypatch, out_paths):
        install_fake_collectors(
            monkeypatch, warnings_answer=WeatherPageStructureError("page landmark not found")
        )

        failed = collect_weather.run("all", "2021-01-01", "2021-01-02", 2021)

        assert failed == 1
        assert not out_paths["warnings"].exists()
        assert out_paths["hourly"].exists()

    def test_counts_every_source_that_failed(self, monkeypatch, out_paths):
        install_fake_collectors(
            monkeypatch,
            hourly_answer=requests.Timeout("read timed out"),
            warnings_answer=WeatherPageStructureError("page landmark not found"),
            hko=requests.ConnectionError("unreachable"),
            sun_moon_answer=requests.ConnectionError("unreachable"),
        )

        assert collect_weather.run("all", "2021-01-01", "2021-01-02", 2021) == 4

    def test_logs_the_source_that_failed_rather_than_printing_it(
        self, monkeypatch, out_paths, caplog, capsys
    ):
        install_fake_collectors(
            monkeypatch, hourly_answer=requests.ConnectionError("Name or service not known")
        )

        with caplog.at_level(logging.WARNING, logger=COLLECT_WEATHER_LOGGER):
            collect_weather.run("hourly", "2021-01-01", "2021-01-02", 2021)

        skipped = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(skipped) == 1
        assert "hourly weather history" in skipped[0].getMessage()
        # The failure is a record of what did not happen, so it must not be
        # mixed into the progress a user reads as good news.
        assert "could not be collected" not in capsys.readouterr().out

    def test_keeps_reporting_progress_on_stdout(self, monkeypatch, out_paths, capsys):
        install_fake_collectors(monkeypatch)

        collect_weather.run("hourly", "2021-01-01", "2021-01-02", 2021)

        printed = capsys.readouterr().out
        assert "Collecting hourly weather history" in printed
        assert "Wrote 1 records" in printed


class TestSunMoonOutage:
    """W-063, driven through the real collector rather than a faked fetch_year."""

    class ForbiddenResponse:
        status_code = 403
        text = "<html><body><p>Forbidden</p></body></html>"

        def raise_for_status(self) -> None:
            raise requests.HTTPError("403 Client Error: Forbidden", response=self)

    def test_a_refused_year_leaves_the_committed_history_in_place(self, monkeypatch, out_paths):
        history = '{"Date": "2021-01-01"}\n'
        out_paths["sun-moon"].write_text(history)
        monkeypatch.setattr(requests, "get", lambda *args, **kwargs: self.ForbiddenResponse())

        failed = collect_weather.run("sun-moon", "2021-01-01", "2021-01-02", 2021)

        assert failed == 1
        assert out_paths["sun-moon"].read_text() == history


class TestRefusedOrEmptySource:
    """S-137, driven through the real collectors behind a faked requests.get."""

    def test_an_hourly_range_with_no_readable_day_leaves_the_history_in_place(
        self, monkeypatch, out_paths
    ):
        # Measured before the fix: fetch_range returned [] and _write_jsonl
        # replaced the file with an empty one while run() reported no failure.
        history = '{"Date": "2021-01-01"}\n'
        out_paths["hourly"].write_text(history)
        monkeypatch.setattr(
            requests,
            "get",
            lambda *args, **kwargs: FakeResponse("<html><body>no history here</body></html>"),
        )

        failed = collect_weather.run("hourly", "2021-01-01", "2021-01-02", 2021)

        assert failed == 1
        assert out_paths["hourly"].read_text() == history

    def test_a_refused_hko_daily_extract_does_not_stop_the_sources_after_it(
        self, monkeypatch, out_paths
    ):
        # Measured before the fix: the 403 body reached json.loads, the
        # JSONDecodeError escaped _COLLECTION_FAILURES and run("all") crashed,
        # so sun-moon -- queued after hko-daily -- was never collected.
        install_fake_collectors(monkeypatch)
        monkeypatch.setattr(hko_daily, "fetch_year", REAL_HKO_FETCH_YEAR)
        monkeypatch.setattr(
            requests,
            "get",
            lambda *args, **kwargs: FakeResponse(
                "<html><body>403 Forbidden</body></html>", status_code=403
            ),
        )

        failed = collect_weather.run("all", "2021-01-01", "2021-01-02", 2021)

        assert failed == 1
        assert not out_paths["hko-daily"].exists()
        assert out_paths["sun-moon"].exists()
        assert out_paths["hourly"].exists()
        assert out_paths["warnings"].exists()

    def test_a_hko_daily_extract_of_the_wrong_shape_leaves_the_history_in_place(
        self, monkeypatch, out_paths
    ):
        # S-149, measured before the fix: a yearly payload that is valid JSON
        # but lacks "stn" let KeyError: 'stn' out of fetch_year, past
        # _COLLECTION_FAILURES, and run() crashed instead of counting a failure.
        history = '{"Date": "2021-01-01"}\n'
        out_paths["hko-daily"].write_text(history)
        monkeypatch.setattr(hko_daily, "fetch_year", REAL_HKO_FETCH_YEAR)
        monkeypatch.setattr(
            requests, "get", lambda *args, **kwargs: FakeResponse(json.dumps({"other": 1}))
        )

        failed = collect_weather.run("hko-daily", "2021-01-01", "2021-01-02", 2021)

        assert failed == 1
        assert out_paths["hko-daily"].read_text() == history


class TestMain:
    def test_defaults_the_range_to_the_whole_year_from_january(
        self, monkeypatch, out_paths, capsys
    ):
        seen = []
        monkeypatch.setattr(hourly, "fetch_range", lambda s, e: seen.append((s, e)) or [])
        monkeypatch.setattr(sys, "argv", ["run365-weather", "--source", "hourly"])

        collect_weather.main()

        assert seen[0][0] == f"{config.DEFAULT_YEAR}-01-01"

    def test_honours_an_explicit_range(self, monkeypatch, out_paths, capsys):
        seen = []
        monkeypatch.setattr(hourly, "fetch_range", lambda s, e: seen.append((s, e)) or [])
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run365-weather",
                "--source",
                "hourly",
                "--start-date",
                "2020-06-01",
                "--end-date",
                "2020-06-30",
            ],
        )

        collect_weather.main()

        assert seen == [("2020-06-01", "2020-06-30")]

    def test_exits_non_zero_when_a_source_could_not_be_collected(
        self, monkeypatch, out_paths, capsys
    ):
        install_fake_collectors(
            monkeypatch, hourly_answer=requests.ConnectionError("Name or service not known")
        )
        monkeypatch.setattr(sys, "argv", ["run365-weather", "--source", "hourly"])

        with pytest.raises(SystemExit) as exit_info:
            collect_weather.main()

        assert exit_info.value.code != 0

    def test_accepts_sun_moon_as_a_source(self, monkeypatch, out_paths, capsys):
        seen = []
        monkeypatch.setattr(sun_moon, "fetch_year", lambda y: seen.append(y) or [])
        monkeypatch.setattr(
            sys, "argv", ["run365-weather", "--source", "sun-moon", "--year", "2020"]
        )

        collect_weather.main()

        assert seen == ["2020"]

    def test_exits_zero_when_every_source_was_collected(self, monkeypatch, out_paths, capsys):
        install_fake_collectors(monkeypatch)
        monkeypatch.setattr(sys, "argv", ["run365-weather", "--source", "all"])

        collect_weather.main()
