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
from run365days.weather.collectors import WeatherPageStructureError, hko_daily, hourly, warnings
from run365days.weather.models import DailyWeather, HourlyWeather, WeatherWarning

COLLECT_WEATHER_LOGGER = "run365days.cli.collect_weather"


@pytest.fixture(autouse=True)
def block_real_sockets(monkeypatch):
    """Make any unfaked outbound connection fail loudly instead of scraping."""

    def refuse(*args, **kwargs):
        raise AssertionError("a test tried to open a real network connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)


@pytest.fixture
def out_paths(monkeypatch, tmp_path):
    """Point the three output files at a temporary directory."""
    paths = {
        "hourly": tmp_path / "weather_history.json",
        "warnings": tmp_path / "weather_warning_history.json",
        "hko-daily": tmp_path / "hko_daily_weather_extract.json",
    }
    monkeypatch.setattr(config, "WEATHER_HISTORY_JSON", paths["hourly"])
    monkeypatch.setattr(config, "WEATHER_WARNING_JSON", paths["warnings"])
    monkeypatch.setattr(config, "HKO_DAILY_JSON", paths["hko-daily"])
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


def install_fake_collectors(monkeypatch, *, hourly_answer=None, warnings_answer=None, hko=None):
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

        collect_weather.run("all", "2021-03-01", "2021-03-09", 2021)

        assert seen == [("2021-03-01", "2021-03-09"), ("2021-03-01", "2021-03-09")]

    def test_passes_the_requested_year_to_the_daily_extract(self, monkeypatch, out_paths):
        seen = []
        monkeypatch.setattr(hko_daily, "fetch_year", lambda y: seen.append(y) or [])

        collect_weather.run("hko-daily", "2021-01-01", "2021-01-02", 2019)

        assert seen == ["2019"]

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
        )

        assert collect_weather.run("all", "2021-01-01", "2021-01-02", 2021) == 3

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

    def test_exits_zero_when_every_source_was_collected(self, monkeypatch, out_paths, capsys):
        install_fake_collectors(monkeypatch)
        monkeypatch.setattr(sys, "argv", ["run365-weather", "--source", "all"])

        collect_weather.main()
