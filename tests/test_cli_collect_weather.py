"""Tests for the run365-weather entry point (CUI-0013).

The three collectors reached 100% under CUI-0010; the CLI that drives them in
production did not, and the hole it left open was the one AU-002 and S-005 had
already closed in the other two entry points -- writing the destination file
whatever the collectors returned.

Nothing here touches the network. ``block_real_sockets`` severs
``socket.socket.connect`` for the whole module, and
``test_the_socket_block_refuses_an_unfaked_collector_call`` exercises that
guard rather than assuming it: "the sandbox has no internet" is not a proof,
because the agent proxy listens on 127.0.0.1 and an unfaked collector connects
to it happily.
"""

import json
import socket
from datetime import datetime
from typing import NoReturn

import pytest

from run365days.cli import collect_weather
from run365days.common import config
from run365days.weather.collectors import hko_daily, hourly, sun_moon, warnings
from run365days.weather.models import DailyWeather, HourlyWeather, SunMoon, WeatherWarning

HOURLY = HourlyWeather(
    date="2021-01-01",
    time="00:00",
    temperature_c=11.0,
    wind_kmh=24.0,
    humidity_pct=24.0,
    description="Clear weather",
)
WARNING = WeatherWarning(
    date="2021-01-01",
    warning_type="Rainstorm",
    warning_signal="AMBER",
    start_time="2021-01-01 06:00:00",
    end_time="2021-01-01 09:00:00",
    icon_url="/images/amber.gif",
)
DAILY = DailyWeather(date="2021-01-01", max_temp_c=15.0, mean_temp_c=11.8)
SUN_MOON = SunMoon(
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


@pytest.fixture(autouse=True)
def block_real_sockets(monkeypatch):
    """Make any unfaked outbound connection fail loudly instead of scraping."""

    def refuse(*args, **kwargs) -> NoReturn:
        raise AssertionError("a test tried to open a real network connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)


@pytest.fixture
def destinations(monkeypatch, tmp_path):
    """Point every destination at a temporary directory and return the paths."""
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


def install_collectors(
    monkeypatch,
    *,
    hourly_rows=(HOURLY,),
    warning_rows=(WARNING,),
    daily_rows=(DAILY,),
    sun_moon_rows=(SUN_MOON,),
):
    """Replace the four collectors with fakes that record how they were called."""
    calls: dict[str, tuple] = {}

    def fake_hourly(start, end) -> list[HourlyWeather]:
        calls["hourly"] = (start, end)
        return list(hourly_rows)

    def fake_warnings(start, end) -> list[WeatherWarning]:
        calls["warnings"] = (start, end)
        return list(warning_rows)

    def fake_daily(year) -> list[DailyWeather]:
        calls["hko-daily"] = (year,)
        return list(daily_rows)

    def fake_sun_moon(year) -> list[SunMoon]:
        calls["sun-moon"] = (year,)
        return list(sun_moon_rows)

    monkeypatch.setattr(hourly, "fetch_range", fake_hourly)
    monkeypatch.setattr(warnings, "fetch_range", fake_warnings)
    monkeypatch.setattr(hko_daily, "fetch_year", fake_daily)
    monkeypatch.setattr(sun_moon, "fetch_year", fake_sun_moon)
    return calls


def read_rows(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


class TestTheNetworkGuardItself:
    def test_the_socket_block_refuses_an_unfaked_collector_call(self):
        # The one test in this module that installs no fake. It must raise, or
        # every other test here is only assumed to be offline.
        with pytest.raises(AssertionError, match="real network connection"):
            hourly.fetch_day("2021-01-01")


class TestRun:
    def test_writes_every_source_when_asked_for_all(self, monkeypatch, destinations):
        install_collectors(monkeypatch)

        empty = collect_weather.run("all", "2021-01-01", "2021-01-31", 2021)

        assert empty == {}
        assert all(path.exists() for path in destinations.values())

    def test_writes_the_raw_column_names_the_export_reads_back(self, monkeypatch, destinations):
        install_collectors(monkeypatch)

        collect_weather.run("hourly", "2021-01-01", "2021-01-31", 2021)

        # to_raw_row(), not __dict__: the export layer reads these files by
        # scraper column name, and writing field names instead is what CUI-0011
        # found broken. Re-stated here because this CLI is the only writer.
        assert read_rows(destinations["hourly"]) == [HOURLY.to_raw_row()]

    def test_collects_only_the_requested_source(self, monkeypatch, destinations):
        calls = install_collectors(monkeypatch)

        collect_weather.run("warnings", "2021-01-01", "2021-01-31", 2021)

        assert list(calls) == ["warnings"]
        assert destinations["warnings"].exists()
        assert not destinations["hourly"].exists()

    def test_passes_the_range_to_the_dated_collectors_and_the_year_to_hko(
        self, monkeypatch, destinations
    ):
        calls = install_collectors(monkeypatch)

        collect_weather.run("all", "2021-03-01", "2021-03-31", 2021)

        assert calls["hourly"] == ("2021-03-01", "2021-03-31")
        assert calls["warnings"] == ("2021-03-01", "2021-03-31")
        assert calls["hko-daily"] == ("2021",)
        assert calls["sun-moon"] == ("2021",)

    def test_writes_the_sun_moon_history_where_config_points(self, monkeypatch, destinations):
        # AU-037: SunMoon had no writer, so config.SUN_MOON_JSON named a file
        # nothing in the package could produce and re-collecting the weather
        # could only ever shrink what data/raw/weather/ holds.
        install_collectors(monkeypatch)

        collect_weather.run("sun-moon", "2021-01-01", "2021-01-31", 2021)

        assert read_rows(destinations["sun-moon"]) == [SUN_MOON.to_raw_row()]

    def test_names_the_source_that_collected_nothing(self, monkeypatch, destinations):
        install_collectors(monkeypatch, warning_rows=())

        empty = collect_weather.run("all", "2021-01-01", "2021-01-31", 2021)

        assert list(empty) == ["warnings"]

    def test_leaves_the_committed_file_untouched_when_a_source_collects_nothing(
        self, monkeypatch, destinations
    ):
        # The whole point. A renamed HKO heading makes every day yield nothing
        # (CUI-0012 turned that from a crash into an empty list), and opening
        # the destination with "w" before knowing that would blank 461 committed
        # rows on the way to reporting success.
        destinations["warnings"].write_text('{"Date": "2021-01-01"}\n', encoding="utf-8")
        install_collectors(monkeypatch, warning_rows=())

        collect_weather.run("warnings", "2021-01-01", "2021-01-31", 2021)

        assert destinations["warnings"].read_text(encoding="utf-8") == '{"Date": "2021-01-01"}\n'


class TestMain:
    def test_defaults_the_range_to_january_first_of_the_target_year(
        self, monkeypatch, destinations
    ):
        calls = install_collectors(monkeypatch)
        monkeypatch.setattr("sys.argv", ["run365-weather", "--source", "hourly", "--year", "2019"])

        collect_weather.main()

        start, end = calls["hourly"]
        assert start == "2019-01-01"
        assert end == datetime.today().strftime("%Y-%m-%d")

    def test_honours_an_explicit_start_and_end_date(self, monkeypatch, destinations):
        calls = install_collectors(monkeypatch)
        monkeypatch.setattr(
            "sys.argv",
            [
                "run365-weather",
                "--source",
                "hourly",
                "--start-date",
                "2021-06-01",
                "--end-date",
                "2021-06-30",
            ],
        )

        collect_weather.main()

        assert calls["hourly"] == ("2021-06-01", "2021-06-30")

    def test_defaults_to_collecting_every_source(self, monkeypatch, destinations):
        calls = install_collectors(monkeypatch)
        monkeypatch.setattr("sys.argv", ["run365-weather"])

        collect_weather.main()

        assert sorted(calls) == ["hko-daily", "hourly", "sun-moon", "warnings"]

    def test_exits_zero_when_every_source_produced_records(self, monkeypatch, destinations):
        install_collectors(monkeypatch)
        monkeypatch.setattr("sys.argv", ["run365-weather"])

        collect_weather.main()  # no SystemExit

    def test_exits_non_zero_naming_every_empty_source(self, monkeypatch, destinations):
        install_collectors(monkeypatch, hourly_rows=(), daily_rows=())
        monkeypatch.setattr("sys.argv", ["run365-weather"])

        with pytest.raises(SystemExit) as exit_info:
            collect_weather.main()

        # Both empty sources are named in one message: a single pass should say
        # whether every source is broken or only one of them is.
        message = str(exit_info.value)
        assert "hourly" in message
        assert "hko-daily" in message
        assert "warnings" not in message

    def test_still_writes_the_sources_that_did_produce_records(self, monkeypatch, destinations):
        install_collectors(monkeypatch, hourly_rows=())
        monkeypatch.setattr("sys.argv", ["run365-weather"])

        with pytest.raises(SystemExit):
            collect_weather.main()

        assert read_rows(destinations["warnings"]) == [WARNING.to_raw_row()]
        assert read_rows(destinations["hko-daily"]) == [DAILY.to_raw_row()]

    def test_progress_goes_to_stdout(self, monkeypatch, destinations, capsys):
        install_collectors(monkeypatch)
        monkeypatch.setattr("sys.argv", ["run365-weather", "--source", "hourly"])

        collect_weather.main()

        out = capsys.readouterr().out
        assert str(destinations["hourly"]) in out
        assert "1 record" in out
