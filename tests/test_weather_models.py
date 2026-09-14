"""The raw-row contract between the weather collectors and the export layer (CUI-0011).

``to_raw_row()`` / ``from_raw_row()`` are the only place that spells the scraper
column names, so these tests pin both halves against each other (round-trip) and
against the committed ``data/raw/weather/*.json`` files (real samples copied
verbatim into ``fixtures/weather/raw_*_sample.json``).

Nothing here touches the network: the models module makes no requests at all.
"""

import json
import math
from pathlib import Path

import pytest

from run365days.common import config
from run365days.weather.models import (
    RAW_DATE_COLUMN,
    RAW_HOURLY_TIME_COLUMN,
    RAW_WARNING_SIGNAL_COLUMN,
    SUN_MOON_SUNRISE_COLUMN,
    SUN_MOON_SUNSET_COLUMN,
    DailyWeather,
    HourlyWeather,
    SunMoon,
    WeatherWarning,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "weather"

# The six sun/moon columns the legacy pipeline joined into the daily extract.
# DailyWeather deliberately owns none of them -- see its to_raw_row() docstring.
JOINED_SUN_MOON_COLUMNS = (
    "Sunrise",
    "Solar Noon",
    "Sunset",
    "Moonrise",
    "Moon Transit",
    "Moonset",
)

DAILY = DailyWeather(
    date="2021-01-08",
    mean_temp_c=18.5,
    max_temp_c=21.0,
    min_temp_c=15.0,
    mean_humidity_pct=70.0,
    total_rainfall_mm=0.0,
    mean_wind_kmh=12.0,
)

HOURLY = HourlyWeather(
    date="2021-01-08",
    time="12:00",
    temperature_c=19.0,
    wind_kmh=10.0,
    humidity_pct=65.0,
    description="Clear weather",
)

SUN_MOON = SunMoon(
    date="2021-01-08",
    sunrise="07:04",
    sunset="17:57",
    solar_noon="12:31",
    day_length="10:52:41",
    moonrise="02:19",
    moon_transit="07:31",
    moonset="12:44",
    moon_illumination_pct=24.3,
)

WARNING = WeatherWarning(
    date="2021-01-08",
    warning_type="Fire Danger Warnings",
    warning_signal="RED FIRE DANGER WARNING",
    start_time="2021-01-08 06:00:00",
    end_time="2021-01-08 18:00:00",
    icon_url="/images_e/firer.gif",
)


def read_rows(name: str) -> list[dict]:
    """Load a JSON Lines fixture copied verbatim out of ``data/raw/weather/``."""
    text = (FIXTURE_DIR / name).read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


class TestRoundTrip:
    """``from_raw_row(to_raw_row(record))`` must give the record back unchanged."""

    def test_daily_weather_round_trips_through_its_raw_row(self):
        assert DailyWeather.from_raw_row(DAILY.to_raw_row()) == DAILY

    def test_hourly_weather_round_trips_through_its_raw_row(self):
        assert HourlyWeather.from_raw_row(HOURLY.to_raw_row()) == HOURLY

    def test_weather_warning_round_trips_through_its_raw_row(self):
        assert WeatherWarning.from_raw_row(WARNING.to_raw_row()) == WARNING

    def test_sun_moon_round_trips_through_its_raw_row(self):
        assert SunMoon.from_raw_row(SUN_MOON.to_raw_row()) == SUN_MOON

    def test_sun_moon_round_trips_when_the_moon_neither_rises_nor_sets(self):
        # The three moon-event columns are "/" in the committed file on the days
        # the event falls outside the calendar day, so None has to survive the
        # trip out to "/" and back rather than becoming the string.
        absent = SunMoon(
            date="2021-01-28",
            sunrise="07:03",
            sunset="18:09",
            solar_noon="12:36",
            day_length="11:05:55",
            moonrise=None,
            moon_transit=None,
            moonset=None,
            moon_illumination_pct=None,
        )
        assert SunMoon.from_raw_row(absent.to_raw_row()) == absent

    def test_daily_weather_round_trips_when_every_reading_is_absent(self):
        empty = DailyWeather(date="2021-01-08")
        assert DailyWeather.from_raw_row(empty.to_raw_row()) == empty

    def test_hourly_weather_round_trips_when_every_reading_is_absent(self):
        empty = HourlyWeather(
            date="2021-01-08",
            time="12:00",
            temperature_c=None,
            wind_kmh=None,
            humidity_pct=None,
            description="Unknown",
        )
        assert HourlyWeather.from_raw_row(empty.to_raw_row()) == empty


class TestCommittedFileFormat:
    """``from_raw_row()`` must read the files already sitting in data/raw/weather/."""

    def test_daily_reads_every_reading_of_a_committed_row(self):
        row = read_rows("raw_hko_daily_sample.json")[0]

        record = DailyWeather.from_raw_row(row)

        assert record == DailyWeather(
            date="2021-01-01",
            mean_temp_c=11.8,
            max_temp_c=15.0,
            min_temp_c=8.6,
            mean_humidity_pct=40.0,
            total_rainfall_mm=0.0,
            mean_wind_kmh=25.6,
        )

    def test_daily_maps_an_unreadable_reading_to_none(self):
        # HKO writes "Trace" rather than a number for immeasurable rainfall.
        trace_row = read_rows("raw_hko_daily_sample.json")[1]

        record = DailyWeather.from_raw_row(trace_row)

        assert record.total_rainfall_mm is None
        assert record.max_temp_c == 20.0

    @pytest.mark.parametrize("index", [0, 1])
    def test_hourly_reads_a_committed_row_whichever_way_the_numbers_are_spelled(self, index):
        # The committed file stores Temperature (°C) as an int in most rows and
        # as a string in 46 of them, so the reader has to accept both.
        row = read_rows("raw_hourly_sample.json")[index]

        record = HourlyWeather.from_raw_row(row)

        assert record.temperature_c == float(row["Temperature (°C)"])
        assert record.humidity_pct == float(row["Humidity (%)"])
        assert record.wind_kmh == float(row["Wind (Km/h)"])
        assert record.description == row["Description"]

    def test_warning_reads_every_column_of_a_committed_row(self):
        row = read_rows("raw_warning_sample.json")[0]

        record = WeatherWarning.from_raw_row(row)

        assert record == WeatherWarning(
            date="2021-01-01",
            warning_type="Fire Danger Warnings",
            warning_signal="RED FIRE DANGER WARNING",
            start_time="2020-12-30 06:00:00",
            end_time="2021-01-02 20:00:00",
            icon_url="/images_e/firer.gif",
        )

    def test_sun_moon_reads_every_column_of_a_committed_row(self):
        row = read_rows("raw_sun_moon_sample.json")[0]

        record = SunMoon.from_raw_row(row)

        assert record == SunMoon(
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

    @pytest.mark.parametrize(
        ("index", "field"),
        [(1, "moonrise"), (2, "moonset"), (3, "moon_transit")],
        ids=["no moonrise", "no moonset", "no meridian passing"],
    )
    def test_sun_moon_reads_the_committed_absent_marker_as_none(self, index, field):
        # One real row apiece out of the committed file: the moon skips a rise,
        # a set or a meridian passing roughly once a lunar month.
        row = read_rows("raw_sun_moon_sample.json")[index]

        assert getattr(SunMoon.from_raw_row(row), field) is None

    def test_sun_moon_reads_a_committed_illumination_as_a_number(self):
        rows = read_rows("raw_sun_moon_sample.json")

        assert [SunMoon.from_raw_row(r).moon_illumination_pct for r in rows] == [
            97.2,
            55.6,
            45.8,
            100.0,
        ]

    def test_sun_moon_writes_the_column_names_the_committed_file_uses(self):
        assert set(SUN_MOON.to_raw_row()) == set(read_rows("raw_sun_moon_sample.json")[0])

    def test_sun_moon_writes_the_absent_marker_the_committed_file_uses(self):
        row = read_rows("raw_sun_moon_sample.json")[1]
        rewritten = SunMoon.from_raw_row(row).to_raw_row()

        # Byte-for-byte the same row, so re-collecting a day the moon skips
        # cannot quietly change how that day is spelled on disk.
        assert rewritten == row

    def test_daily_writes_the_column_names_the_committed_file_uses(self):
        committed = set(read_rows("raw_hko_daily_sample.json")[0])

        assert set(DAILY.to_raw_row()) == committed - set(JOINED_SUN_MOON_COLUMNS)

    def test_hourly_writes_the_column_names_the_committed_file_uses(self):
        assert set(HOURLY.to_raw_row()) == set(read_rows("raw_hourly_sample.json")[0])

    def test_warning_writes_the_column_names_the_committed_file_uses(self):
        assert set(WARNING.to_raw_row()) == set(read_rows("raw_warning_sample.json")[0])


class TestJoinedSunMoonColumnsAreExcluded:
    """Sunrise/Sunset live on SunMoon, not on DailyWeather (CUI-0011 item 4)."""

    def test_daily_weather_declares_no_sun_moon_field(self):
        assert not {"sunrise", "sunset", "solar_noon", "moonrise", "moonset"} & set(
            DailyWeather.__dataclass_fields__
        )

    def test_daily_raw_row_writes_none_of_the_joined_sun_moon_columns(self):
        assert not set(JOINED_SUN_MOON_COLUMNS) & set(DAILY.to_raw_row())

    def test_daily_from_raw_row_ignores_the_joined_sun_moon_columns(self):
        committed = read_rows("raw_hko_daily_sample.json")[0]

        without = {k: v for k, v in committed.items() if k not in JOINED_SUN_MOON_COLUMNS}

        assert DailyWeather.from_raw_row(committed) == DailyWeather.from_raw_row(without)


class TestMappingOnlyNotBounding:
    """The dataclass maps columns; bounding a reading belongs to the writer layer.

    CUI-0009 put ``finite_float`` in front of the nine readings that reach the
    writers. Folding that gate into ``from_raw_row()`` instead would move it off
    the export path and out of the collectors' reach, so the pair stays purely a
    name mapping and keeps ``inf`` / ``nan`` exactly as ``to_float`` parsed them.
    """

    def test_daily_from_raw_row_keeps_a_non_finite_reading(self):
        record = DailyWeather.from_raw_row({RAW_DATE_COLUMN: "2021-01-08", "Max. Temp": "inf"})

        assert math.isinf(record.max_temp_c)

    def test_hourly_from_raw_row_keeps_a_non_finite_reading(self):
        record = HourlyWeather.from_raw_row(
            {
                RAW_DATE_COLUMN: "2021-01-08",
                RAW_HOURLY_TIME_COLUMN: "12:00",
                "Temperature (°C)": "nan",
            }
        )

        assert math.isnan(record.temperature_c)


class TestExportedColumnNames:
    """Columns other modules scan by must be importable, not respelled by hand."""

    def test_the_scan_columns_match_the_committed_files(self):
        daily = read_rows("raw_hko_daily_sample.json")[0]
        hourly = read_rows("raw_hourly_sample.json")[0]
        warning = read_rows("raw_warning_sample.json")[0]

        assert RAW_DATE_COLUMN in daily and RAW_DATE_COLUMN in hourly
        assert RAW_HOURLY_TIME_COLUMN in hourly
        assert RAW_WARNING_SIGNAL_COLUMN in warning
        assert SUN_MOON_SUNRISE_COLUMN in daily and SUN_MOON_SUNSET_COLUMN in daily


class TestMissingStringColumns:
    """A string column the row does not carry reads as ``""`` (CUI-0014).

    CUI-0011 introduced that default as a side effect of keeping the dataclass
    annotations an honest ``str``. It is a real narrowing -- ``""`` and ``None``
    part company downstream, at ``value is None`` and at JSON ``""`` against
    ``null`` -- so it is pinned here as a decision rather than left as something
    nobody chose.

    The decision is to keep ``""``, on three measurements:

    * It cannot be reached from the committed files. All 17,984 hourly rows,
      461 warning rows and 363 daily rows carry exactly one distinct key set
      each, every declared column present, so the export is byte-identical
      either way. :class:`TestCommittedRowsCarryEveryColumn` keeps that true.
    * ``""`` is already in-band in this format rather than an invention of the
      reader: the warnings collector writes ``icon_url=""`` for a row whose
      signal icon is missing, and the export fixture in ``conftest`` carries a
      literal ``"Warning_Signal": ""``.
    * Widening the fields back to ``str | None`` would let ``None`` reach
      ``export.models.WeatherWarning.signal``, which is ``Mapped[str]`` and not
      nullable -- a real type conflict traded for an unreachable one.

    Seven fields take the default, not the five the ticket named: ``time`` on
    the hourly record and ``warning_signal`` and ``icon_url`` on the warning are
    in the same position as the four it listed.
    """

    def test_hourly_reads_a_missing_time_and_description_as_empty_text(self):
        record = HourlyWeather.from_raw_row({RAW_DATE_COLUMN: "2021-01-08"})

        assert record.time == ""
        assert record.description == ""

    def test_warning_reads_every_missing_string_column_as_empty_text(self):
        record = WeatherWarning.from_raw_row({RAW_DATE_COLUMN: "2021-01-08"})

        assert record.warning_type == ""
        assert record.warning_signal == ""
        assert record.start_time == ""
        assert record.end_time == ""
        assert record.icon_url == ""

    def test_no_string_field_reads_as_none(self):
        # The half that would otherwise break silently: an edit widening one
        # field back to None still passes any "is falsy" test written for "".
        hourly = HourlyWeather.from_raw_row({RAW_DATE_COLUMN: "2021-01-08"})
        warning = WeatherWarning.from_raw_row({RAW_DATE_COLUMN: "2021-01-08"})

        assert all(value is not None for value in vars(warning).values())
        assert hourly.time is not None
        assert hourly.description is not None

    def test_sun_moon_reads_every_missing_sun_column_as_empty_text(self):
        record = SunMoon.from_raw_row({RAW_DATE_COLUMN: "2021-01-08"})

        assert record.sunrise == ""
        assert record.sunset == ""
        assert record.solar_noon == ""
        assert record.day_length == ""

    def test_sun_moon_reads_every_missing_moon_column_as_none(self):
        # The moon fields are the other half of the same rule: they are declared
        # optional because "/" is a value the committed file really carries, so
        # absence has somewhere to go that is not a string.
        record = SunMoon.from_raw_row({RAW_DATE_COLUMN: "2021-01-08"})

        assert record.moonrise is None
        assert record.moonset is None
        assert record.moon_transit is None
        assert record.moon_illumination_pct is None

    @pytest.mark.parametrize("model", [HourlyWeather, WeatherWarning, DailyWeather, SunMoon])
    def test_a_row_without_a_date_is_refused_rather_than_defaulted(self, model):
        # Date is the one column with no sensible empty value: a record that
        # cannot say which day it describes joins to nothing. It is read with
        # [] rather than .get() on purpose, and that asymmetry is the decision.
        with pytest.raises(KeyError):
            model.from_raw_row({})


class TestCommittedRowsCarryEveryColumn:
    """The measurement the CUI-0014 decision rests on, kept as a standing check.

    If a future collection ever writes a row short of a column, the default
    above stops being unreachable and the choice has to be made again with that
    in hand. These read the real files rather than the copied samples, because
    it is the whole file the argument is about.
    """

    @pytest.mark.parametrize(
        "path",
        [
            config.WEATHER_HISTORY_JSON,
            config.WEATHER_WARNING_JSON,
            config.HKO_DAILY_JSON,
            config.SUN_MOON_JSON,
        ],
        ids=["hourly", "warnings", "hko-daily", "sun-moon"],
    )
    def test_every_row_of_the_committed_file_has_the_same_columns(self, path):
        if not path.exists():
            pytest.skip(f"{path} is not present in this data directory")

        text = path.read_text(encoding="utf-8")
        key_sets = {tuple(sorted(json.loads(line))) for line in text.splitlines() if line.strip()}

        assert len(key_sets) == 1, f"{path.name} carries {len(key_sets)} different column sets"
