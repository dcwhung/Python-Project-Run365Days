"""Dataclasses for the weather sources used to annotate runs.

Each scraped source also has a *raw row* form: the column names the scrapers
produced and that ``data/raw/weather/*.json`` still carries. Those names live
here, next to the dataclass they describe, as a ``to_raw_row()`` /
``from_raw_row()`` pair.

Keeping the writer and the reader in one place is the whole point. They used to
sit at opposite ends of the pipeline -- ``cli.collect_weather`` wrote
``record.__dict__`` while ``export.records`` read ``row["Date"]`` -- and drifted
apart unnoticed for as long as nobody re-collected the data (CUI-0011).

The pair maps column names and nothing else. Bounding a reading to a finite
value stays in the record layer, in front of the writers, where CUI-0009 put it:
folding it in here would take it off the export path for rows read straight from
the committed files, and would deny the collectors the ``inf`` / ``nan`` that
``to_float`` deliberately preserves.
"""

from dataclasses import dataclass
from typing import Final

from run365days.common.numeric import to_float

RAW_DATE_COLUMN: Final = "Date"
"""``Date``, the one column every scraped weather file agrees on."""

RAW_HOURLY_TIME_COLUMN: Final = "Time"
RAW_WARNING_SIGNAL_COLUMN: Final = "Warning_Signal"

SUN_MOON_SUNRISE_COLUMN: Final = "Sunrise"
SUN_MOON_SUNSET_COLUMN: Final = "Sunset"
"""Two of the six sun/moon columns the legacy pipeline joined into the HKO daily
extract. They belong to :class:`SunMoon`, not to :class:`DailyWeather` -- see
:meth:`DailyWeather.to_raw_row`. Public because ``export.records`` still reads
them off the joined daily rows; the sun/moon columns below have no reader
outside this module."""

_SUN_MOON_SOLAR_NOON_COLUMN: Final = "Solar Noon"
_SUN_MOON_DAY_LENGTH_COLUMN: Final = "Daylength"
_SUN_MOON_MOONRISE_COLUMN: Final = "Moonrise"
_SUN_MOON_MOON_TRANSIT_COLUMN: Final = "Moon Transit"
_SUN_MOON_MOONSET_COLUMN: Final = "Moonset"
_SUN_MOON_ILLUMINATION_COLUMN: Final = "Illumination"

_MOON_EVENT_ABSENT: Final = "/"
"""How ``sun_moon_rise_set_history.json`` spells a moon event that never happens.

The moon rises about fifty minutes later each day, so roughly once a lunar
month a calendar day holds no moonrise, no moonset or no meridian passing, and
the file writes ``"/"`` in that column. It is the file's own marker rather than
an invention of this reader, so the pair maps it to ``None`` on the way in and
writes it back on the way out: a day the moon skips has to survive a
re-collection spelled the way it already is on disk.
``TestTheCommittedSunMoonFile`` checks the whole committed file round-trips."""

_ILLUMINATION_SUFFIX: Final = "%"
_ILLUMINATION_DECIMALS: Final = 1
"""Decimals ``Illumination`` is written to, so a row read from the committed file
(``"97.2%"``, ``"100.0%"``) is written back character for character."""

_HOURLY_TEMPERATURE_COLUMN: Final = "Temperature (°C)"
_HOURLY_WIND_COLUMN: Final = "Wind (Km/h)"
_HOURLY_HUMIDITY_COLUMN: Final = "Humidity (%)"
_HOURLY_DESCRIPTION_COLUMN: Final = "Description"

_WARNING_TYPE_COLUMN: Final = "Type"
_WARNING_START_COLUMN: Final = "Start_Time"
_WARNING_END_COLUMN: Final = "End_Time"
_WARNING_ICON_COLUMN: Final = "Ico"

_DAILY_MAX_TEMP_COLUMN: Final = "Max. Temp"
_DAILY_MEAN_TEMP_COLUMN: Final = "Avg. Temp"
_DAILY_MIN_TEMP_COLUMN: Final = "Min. Temp"
_DAILY_HUMIDITY_COLUMN: Final = "Humidity (%)"
_DAILY_RAINFALL_COLUMN: Final = "Total Rainfall (mm)"
_DAILY_WIND_COLUMN: Final = "Avg. Wind Speed (km/h)"

_MISSING_TEXT: Final = ""
"""What a missing *string* column reads as: empty text, not ``None`` (CUI-0014).

Every string field on these dataclasses is annotated ``str``, and this default is
what keeps that annotation honest -- the alternative was widening five fields to
``str | None`` to describe a case no committed row exhibits. The pre-CUI-0011
readers produced ``None`` here, so this is a deliberate narrowing, accepted on
CUI-0014 and pinned by ``TestMissingStringColumnsReadAsEmptyText``.

It applies to absent *readings* only. The date column is the row's identity
rather than a reading, so it is read with ``row[RAW_DATE_COLUMN]`` and a missing
one raises ``KeyError`` -- see :meth:`HourlyWeather.from_raw_row`.
"""


@dataclass
class HourlyWeather:
    """One hourly observation scraped from freemeteo.hk.

    Attributes:
        date: Observation date ``YYYY-MM-DD``.
        time: Observation time ``HH:MM`` (local).
        temperature_c: Air temperature in Celsius.
        wind_kmh: Wind speed in km/h.
        humidity_pct: Relative humidity in percent.
        description: Human-readable sky condition (e.g. ``"Rain"``).
    """

    date: str  # 'YYYY-MM-DD'
    time: str  # 'HH:MM'
    temperature_c: float | None
    wind_kmh: float | None
    humidity_pct: float | None
    description: str

    def to_raw_row(self) -> dict:
        """Return this observation as a ``weather_history.json`` row."""
        return {
            RAW_DATE_COLUMN: self.date,
            RAW_HOURLY_TIME_COLUMN: self.time,
            _HOURLY_TEMPERATURE_COLUMN: self.temperature_c,
            _HOURLY_WIND_COLUMN: self.wind_kmh,
            _HOURLY_HUMIDITY_COLUMN: self.humidity_pct,
            _HOURLY_DESCRIPTION_COLUMN: self.description,
        }

    @classmethod
    def from_raw_row(cls, row: dict) -> "HourlyWeather":
        """Build an observation from a ``weather_history.json`` row.

        Readings go through :func:`to_float` because the committed file spells
        the same column both ways -- ``Temperature (°C)`` is a JSON number in
        17,938 rows and a string in 46 -- and a fresh collection writes floats.

        A missing ``Time`` or ``Description`` reads as :data:`_MISSING_TEXT`
        (``""``), not ``None``: the fields are annotated ``str`` and stay that
        way. This differs from the pre-CUI-0011 reader, which produced ``None``
        -- a deliberate narrowing accepted on CUI-0014, where all 17,984
        committed hourly rows were found to share one key set, so no real row
        reaches it. ``""`` and ``None`` are not interchangeable downstream
        (``value is None`` catches only one, and JSON writes ``""`` vs ``null``),
        hence the explicit note and the tests that pin it.

        ``Date`` is the exception and is read with ``row[...]``: it is the row's
        identity, the key every export record and day lookup joins on, so a row
        without one is broken data. It raises ``KeyError`` by design rather than
        defaulting to ``""`` and quietly producing a record keyed on empty text.
        """
        return cls(
            date=row[RAW_DATE_COLUMN],
            time=row.get(RAW_HOURLY_TIME_COLUMN, _MISSING_TEXT),
            temperature_c=to_float(row.get(_HOURLY_TEMPERATURE_COLUMN)),
            wind_kmh=to_float(row.get(_HOURLY_WIND_COLUMN)),
            humidity_pct=to_float(row.get(_HOURLY_HUMIDITY_COLUMN)),
            description=row.get(_HOURLY_DESCRIPTION_COLUMN, _MISSING_TEXT),
        )


@dataclass
class WeatherWarning:
    """One Hong Kong Observatory warning or tropical cyclone signal.

    Attributes:
        date: The day the warning was queried for, ``YYYY-MM-DD``.
        warning_type: Category from the HKO legend (e.g. ``"Rainstorm"``).
        warning_signal: Signal code in upper case (e.g. ``"AMBER"``, ``"T8"``).
        start_time: Local start timestamp ``YYYY-MM-DD HH:MM:SS``.
        end_time: Local end timestamp ``YYYY-MM-DD HH:MM:SS``.
        icon_url: Relative URL of the HKO signal icon.
    """

    date: str
    warning_type: str
    warning_signal: str
    start_time: str
    end_time: str
    icon_url: str

    def to_raw_row(self) -> dict:
        """Return this warning as a ``weather_warning_history.json`` row."""
        return {
            RAW_DATE_COLUMN: self.date,
            _WARNING_TYPE_COLUMN: self.warning_type,
            RAW_WARNING_SIGNAL_COLUMN: self.warning_signal,
            _WARNING_START_COLUMN: self.start_time,
            _WARNING_END_COLUMN: self.end_time,
            _WARNING_ICON_COLUMN: self.icon_url,
        }

    @classmethod
    def from_raw_row(cls, row: dict) -> "WeatherWarning":
        """Build a warning from a ``weather_warning_history.json`` row.

        All five text columns default to :data:`_MISSING_TEXT` (``""``) when
        absent, not to ``None``, keeping the ``str`` annotations honest. The
        pre-CUI-0011 reader produced ``None``; the narrowing was accepted on
        CUI-0014 after all 461 committed warning rows were found to carry every
        column, and ``TestMissingStringColumnsReadAsEmptyText`` pins it so it
        cannot drift back silently.

        ``Date`` is deliberately read with ``row[...]`` and raises ``KeyError``
        when absent -- see :meth:`HourlyWeather.from_raw_row` for why the key
        column fails fast while the readings degrade.
        """
        return cls(
            date=row[RAW_DATE_COLUMN],
            warning_type=row.get(_WARNING_TYPE_COLUMN, _MISSING_TEXT),
            warning_signal=row.get(RAW_WARNING_SIGNAL_COLUMN, _MISSING_TEXT),
            start_time=row.get(_WARNING_START_COLUMN, _MISSING_TEXT),
            end_time=row.get(_WARNING_END_COLUMN, _MISSING_TEXT),
            icon_url=row.get(_WARNING_ICON_COLUMN, _MISSING_TEXT),
        )


@dataclass
class DailyWeather:
    """One row of the HKO daily weather extract.

    Attributes:
        date: Observation date ``YYYY-MM-DD``.
        mean_temp_c: Mean air temperature in Celsius.
        max_temp_c: Maximum air temperature in Celsius.
        min_temp_c: Minimum air temperature in Celsius.
        mean_humidity_pct: Mean relative humidity in percent.
        total_rainfall_mm: Total rainfall in millimetres.
        mean_wind_kmh: Mean wind speed in km/h.
    """

    date: str
    mean_temp_c: float | None = None
    max_temp_c: float | None = None
    min_temp_c: float | None = None
    mean_humidity_pct: float | None = None
    total_rainfall_mm: float | None = None
    mean_wind_kmh: float | None = None

    def to_raw_row(self) -> dict:
        """Return this day as an ``hko_daily_weather_extract.json`` row.

        The committed file carries six further columns -- ``Sunrise``,
        ``Solar Noon``, ``Sunset``, ``Moonrise``, ``Moon Transit``, ``Moonset``
        -- which this pair deliberately neither writes nor reads. They are not
        HKO daily-extract readings: the legacy pipeline joined them in from a
        second scrape, and the two copies still disagree: on 2021-01-01 the
        daily extract says ``Sunrise`` ``07:03`` while
        ``sun_moon_rise_set_history.json`` says ``07:02``. :class:`SunMoon`
        already declares them, so restating them on :class:`DailyWeather` would
        recreate the two-owners-one-field problem CUI-0011 exists to end, and
        :mod:`run365days.weather.collectors.hko_daily` could not fill them
        anyway. A re-collection therefore drops those six columns rather than
        overwriting good history with nulls.

        Since AU-037 they have a writer of their own,
        :mod:`run365days.weather.collectors.sun_moon`, so re-collecting no longer
        loses them from ``sun_moon_rise_set_history.json``. That does not make
        the export safe: ``export.records.daily_weather_record`` still reads
        ``sunrise`` and ``sunset`` off *this* row, so re-collect the daily
        extract and both still degrade to ``None``. Reading them from the
        sun/moon file instead would also change the exported values, the two
        copies disagreeing by a minute on many days, and was left out of AU-037
        as a decision about the export rather than about this pair.
        """
        return {
            RAW_DATE_COLUMN: self.date,
            _DAILY_MAX_TEMP_COLUMN: self.max_temp_c,
            _DAILY_MEAN_TEMP_COLUMN: self.mean_temp_c,
            _DAILY_MIN_TEMP_COLUMN: self.min_temp_c,
            _DAILY_HUMIDITY_COLUMN: self.mean_humidity_pct,
            _DAILY_RAINFALL_COLUMN: self.total_rainfall_mm,
            _DAILY_WIND_COLUMN: self.mean_wind_kmh,
        }

    @classmethod
    def from_raw_row(cls, row: dict) -> "DailyWeather":
        """Build a day from an ``hko_daily_weather_extract.json`` row.

        Readings go through :func:`to_float`, so HKO's non-numeric placeholders
        (``"Trace"`` for immeasurable rainfall) read as ``None``. The joined
        sun/moon columns are ignored -- see :meth:`to_raw_row`.

        :data:`_MISSING_TEXT` does not arise here: every reading this class
        declares is ``float | None``, so a missing one already reads as ``None``
        and there is no string field to default. ``Date`` fails fast on ``[...]``
        exactly as on the other two classes -- see
        :meth:`HourlyWeather.from_raw_row` (CUI-0014).
        """
        return cls(
            date=row[RAW_DATE_COLUMN],
            mean_temp_c=to_float(row.get(_DAILY_MEAN_TEMP_COLUMN)),
            max_temp_c=to_float(row.get(_DAILY_MAX_TEMP_COLUMN)),
            min_temp_c=to_float(row.get(_DAILY_MIN_TEMP_COLUMN)),
            mean_humidity_pct=to_float(row.get(_DAILY_HUMIDITY_COLUMN)),
            total_rainfall_mm=to_float(row.get(_DAILY_RAINFALL_COLUMN)),
            mean_wind_kmh=to_float(row.get(_DAILY_WIND_COLUMN)),
        )


@dataclass
class SunMoon:
    """Sunrise, sunset and moon times for one day.

    Attributes:
        date: Calendar date ``YYYY-MM-DD``.
        sunrise: Local sunrise time ``HH:MM``.
        sunset: Local sunset time ``HH:MM``.
        solar_noon: Local solar noon ``HH:MM``.
        day_length: Day length ``HH:MM:SS``.
        moonrise: Local moonrise time, if the moon rises that day.
        moon_transit: Local meridian passing, if the moon transits that day.
        moonset: Local moonset time, if the moon sets that day.
        moon_illumination_pct: Illuminated fraction of the moon in percent.
    """

    date: str
    sunrise: str
    sunset: str
    solar_noon: str
    day_length: str
    moonrise: str | None = None
    moon_transit: str | None = None
    moonset: str | None = None
    moon_illumination_pct: float | None = None

    def to_raw_row(self) -> dict:
        """Return this day as a ``sun_moon_rise_set_history.json`` row.

        All nine committed columns, ``Moon Transit`` included. The field was
        added to this dataclass to write it: the collector exists so that a
        re-collection stops losing history, and a writer that dropped a column
        the file already carries would do exactly that (AU-037).
        """
        return {
            RAW_DATE_COLUMN: self.date,
            SUN_MOON_SUNRISE_COLUMN: self.sunrise,
            _SUN_MOON_SOLAR_NOON_COLUMN: self.solar_noon,
            SUN_MOON_SUNSET_COLUMN: self.sunset,
            _SUN_MOON_DAY_LENGTH_COLUMN: self.day_length,
            _SUN_MOON_MOONRISE_COLUMN: _moon_event_text(self.moonrise),
            _SUN_MOON_MOON_TRANSIT_COLUMN: _moon_event_text(self.moon_transit),
            _SUN_MOON_MOONSET_COLUMN: _moon_event_text(self.moonset),
            _SUN_MOON_ILLUMINATION_COLUMN: _illumination_text(self.moon_illumination_pct),
        }

    @classmethod
    def from_raw_row(cls, row: dict) -> "SunMoon":
        """Build a day from a ``sun_moon_rise_set_history.json`` row.

        The four sun columns are plain text and read as :data:`_MISSING_TEXT`
        when absent, like every other string column here (CUI-0014). The moon
        columns answer ``None`` instead, both for a column the row does not
        carry and for the :data:`_MOON_EVENT_ABSENT` marker -- those are the
        same fact, so they read the same way. ``Date`` fails fast on ``[...]``
        as on the other classes -- see :meth:`HourlyWeather.from_raw_row`.
        """
        return cls(
            date=row[RAW_DATE_COLUMN],
            sunrise=row.get(SUN_MOON_SUNRISE_COLUMN, _MISSING_TEXT),
            sunset=row.get(SUN_MOON_SUNSET_COLUMN, _MISSING_TEXT),
            solar_noon=row.get(_SUN_MOON_SOLAR_NOON_COLUMN, _MISSING_TEXT),
            day_length=row.get(_SUN_MOON_DAY_LENGTH_COLUMN, _MISSING_TEXT),
            moonrise=_moon_event(row.get(_SUN_MOON_MOONRISE_COLUMN)),
            moon_transit=_moon_event(row.get(_SUN_MOON_MOON_TRANSIT_COLUMN)),
            moonset=_moon_event(row.get(_SUN_MOON_MOONSET_COLUMN)),
            moon_illumination_pct=_illumination(row.get(_SUN_MOON_ILLUMINATION_COLUMN)),
        )


def _moon_event(value: object) -> str | None:
    """Read one moon-event cell, mapping the file's absent marker to ``None``."""
    if value is None or value == _MOON_EVENT_ABSENT:
        return None
    return str(value)


def _moon_event_text(value: str | None) -> str:
    """Write one moon-event cell, spelling ``None`` the way the file does."""
    return _MOON_EVENT_ABSENT if value is None else value


def _illumination(value: object) -> float | None:
    """Read ``Illumination`` as a number, dropping the percent sign it carries."""
    if isinstance(value, str):
        return to_float(value.removesuffix(_ILLUMINATION_SUFFIX))
    return to_float(value)


def _illumination_text(value: float | None) -> str:
    """Write ``Illumination`` back in the file's ``"97.2%"`` form."""
    if value is None:
        return _MOON_EVENT_ABSENT
    return f"{value:.{_ILLUMINATION_DECIMALS}f}{_ILLUMINATION_SUFFIX}"
