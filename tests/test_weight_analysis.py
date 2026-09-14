import subprocess
import sys
from datetime import datetime

import pytest

from run365days.common.config import BODY_HEIGHT_CM, LBS_TO_KG
from run365days.weight import analysis
from run365days.weight.analysis import parse_weight_file

# What the API already refuses to load (tests/test_api_imports.py). The weight
# package is held to the same list so the two cannot drift into disagreeing
# about which dependencies count as the parsing stack.
PARSING_STACK = ("pandas", "numpy", "lxml", "bs4", "requests")

_SAMPLE_WEIGHT_DATA = """\
154.8 lbs (1/1)
153.6 lbs (2/1)
154.2 lbs (3/1)
152.0 lbs (4/1)
151.5 lbs (5/1)
"""


@pytest.fixture
def weight_file(tmp_path):
    f = tmp_path / "2021_DailyWeight.txt"
    f.write_text(_SAMPLE_WEIGHT_DATA)
    return f


class TestParseWeightFile:
    def test_parses_all_records(self, weight_file):
        records = parse_weight_file(weight_file, year=2021)
        assert len(records) == 5

    def test_first_record(self, weight_file):
        records = parse_weight_file(weight_file, year=2021)
        r = records[0]
        assert r.weight_lbs == 154.8
        assert r.date == "2021-01-01"
        # Exact, not approximate: the old assertion allowed 0.1% either way,
        # which is wider than the gap between the 0.454 the parser used and the
        # exact factor, so it passed on the wrong number for as long as it stood.
        assert r.weight_kg == round(154.8 * LBS_TO_KG, 2)

    def test_bmi_calculation(self, weight_file):
        records = parse_weight_file(weight_file, year=2021, height_cm=170)
        r = records[0]
        assert r.bmi == round((154.8 * LBS_TO_KG) / 1.70**2, 2)

    def test_uses_the_configured_height_when_none_is_given(self, weight_file):
        default = parse_weight_file(weight_file, year=2021)
        explicit = parse_weight_file(weight_file, year=2021, height_cm=BODY_HEIGHT_CM)
        assert [r.bmi for r in default] == [r.bmi for r in explicit]

    def test_converts_pounds_at_the_international_definition(self):
        assert LBS_TO_KG == 0.45359237

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        assert parse_weight_file(f, year=2021) == []

    def test_ignores_non_matching_lines(self, tmp_path):
        f = tmp_path / "partial.txt"
        f.write_text("some random text\n154.8 lbs (1/1)\nmore text\n")
        records = parse_weight_file(f, year=2021)
        assert len(records) == 1
        # Counting skipped lines would make this weigh-in the second one.
        assert records[0].day_number == 1

    def test_numbers_weigh_ins_rather_than_file_lines(self, tmp_path):
        f = tmp_path / "noisy.txt"
        f.write_text("# exported from the scale app\n154.8 lbs (1/1)\n\n153.6 lbs (2/1)\n")
        records = parse_weight_file(f, year=2021)
        assert [r.day_number for r in records] == [1, 2]
        assert [r.date for r in records] == ["2021-01-01", "2021-01-02"]

    def test_numbers_stay_consecutive_across_a_gap_in_the_calendar(self, tmp_path):
        f = tmp_path / "gap.txt"
        f.write_text("154.8 lbs (1/1)\n153.6 lbs (9/3)\n")
        records = parse_weight_file(f, year=2021)
        assert [r.day_number for r in records] == [1, 2]

    def test_dates_an_undated_final_weigh_in_to_the_day_after(self, tmp_path):
        f = tmp_path / "trailing.txt"
        f.write_text("154.8 lbs (1/1)\n153.6 lbs\n")
        records = parse_weight_file(f, year=2021)
        assert [r.date for r in records] == ["2021-01-01", "2021-01-02"]

    def test_skips_an_undated_weigh_in_with_no_dated_record_before_it(self, tmp_path):
        # Nothing to count from, so the line is not a weigh-in at all.
        f = tmp_path / "headless.txt"
        f.write_text("153.6 lbs\n154.8 lbs (1/1)\n")
        records = parse_weight_file(f, year=2021)
        assert [r.date for r in records] == ["2021-01-01"]

    def test_dates_against_the_current_year_when_none_is_given(self, tmp_path):
        # The default is read at call time, so it is pinned against the clock
        # rather than a literal year that would rot every January.
        f = tmp_path / "undated_year.txt"
        f.write_text("154.8 lbs (1/1)\n")
        records = parse_weight_file(f)
        assert records[0].date == f"{datetime.today().year}-01-01"


class TestPublicSurface:
    """AU-029: ``weight`` publishes what production calls, and nothing else.

    ``build_dataframe``, ``monthly_summary``, ``weekday_summary`` and
    ``describe_weight`` were the ported half of
    ``legacy/05_GetDailyWeightSummary.py`` that only ever printed to a console
    and drew matplotlib figures. They were exported as package API, and in that
    shape they had no caller at all: ``cli/export_data.py`` imports
    ``parse_weight_file`` and nothing else, three of the four had no test
    either, and the fourth was reachable only from its own tests.

    They were also the whole reason this package needed pandas and numpy, so
    dropping them is a real narrowing rather than a tidy-up -- the same trade
    AU-023 made when it deleted ``activities/metrics.py`` outright.
    """

    def test_the_module_publishes_only_what_production_calls(self):
        assert analysis.__all__ == ["WeightRecord", "parse_weight_file"]

    def test_importing_the_weight_package_loads_no_parsing_dependency(self):
        # A subprocess, not sys.modules: this test session has pandas loaded
        # long before it gets here, so only a fresh interpreter can answer.
        code = (
            "import sys; import run365days.weight.analysis, run365days.weight.models; "
            f"heavy = [m for m in {PARSING_STACK!r} if m in sys.modules]; "
            "print(','.join(heavy))"
        )
        # S603: argument list is a literal, the executable is sys.executable, and
        # no shell is involved -- there is no untrusted input to inject through.
        out = subprocess.run(  # noqa: S603
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        )
        assert out.stdout.strip() == "", f"weight import loaded: {out.stdout.strip()}"
