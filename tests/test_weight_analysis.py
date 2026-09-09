import pytest

from run365days.weight.analysis import build_dataframe, parse_weight_file

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
        assert r.weight_kg == pytest.approx(154.8 * 0.454, rel=1e-3)

    def test_bmi_calculation(self, weight_file):
        records = parse_weight_file(weight_file, year=2021, height_cm=170)
        r = records[0]
        expected_bmi = (154.8 * 0.454) / (1.70**2)
        assert r.bmi == pytest.approx(expected_bmi, rel=1e-2)

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        assert parse_weight_file(f, year=2021) == []

    def test_ignores_non_matching_lines(self, tmp_path):
        f = tmp_path / "partial.txt"
        f.write_text("some random text\n154.8 lbs (1/1)\nmore text\n")
        records = parse_weight_file(f, year=2021)
        assert len(records) == 1


class TestBuildDataframe:
    def test_full_year_length(self, weight_file):
        records = parse_weight_file(weight_file, year=2021)
        df = build_dataframe(records, year=2021)
        assert len(df) == 365

    def test_plus_minus_column(self, weight_file):
        records = parse_weight_file(weight_file, year=2021)
        df = build_dataframe(records, year=2021)
        # day 1 to day 2: 154.8 -> 153.6 = decrease (-)
        assert df.iloc[1]["+/-"] == "-"
        # day 2 to day 3: 153.6 -> 154.2 = increase (+)
        assert df.iloc[2]["+/-"] == "+"
