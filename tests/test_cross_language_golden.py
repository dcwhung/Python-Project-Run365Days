"""Cross-language conformance gate for the duplicated year-stats logic (AU-009).

`src/analytics/stats.py` and `frontend/src/data/stats.ts` are two hand-written
implementations of one specification, and so are `analytics.builder.downsample`
and `frontend/src/lib/downsample.ts`. Until this file existed, the only thing
holding them together was that two separately hand-typed fixtures happened to
pin the same numbers, so changing a formula on one side and not the other left
both suites green while the two data modes showed different figures.

How the gate works
------------------
`golden/cases.json` holds the *inputs* -- hand-authored, because an input
encodes intent about which branch it reaches. `golden/expected.tsv` holds the
*outputs* as ``key<TAB>canonical-value`` lines and is machine-generated.

This file recomputes every value from the Python implementation and compares it
to `expected.tsv`. `frontend/src/test/golden.test.ts` recomputes the same keys
from the TypeScript implementation and compares them to the same file. Neither
side owns the golden, so a change to either implementation alone turns that
side red. That is the bidirectionality the audit asked for: a golden generated
by one language and merely checked by the other would only ever protect one of
them.

Regeneration is deliberately not a way out. `UPDATE_GOLDEN=1 pytest` rewrites
`expected.tsv` from Python and `UPDATE_GOLDEN=1 npm test` rewrites it from
TypeScript, but the file is shared: regenerating from one language leaves the
*other* language's suite red on exactly the keys that disagree. There is no
single command that makes both green, so a divergence cannot be regenerated
away -- it can only be fixed, or written down.

Written down means `golden/divergences.tsv`, a hand-maintained ledger of known
disagreements carrying both languages' current values. Nothing generates it.
Each side asserts that a ledgered key still produces *its own* recorded value,
so a ledger entry is not a mute: removing a divergence by fixing one side turns
that side red until the ledger line is deleted, and a new divergence cannot be
absorbed by any tool. Adding a line is a reviewable diff that names the
function, the input and both numbers.

No CI job was added for this, deliberately. The audit suggested one, but both
halves already run on every push and pull request -- this file inside `pytest
tests` in the lint-test job, `golden.test.ts` inside `npm test` in the frontend
job, both from the same checkout. If both are green then Python equals the
golden and TypeScript equals the golden, so Python equals TypeScript by
transitivity; a third job would re-run the same comparison and would need both
toolchains installed to do it. A check that restates a check is a copy that can
drift, which is the defect this ticket is about.

Float comparison is exact, on a canonical shortest-round-trip decimal string.
No tolerance: every value these functions return is either an integer or has
already been rounded to two decimals by the implementation itself, so there is
no genuine floating-point noise for a tolerance to absorb -- only real
disagreement, which is what a tolerance would hide (AU-008).
"""

import hashlib
import json
import math
import os
from datetime import date
from pathlib import Path

import pytest

from run365days.analytics import builder, stats

GOLDEN_DIR = Path(__file__).parent / "golden"
CASES_FILE = GOLDEN_DIR / "cases.json"
EXPECTED_FILE = GOLDEN_DIR / "expected.tsv"
DIVERGENCES_FILE = GOLDEN_DIR / "divergences.tsv"

LANGUAGE_COLUMN = 0
"""Which column of divergences.tsv (after the key) this side must match."""

_TWO_POW_53 = 2**53


def canon(value: object) -> str:
    """Render one output value as the canonical text both languages must produce.

    Integral floats collapse onto integers (Python writes ``23.0`` where
    JavaScript writes ``23`` for the same double, and the contract here is
    numeric, not typed). Everything else uses the shortest decimal that
    round-trips, which Python's ``repr`` and JavaScript's ``String`` both
    produce and which is a bijection with the underlying double -- so equality
    of these strings *is* bit equality, with no tolerance anywhere.

    Exponent notation is rejected rather than emitted: the two languages spell
    it differently (``1e-07`` against ``1e-7``), and no value in this domain
    needs it, so a value that reached for it would be a silent formatting
    divergence rather than a real one.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        if "\t" in value or "\n" in value:
            raise AssertionError(f"value is not tab-safe: {value!r}")
        return value
    if isinstance(value, list):
        return ",".join(canon(v) for v in value)
    number = float(value)
    if not math.isfinite(number):
        raise AssertionError(f"non-finite value reached the golden: {value!r}")
    if number.is_integer() and abs(number) < _TWO_POW_53:
        return str(int(number))
    text = repr(number)
    if "e" in text or "E" in text:
        raise AssertionError(f"value needs exponent notation, which is not canonical: {text}")
    return text


def digest(lines: list[str]) -> str:
    """Hash a full series so a 365-row array costs one golden line, not 1095."""
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


class Emitter:
    """Collects ``key -> canonical value`` pairs in emission order."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def put(self, key: str, value: object) -> None:
        """Record one value, refusing a duplicate key."""
        if key in self.values:
            raise AssertionError(f"duplicate golden key: {key}")
        self.values[key] = canon(value)

    def row(self, prefix: str, row: dict, fields: tuple[str, ...]) -> None:
        """Record one dict under *prefix*, naming every field explicitly.

        The field list is written out rather than derived from the dict so that
        renaming or dropping a field on either side changes the key set and
        fails, instead of quietly renaming a golden line on both sides at once.
        """
        for field in fields:
            self.put(f"{prefix}.{field}", row[field])

    def series(
        self, prefix: str, rows: list[dict], fields: tuple[str, ...], probes: list[int]
    ) -> None:
        """Record a long per-day series as length + digest + a few full rows.

        A year of ``daily_distance`` and ``training_load`` is 730 rows and would
        dominate the golden while saying almost nothing per line. The digest
        covers every row exactly, so any change anywhere is still red; the
        probe rows named by the case give a readable diff at the indices that
        actually carry a branch (1 January, 29 February, 31 December, the first
        days of the CTL warm-up).
        """
        self.put(f"{prefix}.len", len(rows))
        flat = [f"{i}.{f}\t{canon(r[f])}" for i, r in enumerate(rows) for f in fields]
        self.put(f"{prefix}.digest", digest(flat))
        for i in probes:
            if i < len(rows):
                self.row(f"{prefix}[{i}]", rows[i], fields)


TOTALS_FIELDS = (
    "runs",
    "days",
    "active_days",
    "distance_km",
    "duration_sec",
    "calories",
    "avg_pace_sec_per_km",
    "avg_cadence",
    "avg_distance_km",
)
MONTHLY_FIELDS = (
    "month",
    "runs",
    "distance_km",
    "duration_sec",
    "calories",
    "avg_pace_sec_per_km",
    "avg_cadence",
    "best_pace_sec_per_km",
    "best_pace_activity_id",
)
WEEKLY_FIELDS = (
    "week",
    "week_start",
    "runs",
    "distance_km",
    "duration_sec",
    "avg_pace_sec_per_km",
    "longest_km",
    "activity_ids",
)
DAILY_FIELDS = ("date", "distance_km", "activity_id")
LOAD_FIELDS = ("date", "ctl", "atl", "tsb")
PB_FIELDS = ("longest", "fastest", "longest_time", "most_calories", "top_cadence")


def emit(cases: dict) -> dict[str, str]:
    """Run every case through the Python implementation."""
    out = Emitter()
    out.put("meta/cases_sha256", hashlib.sha256(CASES_FILE.read_bytes()).hexdigest())

    for year in cases["days_in_year"]:
        out.put(f"days_in_year/{year}", stats.days_in_year(year))

    for probe in cases["week_index"]:
        year = probe["year"]
        for day in probe["dates"]:
            out.put(f"week_index/{year}/{day}", stats.week_index(date.fromisoformat(day), year))

    for case in cases["stats"]:
        cid, year, acts = case["id"], case["year"], case["activities"]
        prefix = f"stats/{cid}"

        out.row(f"{prefix}/totals", stats.totals(acts, year), TOTALS_FIELDS)

        months = stats.monthly(acts)
        out.put(f"{prefix}/monthly.len", len(months))
        for i, month in enumerate(months):
            out.row(f"{prefix}/monthly[{i}]", month, MONTHLY_FIELDS)

        weeks = stats.weekly(acts, year)
        out.put(f"{prefix}/weekly.len", len(weeks))
        for i, week in enumerate(weeks):
            out.row(f"{prefix}/weekly[{i}]", week, WEEKLY_FIELDS)

        daily = stats.daily_distance(acts, year)
        out.series(f"{prefix}/daily_distance", daily, DAILY_FIELDS, case["probe_days"])
        out.series(
            f"{prefix}/training_load", stats.training_load(daily), LOAD_FIELDS, case["probe_days"]
        )

        bests = stats.personal_bests(acts)
        for field in PB_FIELDS:
            best = bests[field]
            out.put(f"{prefix}/personal_bests.{field}", best["id"] if best else None)

    for case in cases["training_load"]:
        daily = [{"date": r["date"], "distance_km": r["distance_km"]} for r in case["daily"]]
        rows = stats.training_load(daily)
        out.put(f"training_load/{case['id']}.len", len(rows))
        for i, row in enumerate(rows):
            out.row(f"training_load/{case['id']}[{i}]", row, LOAD_FIELDS)

    for case in cases["downsample"]:
        picked = builder.downsample(list(range(case["n"])), case["limit"])
        out.put(f"downsample/{case['id']}", picked)

    return out.values


def read_tsv(path: Path, columns: int) -> dict[str, list[str]]:
    """Read a golden or ledger file as ``key -> remaining columns``."""
    rows: dict[str, list[str]] = {}
    if not path.exists():
        return rows
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < columns:
            raise AssertionError(f"{path.name}:{number}: expected {columns} tab-separated columns")
        rows[parts[0]] = parts[1:]
    return rows


@pytest.fixture(scope="module")
def cases() -> dict:
    return json.loads(CASES_FILE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def emitted(cases) -> dict[str, str]:
    return emit(cases)


@pytest.fixture(scope="module")
def divergences() -> dict[str, list[str]]:
    return read_tsv(DIVERGENCES_FILE, 4)


def _write_expected(emitted: dict[str, str], divergences: dict[str, list[str]]) -> None:
    body = "".join(
        f"{key}\t{value}\n" for key, value in sorted(emitted.items()) if key not in divergences
    )
    EXPECTED_FILE.write_text(body, encoding="utf-8")


def test_golden_matches_this_implementation(emitted, divergences) -> None:
    if os.environ.get("UPDATE_GOLDEN"):
        _write_expected(emitted, divergences)
        pytest.skip("UPDATE_GOLDEN set: expected.tsv rewritten from the Python implementation")

    expected = {key: cols[0] for key, cols in read_tsv(EXPECTED_FILE, 2).items()}
    shared = set(emitted) - set(divergences)

    assert set(expected) == shared, (
        "golden key set has drifted -- "
        f"missing from expected.tsv: {sorted(shared - set(expected))[:10]}; "
        f"stale in expected.tsv: {sorted(set(expected) - shared)[:10]}"
    )
    mismatches = {
        key: (expected[key], emitted[key])
        for key in sorted(shared)
        if expected[key] != emitted[key]
    }
    assert not mismatches, (
        f"{len(mismatches)} value(s) differ from golden/expected.tsv. Each one is either a "
        "Python-side change that TypeScript has not made, or a fix that both sides now agree "
        f"on and the golden has not caught up with. First few: {list(mismatches.items())[:5]}"
    )


def test_every_ledgered_divergence_still_diverges(emitted, divergences) -> None:
    """A ledger line is a claim about both languages; hold Python to its half.

    Fixing the divergence must turn this red, so that the ledger cannot outlive
    the bug it records and quietly keep excusing a key that now agrees.
    """
    assert divergences, "the divergence ledger is empty -- delete it rather than leave a stub"
    unknown = sorted(set(divergences) - set(emitted))
    assert not unknown, f"ledger names keys nothing emits: {unknown}"

    stale = {
        key: (cols[LANGUAGE_COLUMN], emitted[key])
        for key, cols in sorted(divergences.items())
        if emitted[key] != cols[LANGUAGE_COLUMN]
    }
    assert not stale, (
        "ledgered keys no longer produce their recorded Python value. If the implementation was "
        f"fixed, delete the ledger line (and regenerate the golden). Offenders: {stale}"
    )
    agreeing = sorted(key for key, cols in divergences.items() if cols[0] == cols[1])
    assert not agreeing, f"ledger lines whose two columns are equal are not divergences: {agreeing}"


def test_expected_and_ledger_are_disjoint(divergences) -> None:
    expected = read_tsv(EXPECTED_FILE, 2)
    overlap = sorted(set(expected) & set(divergences))
    assert not overlap, f"a key cannot be both agreed and divergent: {overlap}"


def test_canon_is_exact_and_refuses_ambiguous_forms() -> None:
    assert canon(23.0) == "23"
    assert canon(23) == "23"
    assert canon(6.125) == "6.125"
    assert canon(0.1 + 0.2) == "0.30000000000000004"
    assert canon(None) == "null"
    assert canon(True) == "true"
    assert canon(["a", "b"]) == "a,b"
    assert canon([]) == ""
    for bad in (float("nan"), float("inf")):
        with pytest.raises(AssertionError):
            canon(bad)
    with pytest.raises(AssertionError):
        canon(1e-7)
    with pytest.raises(AssertionError):
        canon("has\ttab")
