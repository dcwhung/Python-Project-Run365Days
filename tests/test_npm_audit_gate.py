"""The npm audit gate: zero advisories, except ones a ticket has allowlisted for a while."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
GATE_PATH = REPO_ROOT / "scripts" / "npm_audit_gate.py"
ALLOWLIST_PATH = REPO_ROOT / "frontend" / "npm-audit-allowlist.json"

TODAY = date(2026, 10, 5)
GHSA = "GHSA-vfj7-8cjw-p6xm"
OTHER_GHSA = "GHSA-aaaa-bbbb-cccc"


def _load_gate():
    spec = importlib.util.spec_from_file_location("npm_audit_gate", GATE_PATH)
    module = importlib.util.module_from_spec(spec)
    # Registered before executing: dataclasses resolve string annotations through it.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate = _load_gate()


def _advisory(package: str, ghsa: str) -> dict:
    return {
        "source": 1,
        "name": package,
        "dependency": package,
        "title": f"{package} is vulnerable",
        "url": f"https://github.com/advisories/{ghsa}",
        "severity": "high",
        "range": "*",
    }


def _vuln(package: str, via: list, fix=False) -> dict:
    return {"name": package, "severity": "high", "via": via, "fixAvailable": fix}


def _report(*vulns: dict) -> dict:
    return {
        "auditReportVersion": 2,
        "vulnerabilities": {v["name"]: v for v in vulns},
        "metadata": {"vulnerabilities": {"total": len(vulns)}},
    }


def _braces_report(braces_fix=False, extra: tuple = ()) -> dict:
    """braces carries the advisory; micromatch and fast-glob are flagged through it."""
    return _report(
        _vuln("braces", [_advisory("braces", GHSA)], fix=braces_fix),
        _vuln("micromatch", ["braces"]),
        _vuln("fast-glob", ["micromatch"]),
        *extra,
    )


def _entry(**overrides) -> dict:
    entry = {
        "id": GHSA,
        "package": "braces",
        "ticket": "CUI-0054",
        "expires": (TODAY + timedelta(days=30)).isoformat(),
        "reason": "No fixed version exists; reached only through dev tooling.",
        "dependents": ["fast-glob", "micromatch"],
    }
    entry.update(overrides)
    return entry


def _allowlist(*entries: dict) -> dict:
    return gate.load_allowlist({"advisories": list(entries)})


def _problems(report: dict, allowlist: dict) -> list[str]:
    return gate.evaluate(report, allowlist, TODAY).problems


# --- the zero-advisory baseline is unchanged ------------------------------------------------


def test_a_clean_tree_with_an_empty_allowlist_passes():
    verdict = gate.evaluate(_report(), _allowlist(), TODAY)
    assert verdict.problems == []
    assert verdict.accepted == []


def test_an_advisory_nobody_allowlisted_fails_and_names_itself():
    problems = _problems(_braces_report(), _allowlist())
    assert len(problems) == 1
    assert GHSA in problems[0]
    assert "braces" in problems[0]


# --- an allowlisted advisory passes only while every condition still holds ------------------


def test_an_allowlisted_advisory_with_its_exact_dependents_passes_but_is_reported():
    verdict = gate.evaluate(_braces_report(), _allowlist(_entry()), TODAY)
    assert verdict.problems == []
    assert len(verdict.accepted) == 1
    assert GHSA in verdict.accepted[0]
    assert "CUI-0054" in verdict.accepted[0]


def test_an_entry_past_its_expiry_fails():
    expired = _entry(expires=(TODAY - timedelta(days=1)).isoformat())
    problems = _problems(_braces_report(), _allowlist(expired))
    assert len(problems) == 1
    assert "expired" in problems[0]


def test_an_entry_on_its_expiry_day_still_passes():
    on_the_day = _entry(expires=TODAY.isoformat())
    assert _problems(_braces_report(), _allowlist(on_the_day)) == []


def test_an_entry_dated_beyond_the_review_horizon_fails():
    horizon = TODAY + timedelta(days=gate.MAX_ALLOWLIST_DAYS + 1)
    problems = _problems(_braces_report(), _allowlist(_entry(expires=horizon.isoformat())))
    assert len(problems) == 1
    assert str(gate.MAX_ALLOWLIST_DAYS) in problems[0]


def test_an_entry_dated_exactly_at_the_review_horizon_passes():
    horizon = TODAY + timedelta(days=gate.MAX_ALLOWLIST_DAYS)
    assert _problems(_braces_report(), _allowlist(_entry(expires=horizon.isoformat()))) == []


def test_an_entry_whose_advisory_is_no_longer_reported_fails_as_stale():
    problems = _problems(_report(), _allowlist(_entry()))
    assert len(problems) == 1
    assert "no longer reported" in problems[0]


def test_an_advisory_that_gains_a_non_breaking_fix_fails():
    problems = _problems(_braces_report(braces_fix=True), _allowlist(_entry()))
    assert len(problems) == 1
    assert "npm audit fix" in problems[0]


def test_a_fix_that_needs_a_major_change_elsewhere_does_not_count_as_a_fix():
    downgrade = {"name": "@graphql-codegen/cli", "version": "3.2.0", "isSemVerMajor": True}
    assert _problems(_braces_report(braces_fix=downgrade), _allowlist(_entry())) == []


def test_a_new_package_reaching_the_advisory_fails_as_a_new_path():
    report = _braces_report(extra=(_vuln("vite", ["micromatch"]),))
    problems = _problems(report, _allowlist(_entry()))
    assert len(problems) == 1
    assert "vite" in problems[0]


def test_a_listed_dependent_that_no_longer_reaches_the_advisory_fails():
    report = _report(
        _vuln("braces", [_advisory("braces", GHSA)]),
        _vuln("micromatch", ["braces"]),
    )
    problems = _problems(report, _allowlist(_entry()))
    assert len(problems) == 1
    assert "fast-glob" in problems[0]


def test_an_entry_naming_the_wrong_package_fails():
    problems = _problems(_braces_report(), _allowlist(_entry(package="micromatch")))
    assert len(problems) == 1
    assert "micromatch" in problems[0]


def test_an_allowlisted_advisory_does_not_hide_a_second_advisory_on_a_shared_path():
    report = _braces_report(
        extra=(_vuln("glob-parent", [_advisory("glob-parent", OTHER_GHSA)]),),
    )
    report["vulnerabilities"]["fast-glob"]["via"] = ["micromatch", "glob-parent"]
    problems = _problems(report, _allowlist(_entry()))
    assert len(problems) == 1
    assert OTHER_GHSA in problems[0]


def test_a_cycle_in_the_via_graph_terminates():
    report = _braces_report()
    report["vulnerabilities"]["micromatch"]["via"] = ["braces", "fast-glob"]
    assert _problems(report, _allowlist(_entry())) == []


# --- malformed input is an error, never a pass ----------------------------------------------


def test_an_npm_error_document_is_rejected_rather_than_read_as_clean():
    npm_error = {"message": "request failed, reason: ECONNREFUSED", "error": {"summary": ""}}
    with pytest.raises(gate.AuditInputError):
        gate.evaluate(npm_error, _allowlist(), TODAY)


def test_an_unknown_report_version_is_rejected():
    report = _report()
    report["auditReportVersion"] = 1
    with pytest.raises(gate.AuditInputError):
        gate.evaluate(report, _allowlist(), TODAY)


@pytest.mark.parametrize("missing", ["id", "package", "ticket", "expires", "reason", "dependents"])
def test_an_allowlist_entry_missing_a_field_is_rejected(missing):
    entry = _entry()
    del entry[missing]
    with pytest.raises(gate.AuditInputError):
        gate.load_allowlist({"advisories": [entry]})


@pytest.mark.parametrize(
    "field,value",
    [("id", "CVE-2024-0001"), ("expires", "next year"), ("dependents", "micromatch")],
)
def test_an_allowlist_entry_with_a_malformed_field_is_rejected(field, value):
    with pytest.raises(gate.AuditInputError):
        gate.load_allowlist({"advisories": [_entry(**{field: value})]})


def test_a_duplicated_allowlist_entry_is_rejected():
    with pytest.raises(gate.AuditInputError):
        gate.load_allowlist({"advisories": [_entry(), _entry()]})


# --- the command line ----------------------------------------------------------------------


def _write(tmp_path: Path, name: str, data: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def _run(tmp_path: Path, report: dict, allowlist: dict) -> int:
    return gate.main(
        [
            str(_write(tmp_path, "report.json", report)),
            str(_write(tmp_path, "allowlist.json", allowlist)),
            "--today",
            TODAY.isoformat(),
        ]
    )


def test_main_exits_zero_when_every_advisory_is_allowlisted(tmp_path, capsys):
    assert _run(tmp_path, _braces_report(), {"advisories": [_entry()]}) == 0
    assert "::warning" in capsys.readouterr().out


def test_main_exits_one_on_a_problem(tmp_path, capsys):
    assert _run(tmp_path, _braces_report(), {"advisories": []}) == 1
    assert "::error" in capsys.readouterr().out


def test_main_exits_two_on_input_it_cannot_read(tmp_path):
    assert _run(tmp_path, {"message": "boom", "error": {}}, {"advisories": []}) == 2


def test_main_exits_two_when_the_report_is_not_json(tmp_path):
    report = tmp_path / "report.json"
    report.write_text("npm ERR! something", encoding="utf-8")
    allowlist = _write(tmp_path, "allowlist.json", {"advisories": []})
    assert gate.main([str(report), str(allowlist), "--today", TODAY.isoformat()]) == 2


# --- the committed allowlist ---------------------------------------------------------------


def test_the_committed_allowlist_is_well_formed():
    data = json.loads(ALLOWLIST_PATH.read_text(encoding="utf-8"))
    for entry in gate.load_allowlist(data).values():
        assert entry.ticket.startswith("CUI-")
