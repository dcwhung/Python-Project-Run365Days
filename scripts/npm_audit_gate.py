"""Gate ``npm audit`` at zero advisories, except ones a ticket has allowlisted for a while.

Standard library only, so CI needs nothing installed to run it.

``npm audit`` on its own can only be held at zero or loosened wholesale. Zero is
the rule here (CUI-0049), but an advisory with no fixed version anywhere -- the
first one was GHSA-vfj7-8cjw-p6xm against ``braces``, whose affected range runs
up to the latest release -- leaves zero unreachable by anything a pull request
can do. ``--audit-level`` would let every future advisory at that severity
through as well, so CUI-0054 chose this instead: a list of single advisories,
each tied to a ticket, and each accepted only while all of these still hold:

- it has not passed its ``expires`` date, and that date is no more than
  :data:`MAX_ALLOWLIST_DAYS` ahead, so nothing is waved through indefinitely;
- npm still reports it, so a fixed entry fails until it is deleted;
- npm offers no non-breaking fix for it, so ``npm audit fix`` is never skipped;
- it sits in the package the entry names;
- the set of packages npm flags because of it is exactly the entry's
  ``dependents``, so a new route to it fails even though the advisory is old.

That last rule is the one an exclusion flag cannot express. The case for the
first allowlisting was that the only route to ``braces`` is
``@graphql-codegen/cli``, fed nothing but this repository's own glob patterns;
a new route would make that case unproven, so it fails.

W-034 and S-095 are both findings about allowlists drifting out of date. Every
rule above turns drift into a red run instead.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

SUPPORTED_REPORT_VERSION = 2
"""The ``auditReportVersion`` npm 7+ writes. Any other shape is refused, not guessed at."""

MAX_ALLOWLIST_DAYS = 90
"""How far ahead an entry may expire. Renewing is a fresh decision, not a default."""

REQUIRED_FIELDS = ("id", "package", "ticket", "expires", "reason", "dependents")
"""Every allowlist entry carries all of these."""

GHSA_PATTERN = re.compile(r"^GHSA-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}$")
"""A GitHub advisory id, the identifier ``npm audit`` reports in each advisory URL."""

EXIT_OK = 0
"""Every reported advisory is allowlisted and every entry still holds."""

EXIT_PROBLEMS = 1
"""At least one advisory or entry fails a rule."""

EXIT_BAD_INPUT = 2
"""The report or the allowlist could not be read. Never treated as a pass."""


class AuditInputError(ValueError):
    """The report or the allowlist is not in a shape this gate can judge."""


@dataclass(frozen=True)
class AllowlistEntry:
    """One advisory accepted for a bounded time, with the reason and the routes to it."""

    id: str
    package: str
    ticket: str
    expires: date
    reason: str
    dependents: frozenset[str]


@dataclass
class Verdict:
    """What the gate found: rule failures, and advisories it let through and why."""

    problems: list[str] = field(default_factory=list)
    accepted: list[str] = field(default_factory=list)


def _required_text(raw: dict, key: str) -> str:
    value = raw[key]
    if not isinstance(value, str) or not value.strip():
        raise AuditInputError(f"allowlist field {key!r} must be a non-empty string")
    return value


def _parse_entry(raw: object) -> AllowlistEntry:
    if not isinstance(raw, dict):
        raise AuditInputError("each allowlist entry must be an object")
    missing = [key for key in REQUIRED_FIELDS if key not in raw]
    if missing:
        raise AuditInputError(f"allowlist entry is missing {', '.join(missing)}")
    advisory_id = _required_text(raw, "id")
    if not GHSA_PATTERN.match(advisory_id):
        raise AuditInputError(f"allowlist id {advisory_id!r} is not a GHSA id")
    try:
        expires = date.fromisoformat(_required_text(raw, "expires"))
    except ValueError as exc:
        raise AuditInputError(f"{advisory_id}: expires is not an ISO date") from exc
    dependents = raw["dependents"]
    if not isinstance(dependents, list) or not all(isinstance(d, str) for d in dependents):
        raise AuditInputError(f"{advisory_id}: dependents must be a list of package names")
    return AllowlistEntry(
        id=advisory_id,
        package=_required_text(raw, "package"),
        ticket=_required_text(raw, "ticket"),
        expires=expires,
        reason=_required_text(raw, "reason"),
        dependents=frozenset(dependents),
    )


def load_allowlist(data: object) -> dict[str, AllowlistEntry]:
    """Parse an allowlist document into entries keyed by advisory id.

    Args:
        data: The decoded allowlist JSON, ``{"advisories": [...]}``.

    Returns:
        The entries, keyed by their GHSA id.

    Raises:
        AuditInputError: The document or any entry is malformed, or an id repeats.
    """
    if not isinstance(data, dict) or not isinstance(data.get("advisories"), list):
        raise AuditInputError('the allowlist must be an object with an "advisories" list')
    entries: dict[str, AllowlistEntry] = {}
    for raw in data["advisories"]:
        entry = _parse_entry(raw)
        if entry.id in entries:
            raise AuditInputError(f"{entry.id} is allowlisted twice")
        entries[entry.id] = entry
    return entries


def _vulnerabilities(report: object) -> dict[str, dict]:
    if not isinstance(report, dict) or "error" in report:
        raise AuditInputError("npm audit did not produce a report (it reported an error)")
    if report.get("auditReportVersion") != SUPPORTED_REPORT_VERSION:
        raise AuditInputError(
            f"unsupported auditReportVersion {report.get('auditReportVersion')!r}; "
            f"this gate reads version {SUPPORTED_REPORT_VERSION}"
        )
    vulns = report.get("vulnerabilities")
    if not isinstance(vulns, dict):
        raise AuditInputError('the report has no "vulnerabilities" object')
    return vulns


def _reported_advisories(vulns: dict[str, dict]) -> dict[str, set[str]]:
    """Map each advisory id npm reports to the packages it is reported against."""
    reported: dict[str, set[str]] = {}
    for name, vuln in vulns.items():
        for via in vuln.get("via", []):
            if isinstance(via, dict):
                advisory_id = str(via.get("url", "")).rstrip("/").rsplit("/", 1)[-1]
                reported.setdefault(advisory_id, set()).add(name)
    return reported


def _roots(vulns: dict[str, dict], start: str, reported: dict[str, set[str]]) -> set[str]:
    """Advisory ids *start* is flagged for, following package-name ``via`` links."""
    found: set[str] = set()
    seen: set[str] = set()
    pending = [start]
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        found.update(advisory for advisory, pkgs in reported.items() if name in pkgs)
        pending.extend(v for v in vulns.get(name, {}).get("via", []) if isinstance(v, str))
    return found


def _dependents(vulns: dict[str, dict], reported: dict[str, set[str]]) -> dict[str, set[str]]:
    """Map each advisory id to the packages flagged because of it, not those carrying it."""
    dependents: dict[str, set[str]] = {advisory: set() for advisory in reported}
    for name in vulns:
        for advisory in _roots(vulns, name, reported):
            if name not in reported[advisory]:
                dependents[advisory].add(name)
    return dependents


def _date_problems(entry: AllowlistEntry, today: date) -> list[str]:
    if today > entry.expires:
        return [f"{entry.id}: allowlist entry expired on {entry.expires} ({entry.ticket})"]
    if (entry.expires - today).days > MAX_ALLOWLIST_DAYS:
        return [
            f"{entry.id}: expires {entry.expires}, more than {MAX_ALLOWLIST_DAYS} days "
            f"ahead; renew in shorter steps"
        ]
    return []


def _path_problems(entry: AllowlistEntry, actual: set[str]) -> list[str]:
    problems = []
    added = sorted(actual - entry.dependents)
    removed = sorted(entry.dependents - actual)
    if added:
        problems.append(f"{entry.id}: new path -- now also reached by {', '.join(added)}")
    if removed:
        problems.append(f"{entry.id}: listed dependents no longer reach it: {', '.join(removed)}")
    return problems


def _entry_problems(
    entry: AllowlistEntry,
    packages: set[str],
    dependents: set[str],
    vulns: dict[str, dict],
    today: date,
) -> list[str]:
    problems = []
    if packages != {entry.package}:
        problems.append(
            f"{entry.id}: allowlisted for {entry.package} but reported in "
            f"{', '.join(sorted(packages))}"
        )
    problems.extend(_date_problems(entry, today))
    if any(vulns[pkg].get("fixAvailable") is True for pkg in packages):
        problems.append(f"{entry.id}: a non-breaking fix exists -- run npm audit fix")
    problems.extend(_path_problems(entry, dependents))
    return problems


def evaluate(report: object, allowlist: dict[str, AllowlistEntry], today: date) -> Verdict:
    """Judge an ``npm audit --json`` report against the allowlist.

    Args:
        report: The decoded ``npm audit --json`` output.
        allowlist: Entries from :func:`load_allowlist`.
        today: The date expiry is judged against.

    Returns:
        The rule failures, and a line for each advisory accepted.

    Raises:
        AuditInputError: The report is an npm error or an unknown shape.
    """
    vulns = _vulnerabilities(report)
    reported = _reported_advisories(vulns)
    dependents = _dependents(vulns, reported)
    verdict = Verdict()
    for advisory, packages in sorted(reported.items()):
        entry = allowlist.get(advisory)
        if entry is None:
            verdict.problems.append(
                f"{advisory} in {', '.join(sorted(packages))} is not allowlisted -- "
                f"fix it, or open a ticket before allowlisting it"
            )
            continue
        problems = _entry_problems(entry, packages, dependents[advisory], vulns, today)
        verdict.problems.extend(problems)
        if not problems:
            verdict.accepted.append(
                f"{advisory} in {entry.package} accepted until {entry.expires} "
                f"({entry.ticket}): {entry.reason}"
            )
    for advisory in sorted(set(allowlist) - set(reported)):
        verdict.problems.append(
            f"{advisory}: no longer reported by npm audit -- delete its allowlist entry"
        )
    return verdict


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AuditInputError(f"cannot read {path}: {exc}") from exc


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("report", type=Path, help="output of `npm audit --json`")
    parser.add_argument("allowlist", type=Path, help="the allowlist JSON file")
    parser.add_argument(
        "--today",
        type=date.fromisoformat,
        default=date.today(),
        help="date to judge expiry against (ISO; default: today)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the gate and print GitHub annotations; return the process exit code."""
    args = _parse_args(argv)
    try:
        allowlist = load_allowlist(_read_json(args.allowlist))
        verdict = evaluate(_read_json(args.report), allowlist, args.today)
    except AuditInputError as exc:
        print(f"::error title=npm audit gate::{exc}")
        return EXIT_BAD_INPUT
    for line in verdict.accepted:
        print(f"::warning title=npm audit allowlist::{line}")
    for line in verdict.problems:
        print(f"::error title=npm audit::{line}")
    if verdict.problems:
        return EXIT_PROBLEMS
    print(f"npm audit gate: pass ({len(verdict.accepted)} allowlisted advisories)")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
