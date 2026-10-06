"""npm `overrides` in frontend/package.json: each one is present exactly while it is needed."""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_JSON = REPO_ROOT / "frontend" / "package.json"
PACKAGE_LOCK = REPO_ROOT / "frontend" / "package-lock.json"

GRAPHQL_TOOLS_UTILS = "@graphql-tools/utils"
# GHSA-7mx3-vvmw-hjmv covers <=12.0.0 and no 11.x release carries the fix, so any
# dependent still declaring an 11.x range would resolve to a vulnerable copy (CUI-0055).
VULNERABLE_MAJOR_RANGE = re.compile(r"^[\^~]?11\.")


def _overrides() -> dict:
    return json.loads(PACKAGE_JSON.read_text(encoding="utf-8")).get("overrides", {})


def _declared_ranges(dependency: str) -> set[str]:
    packages = json.loads(PACKAGE_LOCK.read_text(encoding="utf-8"))["packages"]
    return {
        declared
        for path, entry in packages.items()
        if path
        for section in ("dependencies", "peerDependencies", "optionalDependencies")
        if (declared := entry.get(section, {}).get(dependency))
    }


def test_graphql_tools_utils_override_is_present_exactly_while_a_dependent_pins_11x():
    pinned_to_11 = sorted(
        r for r in _declared_ranges(GRAPHQL_TOOLS_UTILS) if VULNERABLE_MAJOR_RANGE.match(r)
    )
    overridden = GRAPHQL_TOOLS_UTILS in _overrides()

    assert overridden == bool(pinned_to_11), (
        f"{GRAPHQL_TOOLS_UTILS}: dependents still declaring 11.x = {pinned_to_11}, "
        f"override present = {overridden}. Add the override while any dependent pins 11.x "
        "(GHSA-7mx3-vvmw-hjmv has no 11.x fix); remove it once none does (CUI-0055)."
    )


def test_every_npm_override_is_covered_by_a_test_in_this_file():
    assert set(_overrides()) <= {GRAPHQL_TOOLS_UTILS}, (
        "a new npm override needs its own present-exactly-while-needed test here"
    )
