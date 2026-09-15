"""The hand-written package list in pyproject.toml must match what src/ holds.

setuptools is configured with an explicit ``packages`` list and no
auto-discovery, so a sub-package that is renamed, added or removed without
editing that list produces a distribution missing code that every source-run
gate still finds. CI installs editable and the lanes run through a PYTHONPATH
shim, so neither sees the gap; Vercel installs for real and fails at import.

That is not hypothetical: renaming ``dashboard`` to ``analytics`` left the list
naming a package that no longer existed, and a real install shipped without
``run365days.analytics`` while 544 tests stayed green.
"""

import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
DISTRIBUTION_NAME = "run365days"


def _packages_declared_in_pyproject() -> set[str]:
    with (REPO_ROOT / "pyproject.toml").open("rb") as f:
        return set(tomllib.load(f)["tool"]["setuptools"]["packages"])


def _packages_present_in_src() -> set[str]:
    found = {DISTRIBUTION_NAME}
    for init in SRC_ROOT.rglob("__init__.py"):
        relative = init.parent.relative_to(SRC_ROOT)
        if relative != Path():
            found.add(f"{DISTRIBUTION_NAME}.{'.'.join(relative.parts)}")
    return found


def test_every_package_under_src_is_declared():
    missing = _packages_present_in_src() - _packages_declared_in_pyproject()
    assert not missing, (
        f"{sorted(missing)} exist under src/ but are absent from pyproject.toml's "
        "packages list, so a real (non-editable) install would omit them"
    )


def test_every_declared_package_exists():
    stale = _packages_declared_in_pyproject() - _packages_present_in_src()
    assert not stale, f"pyproject.toml declares {sorted(stale)}, which no longer exist under src/"
