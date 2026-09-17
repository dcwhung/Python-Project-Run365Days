"""Packaging guards: the Vercel function must not carry the parsing stack.

`requirements.txt` installs the package itself, so `[project].dependencies` is
literally what ends up inside the `api/graphql.py` serverless bundle. Anything
that only runs while *producing* `data/processed/run365.db` belongs in the
`pipeline` extra instead. tests/test_api_imports.py guards the same invariant
from the import side; this file guards it from the packaging side.
"""

import re
import sys
from pathlib import Path

import pytest

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - the repo pins 3.12; this only helps a 3.10 checkout
    tomllib = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = PROJECT_ROOT / "pyproject.toml"
VERCEL_BUILD = PROJECT_ROOT / "scripts" / "vercel-build.sh"

PIPELINE_EXTRA = "pipeline"
DEV_EXTRA = "dev"
# Every name here must be a package the project actually declares, because
# test_pipeline_extra_carries_the_parsing_stack reads this set as a *floor* for
# the pipeline extra: an undeclared name makes that test assert something the
# repository can never satisfy. `pytz` sat here for exactly that reason -- the
# only import of it is legacy/test.py, which `package-dir` never ships and no
# install resolves, so no extra was ever going to carry it.
BUILD_ONLY_PACKAGES = frozenset(
    {"beautifulsoup4", "lxml", "numpy", "pandas", "python-dateutil", "requests"}
)
RUNTIME_PACKAGES = frozenset({"flask", "sqlalchemy", "strawberry-graphql"})

pytestmark = pytest.mark.skipif(tomllib is None, reason="tomllib needs Python 3.11+")


@pytest.fixture(scope="module")
def pyproject() -> dict:
    """Parsed pyproject.toml."""
    with PYPROJECT.open("rb") as handle:
        return tomllib.load(handle)


def _names(requirements: list[str]) -> set[str]:
    """Bare distribution names, with version specifiers, extras and markers dropped."""
    return {re.split(r"[<>=!~\[;\s]", req, maxsplit=1)[0].strip().lower() for req in requirements}


def test_core_dependencies_exclude_the_parsing_stack(pyproject):
    leaked = _names(pyproject["project"]["dependencies"]) & BUILD_ONLY_PACKAGES
    assert not leaked, f"build-only packages would ship in the Vercel function: {sorted(leaked)}"


def test_core_dependencies_cover_the_api_runtime(pyproject):
    missing = RUNTIME_PACKAGES - _names(pyproject["project"]["dependencies"])
    assert not missing, f"the API cannot start without: {sorted(missing)}"


def test_pipeline_extra_carries_the_parsing_stack(pyproject):
    extras = pyproject["project"]["optional-dependencies"]
    assert PIPELINE_EXTRA in extras, f"no '{PIPELINE_EXTRA}' extra declared"
    missing = BUILD_ONLY_PACKAGES - _names(extras[PIPELINE_EXTRA])
    assert not missing, f"'{PIPELINE_EXTRA}' extra is missing: {sorted(missing)}"


def test_dev_extra_pulls_in_the_pipeline_extra(pyproject):
    dev = pyproject["project"]["optional-dependencies"][DEV_EXTRA]
    assert any(f"[{PIPELINE_EXTRA}]" in req for req in dev), (
        f"'{DEV_EXTRA}' must self-reference run365days[{PIPELINE_EXTRA}] so the suite can parse"
    )


def test_vercel_build_installs_the_pipeline_extra():
    script = VERCEL_BUILD.read_text()
    assert f".[{PIPELINE_EXTRA}]" in script, (
        f"{VERCEL_BUILD.name} must install .[{PIPELINE_EXTRA}]; run365-export needs the parsers"
    )
