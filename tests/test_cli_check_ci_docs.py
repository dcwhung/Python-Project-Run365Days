"""The CI-documentation gate: it must read pages.yml, and it must actually fail."""

import subprocess
import sys
from pathlib import Path

import pytest

from run365days.cli.check_ci_docs import (
    END,
    START,
    CiFacts,
    WorkflowError,
    read_facts,
    render,
)

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github/workflows/pages.yml"
GATED_DOCS = ("README.md", "docs/architecture.md", "docs/deployment.md")

FLOW_WORKFLOW = """\
name: CI and GitHub Pages

on:
  push:
    branches: [develop, master]
  pull_request:
    branches: [develop, master]
  workflow_dispatch:

concurrency:
  group: pages-${{ github.ref == 'refs/heads/develop' && 'deploy' || github.ref }}

jobs:
  lint-test:
    name: Lint and test
    runs-on: ubuntu-latest
  build:
    # a comment inside the job must not end the job block
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
  deploy:
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
"""


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "pages.yml"
    path.write_text(text)
    return path


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    # S603: the executable is sys.executable and the arguments come from the test
    # body, not from input -- no shell, so nothing to inject through.
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "run365days.cli.check_ci_docs", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
    )


# --- reading the workflow ---------------------------------------------------


def test_reads_the_real_workflow():
    facts = read_facts(WORKFLOW)
    assert facts.push == ("develop", "master")
    assert facts.pull_request == ("develop", "master")
    assert facts.deploy == "develop"
    assert facts.dispatch is True


def test_reads_flow_style_branch_lists(tmp_path):
    assert read_facts(_write(tmp_path, FLOW_WORKFLOW)).push == ("develop", "master")


def test_reads_block_style_branch_lists(tmp_path):
    text = FLOW_WORKFLOW.replace(
        "  push:\n    branches: [develop, master]\n",
        "  push:\n    branches:\n      - develop\n      - master\n",
    )
    assert read_facts(_write(tmp_path, text)).push == ("develop", "master")


def test_notices_a_missing_workflow_dispatch(tmp_path):
    text = FLOW_WORKFLOW.replace("  workflow_dispatch:\n", "")
    assert read_facts(_write(tmp_path, text)).dispatch is False


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        # a half-finished branch switch: concurrency still names the old branch
        (
            "'refs/heads/develop'\n    runs-on",
            "'refs/heads/master'\n    runs-on",
            "branches as refs/heads",
        ),
        # build and deploy gated differently
        (
            "  deploy:\n    if: github.ref == 'refs/heads/develop'",
            "  deploy:\n    if: github.event_name != 'pull_request'",
            "gated differently",
        ),
        # the gate is gone entirely, so every branch would deploy
        ("  deploy:\n    if: github.ref == 'refs/heads/develop'\n", "  deploy:\n", "no `if:` key"),
        # no branch filter at all
        (
            "    branches: [develop, master]\n  pull_request:",
            "  pull_request:",
            "no `branches:` key",
        ),
    ],
)
def test_a_workflow_it_cannot_read_is_an_error_not_a_default(tmp_path, old, new, message):
    text = FLOW_WORKFLOW.replace(old, new)
    assert text != FLOW_WORKFLOW
    with pytest.raises(WorkflowError) as excinfo:
        read_facts(_write(tmp_path, text))
    assert message in str(excinfo.value)


def test_a_deploy_branch_outside_the_push_list_is_an_error(tmp_path):
    text = FLOW_WORKFLOW.replace("[develop, master]", "[master]")
    with pytest.raises(WorkflowError, match="can never deploy"):
        read_facts(_write(tmp_path, text))


# --- rendering --------------------------------------------------------------


def test_render_names_every_branch_and_wraps():
    block = render(read_facts(WORKFLOW))
    assert block[0] == START
    assert block[-1] == END
    body = "\n".join(block)
    assert "`develop` and `master`" in body
    assert "deployed from `develop` only" in body
    assert max(len(line) for line in block) <= 80


def test_render_separates_push_and_pull_request_when_they_differ():
    facts = CiFacts(
        push=("develop",), pull_request=("develop", "master"), deploy="develop", dispatch=False
    )
    body = "\n".join(render(facts))
    assert "every push to `develop`" in body
    assert "pull request to `develop` and `master`" in body
    assert "workflow_dispatch" not in body


# --- the gate ---------------------------------------------------------------


def test_the_committed_docs_are_in_sync():
    result = _run("--check", *GATED_DOCS)
    assert result.returncode == 0, result.stderr


def test_every_gated_document_carries_a_block():
    for name in GATED_DOCS:
        text = (REPO / name).read_text()
        assert text.count(START) == 1 and text.count(END) == 1, name


def test_a_stale_block_fails_the_check(tmp_path):
    doc = tmp_path / "doc.md"
    doc.write_text(f"intro\n\n{START}\n- nonsense\n{END}\n")
    result = _run("--check", "--workflow", str(WORKFLOW), str(doc))
    assert result.returncode == 1
    assert "out of date" in result.stderr


def test_write_then_check_round_trips(tmp_path):
    doc = tmp_path / "doc.md"
    doc.write_text(f"intro\n\n{START}\n{END}\n\noutro\n")
    assert _run("--write", "--workflow", str(WORKFLOW), str(doc)).returncode == 0
    assert doc.read_text().startswith("intro\n\n<!-- ci-facts:start -->")
    assert doc.read_text().endswith("outro\n")
    assert _run("--check", "--workflow", str(WORKFLOW), str(doc)).returncode == 0


def test_the_block_keeps_the_indentation_of_its_start_marker(tmp_path):
    doc = tmp_path / "doc.md"
    doc.write_text(f"- bullet:\n\n  {START}\n  {END}\n")
    _run("--write", "--workflow", str(WORKFLOW), str(doc))
    body = [line for line in doc.read_text().splitlines() if line.strip().startswith("-")]
    assert all(line.startswith("  ") for line in body[1:])


def test_a_branch_claim_outside_the_block_fails_the_check(tmp_path):
    doc = tmp_path / "doc.md"
    doc.write_text(f"{START}\n{END}\n\nThe deploy job tests `refs/heads/develop`.\n")
    _run("--write", "--workflow", str(WORKFLOW), str(doc))
    result = _run("--check", "--workflow", str(WORKFLOW), str(doc))
    assert result.returncode == 1
    assert "outside the generated block" in result.stderr


def test_a_badge_pointing_at_the_wrong_branch_fails_the_check(tmp_path):
    doc = tmp_path / "doc.md"
    badge = "![CI](https://x/actions/workflows/pages.yml/badge.svg?branch=master)"
    doc.write_text(f"{badge}\n\n{START}\n{END}\n")
    _run("--write", "--workflow", str(WORKFLOW), str(doc))
    result = _run("--check", "--workflow", str(WORKFLOW), str(doc))
    assert result.returncode == 1
    assert "badge tracks `master`" in result.stderr


def test_a_missing_block_fails_the_check(tmp_path):
    doc = tmp_path / "doc.md"
    doc.write_text("no markers here\n")
    result = _run("--check", "--workflow", str(WORKFLOW), str(doc))
    assert result.returncode == 1
    assert "expected exactly one" in result.stderr
