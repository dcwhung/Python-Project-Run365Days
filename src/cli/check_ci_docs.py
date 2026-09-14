"""Keep the prose that describes CI in step with `.github/workflows/pages.yml`.

The workflow is the only source of truth for which branches are gated and which
branch deploys. Documentation that restates those facts in English goes stale
every time the workflow changes, silently and without any check noticing. This
tool removes the restating: the branch facts live in generated blocks delimited
by `<!-- ci-facts:start -->` / `<!-- ci-facts:end -->`, regenerated from the
workflow, and `--check` fails while a block, a status badge or a stray
`refs/heads/...` mention disagrees with the workflow.

It reads the workflow with the standard library only, matching the small set of
keys it needs. Anything it cannot find is an error rather than a default, so a
restructured workflow turns the gate red instead of quietly passing.

Usage
-----
    run365-ci-docs --write README.md docs/architecture.md docs/deployment.md
    run365-ci-docs --check README.md docs/architecture.md docs/deployment.md
"""

import argparse
import re
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path

START = "<!-- ci-facts:start -->"
END = "<!-- ci-facts:end -->"
DEFAULT_WORKFLOW = Path(".github/workflows/pages.yml")
WRAP_WIDTH = 76

_REF = re.compile(r"refs/heads/([\w./-]+)")
_BADGE = re.compile(r"workflows/pages\.yml/badge\.svg\?branch=([\w./-]+)")


class WorkflowError(Exception):
    """The workflow file does not have the shape this tool knows how to read."""


@dataclass(frozen=True)
class CiFacts:
    """The branch behaviour of the Pages workflow, as read from the workflow file."""

    push: tuple[str, ...]
    pull_request: tuple[str, ...]
    deploy: str
    dispatch: bool


def _indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _section(lines: list[str], name: str, indent: int) -> list[str]:
    """Return the lines nested under ``name:`` at ``indent`` columns."""
    header = " " * indent + name + ":"
    start = None
    for i, line in enumerate(lines):
        if line.rstrip() == header or line.startswith(header + " "):
            start = i
            break
    if start is None:
        raise WorkflowError(f"no `{name}:` key at indent {indent}")
    body = []
    for line in lines[start + 1 :]:
        if line.strip() and _indent_of(line) <= indent:
            break
        body.append(line)
    return body


def _scalar(lines: list[str], name: str, indent: int) -> str:
    """Return the inline value of ``name:`` at ``indent`` columns."""
    prefix = " " * indent + name + ":"
    for line in lines:
        if line.startswith(prefix):
            return line[len(prefix) :].strip()
    raise WorkflowError(f"no `{name}:` key at indent {indent}")


def _branches(on_lines: list[str], event: str) -> tuple[str, ...]:
    """Return the branch filter of ``event`` inside the workflow's ``on:`` block."""
    event_lines = _section(on_lines, event, 2)
    inline = _scalar(event_lines, "branches", 4)
    if inline:
        if not (inline.startswith("[") and inline.endswith("]")):
            raise WorkflowError(f"`{event}.branches` is not a list: {inline}")
        raw = inline[1:-1].split(",")
    else:
        raw = [line.strip().lstrip("- ") for line in _section(event_lines, "branches", 4)]
    branches = tuple(item.strip().strip("'\"") for item in raw if item.strip().strip("'\""))
    if not branches:
        raise WorkflowError(f"`{event}.branches` is empty")
    return branches


def _deploy_branch(lines: list[str], text: str) -> str:
    """Return the one branch the `build` and `deploy` jobs are gated on."""
    jobs = _section(lines, "jobs", 0)
    gates = {job: _scalar(_section(jobs, job, 2), "if", 4) for job in ("build", "deploy")}
    if len(set(gates.values())) != 1:
        raise WorkflowError(f"`build` and `deploy` are gated differently: {gates}")
    named = sorted(set(_REF.findall(text)))
    if len(named) != 1:
        raise WorkflowError(
            f"the workflow names {len(named)} branches as refs/heads/...: {named or 'none'}. "
            "Every gate must agree on a single deploy branch."
        )
    branch = named[0]
    if f"refs/heads/{branch}" not in next(iter(gates.values())):
        raise WorkflowError(f"the `build`/`deploy` gate does not test refs/heads/{branch}")
    return branch


def read_facts(workflow: Path) -> CiFacts:
    """Read the branch behaviour out of the Pages workflow."""
    text = workflow.read_text()
    lines = text.splitlines()
    on_lines = _section(lines, "on", 0)
    facts = CiFacts(
        push=_branches(on_lines, "push"),
        pull_request=_branches(on_lines, "pull_request"),
        deploy=_deploy_branch(lines, text),
        dispatch=any(line.startswith("  workflow_dispatch:") for line in on_lines),
    )
    if facts.deploy not in facts.push:
        raise WorkflowError(
            f"the deploy branch `{facts.deploy}` is not in the push trigger list "
            f"{list(facts.push)}, so the workflow can never deploy"
        )
    return facts


def _joined(branches: tuple[str, ...]) -> str:
    names = [f"`{branch}`" for branch in branches]
    if len(names) == 1:
        return names[0]
    return ", ".join(names[:-1]) + " and " + names[-1]


def render(facts: CiFacts) -> list[str]:
    """Render the generated block, unindented, as a list of lines."""
    bullets = []
    if facts.push == facts.pull_request:
        bullets.append(
            "Lint, tests and the frontend checks run on every push and pull "
            f"request to {_joined(facts.push)}."
        )
    else:
        bullets.append(
            f"Lint, tests and the frontend checks run on every push to {_joined(facts.push)} "
            f"and on every pull request to {_joined(facts.pull_request)}."
        )
    bullets.append(
        f"The site is built and deployed from `{facts.deploy}` only; every "
        "other branch stops after the checks."
    )
    if facts.dispatch:
        bullets.append(
            "A manual `workflow_dispatch` run follows the same rule: it "
            f"re-deploys when run on `{facts.deploy}`, and is checks-only elsewhere."
        )
    block = [
        START,
        "<!-- Generated from .github/workflows/pages.yml by `run365-ci-docs --write`.",
        "     Change the workflow, re-run the command, and commit both. -->",
    ]
    for bullet in bullets:
        block += textwrap.fill(
            bullet, width=WRAP_WIDTH, initial_indent="- ", subsequent_indent="  "
        ).splitlines()
    block.append(END)
    return block


def _span(lines: list[str], path: Path) -> tuple[int, int, str]:
    """Return the start index, end index and indentation of the generated block."""
    starts = [i for i, line in enumerate(lines) if line.strip() == START]
    ends = [i for i, line in enumerate(lines) if line.strip() == END]
    if len(starts) != 1 or len(ends) != 1:
        raise WorkflowError(
            f"{path}: expected exactly one `{START}` / `{END}` pair, "
            f"found {len(starts)} and {len(ends)}"
        )
    if ends[0] < starts[0]:
        raise WorkflowError(f"{path}: `{END}` comes before `{START}`")
    return starts[0], ends[0], " " * _indent_of(lines[starts[0]])


def _expected(lines: list[str], facts: CiFacts, path: Path) -> list[str]:
    start, end, indent = _span(lines, path)
    block = [(indent + line).rstrip() for line in render(facts)]
    return lines[:start] + block + lines[end + 1 :]


def _prose_problems(lines: list[str], facts: CiFacts, path: Path) -> list[str]:
    """Report branch claims that sit outside the generated block, where nothing checks them."""
    start, end, _ = _span(lines, path)
    problems = []
    for number, line in enumerate(lines, start=1):
        if start <= number - 1 <= end:
            continue
        if _REF.search(line):
            problems.append(
                f"{path}:{number}: `refs/heads/...` outside the generated block. Move the "
                "claim into the block, or word the sentence without naming a branch."
            )
        badge = _BADGE.search(line)
        if badge and badge.group(1) != facts.deploy:
            problems.append(
                f"{path}:{number}: the pages.yml badge tracks `{badge.group(1)}` but the "
                f"workflow deploys `{facts.deploy}`."
            )
    return problems


def _process(path: Path, facts: CiFacts, write: bool) -> list[str]:
    original = path.read_text()
    lines = original.splitlines()
    updated = _expected(lines, facts, path)
    problems = _prose_problems(updated if write else lines, facts, path)
    if write:
        path.write_text("\n".join(updated) + "\n")
        return problems
    if updated != lines:
        problems.insert(0, f"{path}: the generated CI block is out of date.")
    return problems


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("files", nargs="+", type=Path, help="markdown files carrying a ci-facts block")
    ap.add_argument("--workflow", type=Path, default=DEFAULT_WORKFLOW, help="the CI workflow")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="exit 1 if anything is out of date")
    mode.add_argument("--write", action="store_true", help="regenerate the blocks in place")
    args = ap.parse_args()

    try:
        facts = read_facts(args.workflow)
        problems = [p for path in args.files for p in _process(path, facts, args.write)]
    except (WorkflowError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)

    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        if args.check:
            hint = "run `run365-ci-docs --write <files>` after changing the workflow"
            print(hint, file=sys.stderr)
        sys.exit(1)
    verb = "regenerated" if args.write else "up to date"
    print(f"CI docs {verb} ({len(args.files)} files, deploy branch `{facts.deploy}`)")


if __name__ == "__main__":
    main()
