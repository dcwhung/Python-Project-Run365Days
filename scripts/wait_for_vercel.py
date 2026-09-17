"""Wait for Vercel's production deployment of one commit, and print its URL.

Standard library only, so CI needs nothing installed to run it.

Vercel deploys outside GitHub Actions, so a workflow that wants to test what was
deployed has to find out from GitHub when Vercel finished. It reports twice, and
which mechanism to use is not a matter of taste:

- a **commit status** with context ``Vercel``, and
- a **GitHub Deployment**, created by ``vercel[bot]``, in environment
  ``Production`` for the production branch and ``Preview`` for everything else.

Measured against this repository on 2026-09-17: 100 deployments listed, 8
``Production`` and 84 ``Preview`` from ``vercel[bot]`` (the other 8 are
``github-pages``). Only the Deployment carries ``environment_url``, and only it
can be filtered server-side by ``sha`` *and* ``environment``, which is what makes
"the production deployment of exactly this commit" answerable rather than
guessed at. So this polls Deployments.

Note ``production_environment`` is ``false`` on those Production deployments, so
filtering on that flag finds nothing; the ``environment`` name is what to match.

Polling rather than the ``deployment_status`` event trigger, deliberately.
``deployment_status`` would be the natural trigger and needs no polling at all,
but GitHub only fires it for workflow files on the **default branch** -- which
here is ``master``, while deployments come from ``develop``. A workflow written
that way would sit on ``develop`` doing nothing at all, silently, until the next
release merge carried it to ``master``. A gate whose failure mode is "never
runs" is worse than one that costs a few API calls, so this runs from ``push``,
which has no such restriction.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

API_ROOT = "https://api.github.com"
"""GitHub REST API root."""

PRODUCTION_ENVIRONMENT = "Production"
"""Deployment environment name Vercel uses for the production branch."""

TERMINAL_FAILURE_STATES = frozenset({"failure", "error"})
"""Deployment-status states that mean waiting longer cannot help."""

POLL_INTERVAL_SEC = 15
"""Seconds between polls."""

DEFAULT_TIMEOUT_SEC = 900
"""How long to wait for Vercel before giving up. A cold build takes minutes."""


def _get(url: str, token: str | None) -> list | dict:
    """GET *url* and decode the JSON body.

    Args:
        url: Absolute API URL.
        token: Bearer token, or None to call unauthenticated.

    Returns:
        The decoded body.

    Raises:
        urllib.error.HTTPError: The API refused the request.
    """
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "run365days-smoke"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def latest_production_url(repo: str, sha: str, token: str | None) -> tuple[str, str] | None:
    """Return ``(state, environment_url)`` for the newest production deployment of *sha*.

    Args:
        repo: ``owner/name``.
        sha: Full commit SHA.
        token: Bearer token, or None.

    Returns:
        The newest status's state and the URL it published, or None when Vercel
        has not created a production deployment for this commit yet, or has
        created one but not yet posted a status to it.
    """
    url = f"{API_ROOT}/repos/{repo}/deployments?sha={sha}&environment={PRODUCTION_ENVIRONMENT}"
    deployments = _get(url, token)
    if not deployments:
        return None
    # A re-deploy of the same commit adds another deployment, so sort rather
    # than trusting the listing order.
    newest = max(deployments, key=lambda d: d["created_at"])
    statuses = _get(f"{API_ROOT}/repos/{repo}/deployments/{newest['id']}/statuses", token)
    if not statuses:
        return None
    latest = max(statuses, key=lambda s: s["created_at"])
    return latest["state"], latest.get("environment_url") or ""


def wait(repo: str, sha: str, token: str | None, timeout: float) -> int:
    """Poll until the production deployment of *sha* succeeds, fails or times out.

    Args:
        repo: ``owner/name``.
        sha: Full commit SHA.
        token: Bearer token, or None.
        timeout: Seconds to wait before giving up.

    Returns:
        0 once the deployment reports success, 1 on a failed or timed-out one.
    """
    deadline = time.monotonic() + timeout
    print(f"Waiting for the Vercel {PRODUCTION_ENVIRONMENT} deployment of {sha} in {repo}")
    while True:
        try:
            found = latest_production_url(repo, sha, token)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            print(f"  .... API call failed, will retry: {type(exc).__name__}: {exc}")
            found = None
        if found:
            state, environment_url = found
            print(f"  .... state={state} url={environment_url or '(none published)'}")
            if state == "success":
                _publish(environment_url)
                return 0
            if state in TERMINAL_FAILURE_STATES:
                print(f"Vercel reported {state}; there is nothing to smoke-test.")
                return 1
        else:
            print("  .... no production deployment for this commit yet")
        if time.monotonic() >= deadline:
            print(
                f"Gave up after {timeout:.0f}s. Either Vercel never deployed this commit, "
                f"or its production branch is no longer the branch this workflow runs on "
                f"-- that setting lives in the Vercel dashboard and cannot be read from a "
                f"checkout (see docs/deployment.md)."
            )
            return 1
        time.sleep(POLL_INTERVAL_SEC)


def _publish(environment_url: str) -> None:
    """Record the deployment URL as a step output when running under Actions.

    Args:
        environment_url: The per-deployment URL Vercel published, possibly empty.
    """
    print(f"Vercel reports success. Deployment URL: {environment_url or '(none published)'}")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(f"deployment_url={environment_url}\n")


def main() -> int:
    """Parse arguments and wait.

    Returns:
        The process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--sha", default=os.environ.get("GITHUB_SHA", ""))
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SEC)
    args = parser.parse_args()
    if not args.repo or not args.sha:
        parser.error("--repo and --sha are required outside GitHub Actions")
    return wait(args.repo, args.sha, os.environ.get("GITHUB_TOKEN") or None, args.timeout)


if __name__ == "__main__":
    sys.exit(main())
