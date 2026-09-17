"""Smoke-test a deployed run365days API: the health endpoint and one real query.

Standard library only, so CI needs nothing installed to run it.

Two checks, because neither one alone separates the failures that matter:

- ``GET /api/health`` proves an app is answering *and* that it is the real one.
  ``api/graphql.py`` serves a minimal fallback app when start-up fails, and that
  fallback answers every path -- including this one. It is distinguishable:
  measured against both apps, the real one answers ``200`` with
  ``{"status": "ok", "db": ...}`` and the fallback answers ``500`` with
  ``{"status": "error", "error": ...}``, so the status code and the body agree
  and either alone would do. Both are asserted anyway; the cost is a line and it
  removes the question.
- one real GraphQL query proves the bundled database is present *and* populated.
  Health cannot: it reports the path it resolved, never a row. A build that
  exported an empty database passes health and serves an empty dashboard, which
  is not hypothetical here -- ``docs/deployment.md`` records exactly that
  failure, a 64 KB database with 0 activities, from the first Vercel build. So
  the query asserts a row count rather than a 200.

``meta`` and ``activitiesCount`` are the query because they read two different
tables, are O(1) on the server, and are both non-null in the schema: a missing
meta row raises, and an empty activities table returns 0 and fails the count
assertion below. Introspection is deliberately not used -- the deployment
rejects ``__schema`` documents unless ``RUN365_GRAPHIQL`` is set, so a smoke
test built on it would fail for a reason that is not a fault.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

HEALTH_PATH = "/api/health"
"""Health endpoint, matching ``run365days.api.app.HEALTH_PATH``."""

GRAPHQL_PATH = "/api/graphql"
"""GraphQL endpoint, matching ``run365days.api.app.GRAPHQL_PATH``."""

QUERY = "{ meta { year generatedAt } activitiesCount }"
"""The one query this smoke test sends. See the module docstring for why."""

TIMEOUT_SEC = 30
"""Per-request timeout. Generous: a cold serverless function opens the database."""

DEFAULT_ATTEMPTS = 5
"""Attempts per check before giving up."""

DEFAULT_DELAY_SEC = 10
"""Seconds between attempts."""


class SmokeError(Exception):
    """A check that failed in a way retrying cannot fix."""


def _request(url: str, payload: bytes | None = None) -> tuple[int, str]:
    """Send one request and return ``(status, body)``, reading error bodies too.

    Args:
        url: Absolute URL to request.
        payload: JSON body to POST, or None for a GET.

    Returns:
        The HTTP status and the decoded response body. An HTTP error status is
        returned rather than raised, because the body of a 500 is the most
        useful thing this script can print.

    Raises:
        urllib.error.URLError: The request never reached an HTTP response.
    """
    headers = {"Content-Type": "application/json"} if payload else {}
    request = urllib.request.Request(url, data=payload, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SEC) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")


def _json_body(status: int, body: str) -> dict:
    """Parse *body* as a JSON object, quoting it in the error when it is not one.

    Args:
        status: HTTP status the body arrived with, for the error message.
        body: Raw response body.

    Returns:
        The decoded object.

    Raises:
        SmokeError: The body is not a JSON object.
    """
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SmokeError(f"HTTP {status}: body is not JSON ({exc}): {body[:400]!r}") from exc
    if not isinstance(parsed, dict):
        raise SmokeError(f"HTTP {status}: body is not a JSON object: {body[:400]!r}")
    return parsed


def check_health(base_url: str) -> str:
    """Assert the real app -- not the start-up fallback -- answers the health endpoint.

    Args:
        base_url: Deployment root, without a trailing slash.

    Returns:
        A one-line description of what answered.

    Raises:
        SmokeError: The endpoint did not answer as the real app does.
    """
    status, body = _request(base_url + HEALTH_PATH)
    payload = _json_body(status, body)
    if status != 200:
        raise SmokeError(
            f"{HEALTH_PATH} answered HTTP {status}, expected 200. "
            f"A 500 here is the start-up fallback in api/graphql.py reporting why the "
            f"app could not be built: {payload.get('error', body[:400])!r}"
        )
    if payload.get("status") != "ok":
        raise SmokeError(f"{HEALTH_PATH} answered 200 but not as the real app: {payload!r}")
    return f"{HEALTH_PATH}: HTTP 200, status=ok, db={payload.get('db')!r}"


def check_query(base_url: str, min_activities: int) -> str:
    """Assert one real query reads a populated database.

    Args:
        base_url: Deployment root, without a trailing slash.
        min_activities: Smallest activity count treated as a healthy deployment.

    Returns:
        A one-line description of what came back.

    Raises:
        SmokeError: The query errored, or the database it read is not populated.
    """
    payload = json.dumps({"query": QUERY}).encode()
    status, body = _request(base_url + GRAPHQL_PATH, payload)
    parsed = _json_body(status, body)
    if status != 200:
        raise SmokeError(f"{GRAPHQL_PATH} answered HTTP {status}: {body[:400]!r}")
    if parsed.get("errors"):
        raise SmokeError(f"{GRAPHQL_PATH} returned GraphQL errors: {parsed['errors']!r}")
    data = parsed.get("data")
    if not isinstance(data, dict):
        raise SmokeError(f"{GRAPHQL_PATH} returned no data object: {body[:400]!r}")
    meta = data.get("meta")
    if not isinstance(meta, dict) or meta.get("year") is None:
        raise SmokeError(f"meta did not resolve: {data!r}")
    count = data.get("activitiesCount")
    if not isinstance(count, int):
        raise SmokeError(f"activitiesCount did not resolve to a number: {data!r}")
    if count < min_activities:
        raise SmokeError(
            f"activitiesCount is {count}, expected at least {min_activities}. "
            f"The app started and the database opened, so this is a deployment that "
            f"bundled an empty or partial export rather than one that failed to build."
        )
    return f"{GRAPHQL_PATH}: meta.year={meta['year']}, activitiesCount={count}"


def run(base_url: str, attempts: int, delay: float, min_activities: int) -> int:
    """Run both checks, retrying each one, and return a process exit code.

    Retries exist for a cold serverless function and for the seconds after an
    alias is promoted, not to paper over a failure: a deployment that is
    genuinely broken fails every attempt and this still ends non-zero. The last
    failure is printed in full.

    Args:
        base_url: Deployment root; a trailing slash is stripped.
        attempts: Attempts per check.
        delay: Seconds between attempts.
        min_activities: Smallest activity count treated as a healthy deployment.

    Returns:
        0 when every check passed, 1 otherwise.
    """
    base_url = base_url.rstrip("/")
    print(f"Smoke-testing {base_url}")
    checks = (
        ("health", lambda: check_health(base_url)),
        ("query", lambda: check_query(base_url, min_activities)),
    )
    for name, check in checks:
        last = ""
        for attempt in range(1, attempts + 1):
            try:
                print(f"  PASS {name}: {check()}")
                break
            except (SmokeError, urllib.error.URLError, TimeoutError, OSError) as exc:
                last = f"{type(exc).__name__}: {exc}"
                print(f"  .... {name} attempt {attempt}/{attempts}: {last}")
                if attempt < attempts:
                    time.sleep(delay)
        else:
            print(f"  FAIL {name}: {last}")
            return 1
    print("Smoke test passed.")
    return 0


def main() -> int:
    """Parse arguments and run the smoke test.

    Returns:
        The process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("base_url", help="Deployment root, e.g. https://example.vercel.app")
    parser.add_argument("--attempts", type=int, default=DEFAULT_ATTEMPTS)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SEC)
    parser.add_argument(
        "--min-activities",
        type=int,
        default=1,
        help="Fail when the deployment reports fewer activities than this (default: 1)",
    )
    args = parser.parse_args()
    return run(args.base_url, args.attempts, args.delay, args.min_activities)


if __name__ == "__main__":
    sys.exit(main())
