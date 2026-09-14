"""The Vercel response-header policy, and the app assumptions it depends on.

`vercel.json` is never exercised by the test suite or by a local run, so a
weakened policy would otherwise only surface in production. These tests pin the
decisions and, more usefully, pin the two app-side facts the strict directives
rest on: the HTML shell carries no inline script, and the front end talks only
to its own origin.
"""

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
VERCEL = REPO / "vercel.json"
INDEX_HTML = REPO / "frontend/index.html"

DOCUMENT_SOURCE = "/((?!api/).*)"
API_SOURCE = "/api/(.*)"
GLOBAL_SOURCE = "/(.*)"


def _config() -> dict:
    return json.loads(VERCEL.read_text(encoding="utf-8"))


def _headers_for(source: str) -> dict[str, str]:
    for block in _config()["headers"]:
        if block["source"] == source:
            return {h["key"]: h["value"] for h in block["headers"]}
    raise AssertionError(f"no header block for {source!r}")


def _csp() -> dict[str, list[str]]:
    raw = _headers_for(DOCUMENT_SOURCE)["Content-Security-Policy"]
    directives = {}
    for part in raw.split(";"):
        tokens = part.split()
        if tokens:
            directives[tokens[0]] = tokens[1:]
    return directives


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("X-Content-Type-Options", "nosniff"),
        ("X-Frame-Options", "DENY"),
        ("Referrer-Policy", "strict-origin-when-cross-origin"),
        ("Cross-Origin-Opener-Policy", "same-origin"),
    ],
)
def test_baseline_headers_apply_to_every_response(key, expected):
    assert _headers_for(GLOBAL_SOURCE)[key] == expected


def test_hsts_lasts_at_least_a_year():
    value = _headers_for(GLOBAL_SOURCE)["Strict-Transport-Security"]
    max_age = int(re.search(r"max-age=(\d+)", value).group(1))
    assert max_age >= 31536000


def test_permissions_policy_denies_the_features_the_app_never_uses():
    value = _headers_for(GLOBAL_SOURCE)["Permissions-Policy"]
    for feature in ("camera", "microphone", "geolocation", "payment", "usb"):
        assert f"{feature}=()" in value


def test_scripts_run_only_from_our_own_origin():
    assert _csp()["script-src"] == ["'self'"]


def test_csp_pins_the_dangerous_directives_shut():
    csp = _csp()
    assert csp["object-src"] == ["'none'"]
    assert csp["base-uri"] == ["'none'"]
    assert csp["frame-ancestors"] == ["'none'"]
    assert csp["default-src"] == ["'self'"]


def test_csp_keeps_network_calls_same_origin():
    assert _csp()["connect-src"] == ["'self'"]


def test_api_is_not_readable_cross_origin():
    api = _headers_for(API_SOURCE)
    assert api["Cross-Origin-Resource-Policy"] == "same-origin"
    assert "Access-Control-Allow-Origin" not in api


def test_no_header_block_opens_cors():
    for block in _config()["headers"]:
        keys = {h["key"] for h in block["headers"]}
        assert "Access-Control-Allow-Origin" not in keys


def test_api_responses_are_never_cached():
    assert _headers_for(API_SOURCE)["Cache-Control"] == "no-store"


def test_hashed_assets_stay_immutable():
    assert "immutable" in _headers_for("/assets/(.*)")["Cache-Control"]


def test_html_shell_carries_no_inline_script():
    """`script-src 'self'` has no hash or nonce, so an inline script would break the app."""
    html = INDEX_HTML.read_text(encoding="utf-8")
    for tag in re.findall(r"<script\b[^>]*>", html):
        assert "src=" in tag, f"inline script would be blocked by the CSP: {tag}"
