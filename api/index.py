"""Vercel serverless entry point: exposes the Flask app as `app`.

Vercel's Python runtime looks for a WSGI callable named `app` in files under
api/; this one is served at /api/index and vercel.json rewrites every /api/*
request to it (Flask then routes /api/graphql and /api/health). The file is
deliberately not called graphql.py: that module name would shadow the
graphql-core package Strawberry imports. Two more things differ from a normal
install and are handled here:

- The runtime installs the *dependencies* from pyproject.toml but not the
  project itself, so ``run365days`` is loaded straight from ``src/`` when
  it is not importable (pyproject maps the package name onto that folder).
- The SQLite file is bundled via ``includeFiles`` in vercel.json and sits at
  ``data/processed/run365.db`` relative to the repository root;
  ``RUN365_DB_PATH`` overrides that.

If start-up fails, a minimal app still answers ``/api/health`` with the
error so the cause is visible without digging through function logs.
"""

import importlib.util
import json
import os
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGE_NAME = "run365days"
PACKAGE_DIR = REPO_ROOT / "src"
DEFAULT_DB = REPO_ROOT / "data" / "processed" / "run365.db"


def _load_package_from_source() -> None:
    """Register src/ as the `run365days` package when it is not installed."""
    if importlib.util.find_spec(PACKAGE_NAME) is not None:
        return
    spec = importlib.util.spec_from_file_location(
        PACKAGE_NAME,
        PACKAGE_DIR / "__init__.py",
        submodule_search_locations=[str(PACKAGE_DIR)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[PACKAGE_NAME] = module
    spec.loader.exec_module(module)


def _error_app(exc: BaseException):
    """App that reports why the real app could not start (Flask if available, else raw WSGI)."""
    message = f"{type(exc).__name__}: {exc}"
    print(f"[run365days] start-up failed: {message}", file=sys.stderr)
    traceback.print_exception(exc, file=sys.stderr)
    try:
        from flask import Flask, jsonify

        fallback = Flask(__name__)

        @fallback.route("/", defaults={"path": ""})
        @fallback.route("/<path:path>")
        def report(path):
            return jsonify({"status": "error", "error": message}), 500

        return fallback
    except Exception:  # noqa: BLE001 - flask itself missing
        payload = json.dumps({"status": "error", "error": message}).encode()

        def app(environ, start_response):
            start_response("500 Internal Server Error", [("Content-Type", "application/json")])
            return [payload]

        return app


try:
    _load_package_from_source()
    from run365days.api.app import create_app

    app = create_app(os.environ.get("RUN365_DB_PATH", DEFAULT_DB), graphiql=True)
except Exception as exc:  # noqa: BLE001 - surface any start-up failure
    app = _error_app(exc)
