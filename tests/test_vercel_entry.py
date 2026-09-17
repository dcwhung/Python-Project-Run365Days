"""The Vercel entry point, api/graphql.py.

Split out of tests/test_api.py (CUI-0045): the entry is a deploy shim, not part
of the src/api/ package the rest of that file covers. Nothing here was rewritten
in the move.
"""

import importlib.util
import json
import sys
from pathlib import Path

from run365days.api import db
from run365days.api.app import GRAPHQL_PATH, HEALTH_PATH
from run365days.export.sqlite import write_sqlite

# Also defined in tests/test_api.py, which still needs it for the W-008
# introspection tests that read the same flag through the normal app.
GRAPHIQL_ENV = "RUN365_GRAPHIQL"
VERCEL_ENTRY = Path(__file__).resolve().parents[1] / "api" / "graphql.py"


# ── AU-001: GraphiQL is off unless the environment asks for it ─────────────
def _load_vercel_entry(monkeypatch, db_path, graphiql_env=None):
    monkeypatch.setenv(db.DB_PATH_ENV, str(db_path))
    if graphiql_env is None:
        monkeypatch.delenv(GRAPHIQL_ENV, raising=False)
    else:
        monkeypatch.setenv(GRAPHIQL_ENV, graphiql_env)
    spec = importlib.util.spec_from_file_location("run365_vercel_entry", VERCEL_ENTRY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_graphiql_is_disabled_by_default(monkeypatch, sample_records, tmp_path):
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    module = _load_vercel_entry(monkeypatch, path)
    r = module.app.test_client().get(GRAPHQL_PATH, headers={"Accept": "text/html"})
    assert b"graphiql" not in r.data.lower()


def test_graphiql_is_served_when_the_env_var_is_on(monkeypatch, sample_records, tmp_path):
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    module = _load_vercel_entry(monkeypatch, path, graphiql_env="1")
    r = module.app.test_client().get(GRAPHQL_PATH, headers={"Accept": "text/html"})
    assert r.status_code == 200
    assert b"graphiql" in r.data.lower()


# ── CUI-0035: the Vercel entry's start-up failure path ─────────────────────
# api/graphql.py keeps a reporting app so that a deploy which cannot start still
# says why on /api/health instead of only in the function logs. That app runs
# only once start-up has already failed, which is also the moment nobody is
# placed to notice it is itself broken -- so it is pinned here, not in
# production. `_load_vercel_entry` above exec_module()s the real file, so these
# exercise the shipped entry rather than a copy of it.
START_UP_ERROR = "no database bundled"


def _entry_with_failing_start_up(monkeypatch, tmp_path, *, flask_importable=True):
    def explode(*_args, **_kwargs):
        raise RuntimeError(START_UP_ERROR)

    monkeypatch.setattr("run365days.api.app.create_app", explode)
    if not flask_importable:
        # `from flask import ...` raises once the name maps to None, which is
        # the only way the second fallback layer is ever reached.
        monkeypatch.setitem(sys.modules, "flask", None)
    return _load_vercel_entry(monkeypatch, tmp_path / "unreadable.db")


def test_a_failed_start_up_reports_the_reason_instead_of_serving_the_api(monkeypatch, tmp_path):
    module = _entry_with_failing_start_up(monkeypatch, tmp_path)
    response = module.app.test_client().get(HEALTH_PATH)
    assert response.status_code == 500
    assert response.get_json() == {
        "status": "error",
        "error": f"RuntimeError: {START_UP_ERROR}",
    }, "the health endpoint has to name the exception, not merely fail"


def test_the_failure_report_answers_every_path_the_rewrite_sends_it(monkeypatch, tmp_path):
    # vercel.json rewrites all of /api/* to this one function, so a request that
    # would have been GraphQL has to get the reason too rather than a 404 from
    # the fallback app's own routing table.
    module = _entry_with_failing_start_up(monkeypatch, tmp_path)
    client = module.app.test_client()
    for path in (GRAPHQL_PATH, "/", "/api/anything"):
        response = client.get(path)
        assert response.status_code == 500, path
        assert START_UP_ERROR in response.get_json()["error"], path


def test_the_reason_is_still_reported_when_flask_itself_is_missing(monkeypatch, tmp_path):
    module = _entry_with_failing_start_up(monkeypatch, tmp_path, flask_importable=False)
    assert not hasattr(module.app, "test_client"), "flask is gone, so this must be raw WSGI"

    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    chunks = module.app({"PATH_INFO": HEALTH_PATH, "REQUEST_METHOD": "GET"}, start_response)

    assert captured["status"] == "500 Internal Server Error"
    assert ("Content-Type", "application/json") in captured["headers"]
    assert json.loads(b"".join(chunks)) == {
        "status": "error",
        "error": f"RuntimeError: {START_UP_ERROR}",
    }


def test_the_entry_registers_src_as_the_package_when_it_is_not_installed(
    monkeypatch, sample_records, tmp_path
):
    # The docstring's "safety net": Vercel installs the project from
    # pyproject.toml, and were that to stop putting `run365days` on the path the
    # entry registers src/ itself instead of failing to start.
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    module = _load_vercel_entry(monkeypatch, path)

    # _load_package_from_source rebinds sys.modules[PACKAGE_NAME]; registering
    # the installed module here is what restores it for the rest of the session.
    installed = sys.modules[module.PACKAGE_NAME]
    monkeypatch.setitem(sys.modules, module.PACKAGE_NAME, installed)
    monkeypatch.setattr(importlib.util, "find_spec", lambda _name: None)

    module._load_package_from_source()

    loaded = sys.modules[module.PACKAGE_NAME]
    # The dev install is editable, so `__file__` already points at src/ and
    # cannot tell the two apart; only a fresh module object shows the loader ran.
    assert loaded is not installed, "src/ was never executed, so nothing was registered"
    assert Path(__file__).resolve().parents[1] / "src" == module.PACKAGE_DIR
    assert Path(loaded.__file__) == module.PACKAGE_DIR / "__init__.py"


def test_an_installed_package_is_left_alone(monkeypatch, sample_records, tmp_path):
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    module = _load_vercel_entry(monkeypatch, path)
    installed = sys.modules[module.PACKAGE_NAME]

    module._load_package_from_source()

    assert sys.modules[module.PACKAGE_NAME] is installed, (
        "an importable package must not be re-executed from src/ behind its back"
    )
