"""CUI-0005: a request must release its database session in every environment.

The view used to close the per-request session from ``response.call_on_close``.
That callback only runs when the WSGI server closes the response iterable, so
the session's lifetime depended on what sat in front of the app: a real server
closed it, Flask's ``test_client`` never did. Sessions then piled up until the
pool was exhausted, and the symptom was easy to misread -- the request still
answered ``200``, just slowly and with a GraphQL error in place of data.

These tests pin the session to the *request*, and check that the test client
and a real WSGI server agree. The server below binds 127.0.0.1 and is reached
over a loopback ``HTTPConnection``, which deliberately bypasses any proxy: the
real response-iterable path is exercised without opening outbound network.
"""

import json
import threading
import time
from dataclasses import dataclass, field
from http.client import HTTPConnection

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session
from werkzeug.serving import make_server

from run365days.api import db
from run365days.api.app import GRAPHQL_PATH, create_app
from run365days.export.sqlite import sqlite_url, write_sqlite

# A deliberately tiny pool: exhaustion then shows up on the third request
# instead of the 134th, and a leak fails in a second rather than in minutes.
POOL_SIZE = 2
POOL_TIMEOUT_SEC = 1
LOOP_REQUESTS = POOL_SIZE * 5
DRAIN_TIMEOUT_SEC = 2
POLL_SEC = 0.01

META_QUERY = "{ meta { year } }"
EXPECTED_YEAR = 2021
HTTP_OK = 200
HTTP_BAD_REQUEST = 400
JSON_HEADERS = {"Content-Type": "application/json"}


@dataclass
class Harness:
    """An app whose engine, opened sessions and closed sessions the test can inspect."""

    app: object
    engine: Engine
    opened: list[Session] = field(default_factory=list)
    closed: list[Session] = field(default_factory=list)


def _small_engine(db_file) -> Engine:
    """The production engine, but with a pool small enough to exhaust quickly."""
    return create_engine(
        sqlite_url(db_file, read_only=True),
        pool_size=POOL_SIZE,
        max_overflow=0,
        pool_timeout=POOL_TIMEOUT_SEC,
    )


def _tracking_factory(opened: list, closed: list):
    """Return a ``db.Session`` stand-in that records every open and every close."""

    def make_session(engine: Engine) -> Session:
        session = Session(engine)
        inner_close = session.close

        def close_and_record() -> None:
            closed.append(session)
            inner_close()

        session.close = close_and_record
        opened.append(session)
        return session

    return make_session


@pytest.fixture
def harness(sample_records, tmp_path, monkeypatch) -> Harness:
    """An app on a real database file, with a small pool and tracked sessions."""
    path = tmp_path / "run365.db"
    write_sqlite(sample_records, path)
    captured: list[Engine] = []
    opened: list[Session] = []
    closed: list[Session] = []

    def make_engine(db_file):
        captured.append(_small_engine(db_file))
        return captured[-1]

    monkeypatch.setattr(db, "make_engine", make_engine)
    monkeypatch.setattr(db, "Session", _tracking_factory(opened, closed))
    app = create_app(path, graphiql=False)
    app.testing = True
    return Harness(app=app, engine=captured[0], opened=opened, closed=closed)


@pytest.fixture
def wsgi_port(harness):
    """Serve the app on a loopback port for the duration of one test."""
    server = make_server("127.0.0.1", 0, harness.app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        thread.join(timeout=DRAIN_TIMEOUT_SEC)


def drained(engine: Engine) -> int:
    """Connections still checked out, after giving an async closer a moment to run."""
    deadline = time.monotonic() + DRAIN_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if engine.pool.checkedout() == 0:
            return 0
        time.sleep(POLL_SEC)
    return engine.pool.checkedout()


def settled(harness: Harness) -> int:
    """Sessions still open, after giving an async closer a moment to run."""
    deadline = time.monotonic() + DRAIN_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if len(harness.closed) == len(harness.opened):
            return 0
        time.sleep(POLL_SEC)
    return len(harness.opened) - len(harness.closed)


def assert_meta_payload(raw: bytes) -> None:
    """A healthy answer carries data and no errors; a drained pool answers 200 with errors."""
    payload = json.loads(raw)
    assert "errors" not in payload, payload["errors"]
    assert payload["data"]["meta"]["year"] == EXPECTED_YEAR


def post_via_wsgi_server(port: int, body: bytes) -> tuple[int, bytes]:
    """Send *body* to the loopback server, exercising the real WSGI close path."""
    conn = HTTPConnection("127.0.0.1", port, timeout=DRAIN_TIMEOUT_SEC + POOL_TIMEOUT_SEC)
    try:
        conn.request("POST", GRAPHQL_PATH, body=body, headers=JSON_HEADERS)
        response = conn.getresponse()
        return response.status, response.read()
    finally:
        conn.close()


def test_test_client_returns_the_connection_after_one_request(harness):
    response = harness.app.test_client().post(GRAPHQL_PATH, json={"query": META_QUERY})
    assert response.status_code == HTTP_OK
    assert_meta_payload(response.data)
    assert drained(harness.engine) == 0
    assert settled(harness) == 0


def test_test_client_survives_more_requests_than_the_pool_holds(harness):
    client = harness.app.test_client()
    for _ in range(LOOP_REQUESTS):
        response = client.post(GRAPHQL_PATH, json={"query": META_QUERY})
        assert response.status_code == HTTP_OK
        assert_meta_payload(response.data)
    assert drained(harness.engine) == 0
    assert settled(harness) == 0


def test_real_wsgi_server_survives_more_requests_than_the_pool_holds(harness, wsgi_port):
    body = json.dumps({"query": META_QUERY}).encode()
    for _ in range(LOOP_REQUESTS):
        status, raw = post_via_wsgi_server(wsgi_port, body)
        assert status == HTTP_OK
        assert_meta_payload(raw)
    assert drained(harness.engine) == 0
    assert settled(harness) == 0


def test_both_environments_open_and_close_the_same_number_of_sessions(harness, wsgi_port):
    harness.app.test_client().post(GRAPHQL_PATH, json={"query": META_QUERY})
    post_via_wsgi_server(wsgi_port, json.dumps({"query": META_QUERY}).encode())
    assert settled(harness) == 0
    assert len(harness.opened) == 2


def test_session_is_closed_when_the_request_never_reaches_a_resolver(harness):
    """A rejected body makes Strawberry discard the response ``call_on_close`` was bound to."""
    response = harness.app.test_client().post(GRAPHQL_PATH, json={"notAQuery": 1})
    assert response.status_code == HTTP_BAD_REQUEST
    assert len(harness.opened) == 1
    assert settled(harness) == 0
