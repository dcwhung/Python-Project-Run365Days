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

CUI-0021 finishes the sentence that docstring starts. Fixing the leak stopped
the pool from draining by accident, but said nothing about what the API answers
when it drains for some other reason -- a slow disk, a burst of traffic, a
serverless instance holding connections open. That answer was still ``200``,
so a harness that reads status codes, and every alert built on 5xx rates, saw a
healthy service. The last group of tests below draws the line: the API answers
``200`` when it has *answered* the client, whether that answer is data or a
rejection, and 5xx when it never managed to answer at all.
"""

import json
import threading
import time
from collections.abc import Callable
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
HTTP_SERVER_ERROR = 500
JSON_HEADERS = {"Content-Type": "application/json"}

# Two documents the API answers by rejecting them. Neither reaches a database,
# and neither is the backend's fault, so both must stay 200 once CUI-0021 starts
# marking failures: a rejection is an answer.
OUT_OF_RANGE_QUERY = "{ activities(limit: 0) { id } }"
UNKNOWN_FIELD_QUERY = "{ noSuchField }"


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


def _tracking_factory(opened: list, closed: list) -> Callable[[Engine], Session]:
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

    def make_engine(db_file) -> Engine:
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


# ── CUI-0021: a broken backend must not answer 200 ─────────────────────────
def hold_whole_pool(engine: Engine) -> list:
    """Check out every connection the pool holds, so the next request gets none."""
    return [engine.connect() for _ in range(POOL_SIZE)]


def post_with_pool_exhausted(harness: Harness, query: str) -> tuple[int, bytes]:
    """Send *query* while no connection is free, then give the pool back."""
    held = hold_whole_pool(harness.engine)
    try:
        response = harness.app.test_client().post(GRAPHQL_PATH, json={"query": query})
        return response.status_code, response.data
    finally:
        for connection in held:
            connection.close()


def test_exhausted_pool_answers_with_a_server_error(harness):
    """The pool giving out is the backend failing, and a status code must say so."""
    status, _ = post_with_pool_exhausted(harness, META_QUERY)
    assert status == HTTP_SERVER_ERROR


def test_exhausted_pool_still_returns_the_graphql_error_body(harness):
    """The status changes; the payload does not.

    ``graphql-request`` parses the body before it looks at the status and reads
    its thrown error's message out of ``errors[0]``, so dropping the payload on
    a 5xx would trade a named cause for "GraphQL Error (Code: 500)".
    """
    _, raw = post_with_pool_exhausted(harness, META_QUERY)
    payload = json.loads(raw)
    assert payload["data"] is None
    assert "QueuePool" in payload["errors"][0]["message"]


def test_exhausted_pool_answers_the_same_way_through_a_real_server(harness, wsgi_port):
    """The status is set on the response Strawberry returns, not by the test client."""
    held = hold_whole_pool(harness.engine)
    try:
        status, raw = post_via_wsgi_server(wsgi_port, json.dumps({"query": META_QUERY}).encode())
    finally:
        for connection in held:
            connection.close()
    assert status == HTTP_SERVER_ERROR
    assert "QueuePool" in json.loads(raw)["errors"][0]["message"]


@pytest.mark.parametrize("query", [OUT_OF_RANGE_QUERY, UNKNOWN_FIELD_QUERY])
def test_a_rejected_request_is_an_answer_and_stays_200(harness, query):
    """An argument the API refuses, and a field it does not have, are both answers."""
    response = harness.app.test_client().post(GRAPHQL_PATH, json={"query": query})
    assert response.status_code == HTTP_OK
    assert json.loads(response.data)["errors"]


def test_pool_recovers_and_the_status_goes_back_to_200(harness):
    """The failure marking is per request: it must not stick to the app."""
    post_with_pool_exhausted(harness, META_QUERY)
    response = harness.app.test_client().post(GRAPHQL_PATH, json={"query": META_QUERY})
    assert response.status_code == HTTP_OK
    assert_meta_payload(response.data)
