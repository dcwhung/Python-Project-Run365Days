"""Flask application factory."""

from pathlib import Path
from typing import Any

from flask import Flask, Request, Response, g, jsonify
from sqlalchemy import Engine
from sqlalchemy.orm import Session
from strawberry.flask.views import GraphQLView
from strawberry.types import ExecutionResult

from run365days.api import db
from run365days.api.schema import ClientArgumentError, graphiql_enabled, schema

GRAPHQL_PATH = "/api/graphql"
HEALTH_PATH = "/api/health"
SESSION_KEY = "run365_session"
"""Attribute on Flask's ``g`` holding this request's session."""

HTTP_SERVER_ERROR = 500
"""Status for a request the backend could not answer (CUI-0021).

500 rather than 503, though a drained pool is exactly the transient condition
503 was written for. Splitting the failures into "retry may help" and "retry
will not" needs a second list of exception types to keep correct, and nothing
downstream would read it: the front end's TanStack Query retries on any thrown
error regardless of status, and alerting keys on the 5xx class. One honest
class of failure now; the split is worth making when something acts on it.
"""


def _backend_failed(result: ExecutionResult) -> bool:
    """Report whether any error in *result* is the backend's own failure.

    The line is the one drawn everywhere else in this project: an answer that
    arrived but cannot be used is a data problem, and an answer that never
    arrived is an environment problem.

    An error with no ``original_error`` came from graphql-core itself while
    parsing or validating the document -- the API read what the client sent and
    answered "no". A :class:`~run365days.api.schema.ClientArgumentError` is the
    same answer raised one layer further in, by a resolver checking a bound the
    schema publishes. Both are answers, and both keep their ``200``.

    Anything else reached a resolver and the resolver could not finish: a pool
    timeout, an unreadable database file, a bug of ours. The client is left
    with a gap rather than an answer, and a gap reported as ``200`` is what
    every status-code check in front of this API will read as success.

    Args:
        result: One executed operation, errors included.

    Returns:
        ``True`` if at least one error is a backend failure.
    """
    return any(
        error.original_error is not None
        and not isinstance(error.original_error, ClientArgumentError)
        for error in result.errors or ()
    )


class _SessionView(GraphQLView):
    """GraphQL view that opens one database session per request."""

    def __init__(self, engine: Engine, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._engine = engine

    def get_context(self, request: Request, response: Response) -> dict[str, Any]:
        return {"request": request, "response": response, "session": self._request_session()}

    def execute_operation(
        self,
        request: Request,
        context: dict[str, Any],
        root_value: object | None,
        sub_response: Response,
    ) -> ExecutionResult | list[ExecutionResult]:
        """Run the operation, then let a backend failure reach the status code.

        This override is where the API stops following the GraphQL convention
        that every executed document answers ``200`` with its errors in the
        payload. That convention is right for a public endpoint serving clients
        it has never met, which must be able to read a partial result; it is
        wrong here. This endpoint serves one front end, and the cost of the
        convention was measured: with the pool drained, a check of 400 requests
        reported 400 successes and quietly returned 341 of 365 activities.

        Strawberry puts the decision exactly here. ``sub_response`` is the
        response object the view later fills with the encoded payload, so
        setting its status leaves the body untouched -- errors still arrive in
        the shape a GraphQL client expects, and only the envelope changes.
        Errors reach this point as ``GraphQLError``, still carrying the
        exception that caused them; by ``create_response`` they have been
        flattened to dicts and the cause is gone.

        Args:
            request: The incoming Flask request.
            context: The context handed to resolvers.
            root_value: Strawberry's root value, unused here.
            sub_response: The response the view will return.

        Returns:
            The execution result, or one per operation for a batched document.
        """
        result = super().execute_operation(
            request=request, context=context, root_value=root_value, sub_response=sub_response
        )
        executed = result if isinstance(result, list) else [result]
        if any(_backend_failed(one) for one in executed):
            sub_response.status_code = HTTP_SERVER_ERROR
        return result

    def _request_session(self) -> Session:
        """Return this request's session, opening it on first use.

        The session hangs off ``g`` and is closed by ``teardown_request``, not
        by ``response.call_on_close``. Two reasons the response is the wrong
        anchor: that callback only runs once a WSGI server closes the response
        iterable, which ``app.test_client()`` never does, so sessions piled up
        under test until the pool was exhausted; and Strawberry throws the
        response away when it raises an HTTPException, taking the callback with
        it. Flask pops the request context in every environment and on every
        exit path, so the teardown hook always runs.
        """
        session = g.get(SESSION_KEY)
        if session is None:
            session = db.Session(self._engine)
            setattr(g, SESSION_KEY, session)
        return session


def create_app(db_path: Path | str | None = None, graphiql: bool | None = None) -> Flask:
    """Build the API.

    Args:
        db_path: SQLite file; defaults to ``RUN365_DB_PATH`` or the config path.
        graphiql: Serve the GraphiQL IDE on GET requests to the endpoint.
            ``None`` defers to ``RUN365_GRAPHIQL``, the same flag that opens
            introspection, so the IDE and the schema access it needs cannot
            disagree. ``True`` or ``False`` overrides the flag for this app.

    Returns:
        A Flask app exposing ``/api/graphql`` and ``/api/health``.
    """
    path = db.resolve_db_path(db_path)
    engine = db.make_engine(path)
    app = Flask(__name__)
    app.config["RUN365_DB_PATH"] = str(path)

    # Read per call, not as a default expression, which would freeze it at import.
    serve_ide = graphiql_enabled() if graphiql is None else graphiql
    view = _SessionView.as_view(
        "graphql", engine=engine, schema=schema, graphql_ide="graphiql" if serve_ide else None
    )
    app.add_url_rule(GRAPHQL_PATH, view_func=view, methods=["GET", "POST"])

    @app.teardown_request
    def close_session(exc: BaseException | None) -> None:
        """Release this request's session, whether it ended in a response or an error."""
        session = g.pop(SESSION_KEY, None)
        if session is not None:
            session.close()

    @app.get(HEALTH_PATH)
    def health() -> Response:
        return jsonify({"status": "ok", "db": str(path)})

    return app
