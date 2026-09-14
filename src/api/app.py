"""Flask application factory."""

from pathlib import Path

from flask import Flask, g, jsonify
from strawberry.flask.views import GraphQLView

from run365days.api import db
from run365days.api.schema import graphiql_enabled, schema

GRAPHQL_PATH = "/api/graphql"
HEALTH_PATH = "/api/health"
SESSION_KEY = "run365_session"
"""Attribute on Flask's ``g`` holding this request's session."""


class _SessionView(GraphQLView):
    """GraphQL view that opens one database session per request."""

    def __init__(self, engine, **kwargs):
        super().__init__(**kwargs)
        self._engine = engine

    def get_context(self, request, response):
        return {"request": request, "response": response, "session": self._request_session()}

    def _request_session(self):
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
    def close_session(exc):
        """Release this request's session, whether it ended in a response or an error."""
        session = g.pop(SESSION_KEY, None)
        if session is not None:
            session.close()

    @app.get(HEALTH_PATH)
    def health():
        return jsonify({"status": "ok", "db": str(path)})

    return app
