"""Flask application factory."""

from pathlib import Path

from flask import Flask, g, jsonify
from strawberry.flask.views import GraphQLView

from run365days.api import db
from run365days.api.schema import schema

GRAPHQL_PATH = "/api/graphql"
HEALTH_PATH = "/api/health"

SESSION_KEY = "run365_sessions"
"""Key on Flask's ``g`` holding the sessions a request opened, for its teardown.

On ``g`` rather than closed over by the view, because the view instance is
rebuilt per request while the teardown is registered once on the app, and ``g``
is the one name both can reach for the same request.

A list rather than a single session so the teardown is total: today
``get_context`` runs once per request, but a list closes whatever was opened
instead of closing one and silently leaking the rest if that ever stops being
true.
"""


class _SessionView(GraphQLView):
    """GraphQL view that opens one database session per request."""

    def __init__(self, engine, **kwargs):
        super().__init__(**kwargs)
        self._engine = engine

    def get_context(self, request, response):
        session = db.Session(self._engine)
        # Handed to `teardown_request` rather than to
        # `response.call_on_close`, which was the same intent aimed at the one
        # hook the test client does not run: `call_on_close` fires when the
        # WSGI iterable is closed, and `app.test_client()` never closes it.
        # Measured on this branch, 40 requests per transport with every
        # session tracked: a real WSGI server opened 40 and closed 40, the test
        # client opened 40 and closed none -- so sessions accumulated until the
        # pool ran out, which CUI-0005 saw at request 134 and a re-run here saw
        # at 100. Production was never the problem (the real server was always
        # correct); what was wrong was that the two transports ended a request
        # differently, which made a paging test look like a paging bug.
        #
        # `teardown_request` runs on both, on every request, and whether or not
        # the view raised, so the lifecycle stops depending on the transport.
        g.setdefault(SESSION_KEY, []).append(session)
        return {"request": request, "response": response, "session": session}


def create_app(db_path: Path | str | None = None, graphiql: bool = True) -> Flask:
    """Build the API.

    Args:
        db_path: SQLite file; defaults to ``RUN365_DB_PATH`` or the config path.
        graphiql: Serve the GraphiQL IDE on GET requests to the endpoint.

    Returns:
        A Flask app exposing ``/api/graphql`` and ``/api/health``.
    """
    path = db.resolve_db_path(db_path)
    engine = db.make_engine(path)
    app = Flask(__name__)
    app.config["RUN365_DB_PATH"] = str(path)

    view = _SessionView.as_view(
        "graphql", engine=engine, schema=schema, graphql_ide="graphiql" if graphiql else None
    )
    app.add_url_rule(GRAPHQL_PATH, view_func=view, methods=["GET", "POST"])

    @app.teardown_request
    def close_sessions(_exception):
        # Runs on every request, so most of the time this pops nothing: the
        # health endpoint below opens no session at all.
        for session in g.pop(SESSION_KEY, ()):
            session.close()

    @app.get(HEALTH_PATH)
    def health():
        return jsonify({"status": "ok", "db": str(path)})

    return app
