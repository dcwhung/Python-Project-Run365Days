"""Vercel serverless entry point: exposes the Flask app as `app`.

Vercel's Python runtime looks for a WSGI callable named `app` in files
under api/. The database path comes from RUN365_DB_PATH (set in
vercel.json) and the file itself is bundled via `includeFiles`.
"""

from run365days.api.app import create_app

app = create_app(graphiql=True)
