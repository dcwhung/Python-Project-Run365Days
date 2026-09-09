"""Vercel serverless entry point: exposes the Flask app as `app`.

Vercel's Python runtime looks for a WSGI callable named `app` in files under
api/. The SQLite file is bundled next to the source via `includeFiles` in
vercel.json, so by default it is resolved relative to this repository root;
RUN365_DB_PATH overrides that.
"""

import os
from pathlib import Path

from run365days.api.app import create_app

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "processed" / "run365.db"

app = create_app(os.environ.get("RUN365_DB_PATH", DEFAULT_DB), graphiql=True)
