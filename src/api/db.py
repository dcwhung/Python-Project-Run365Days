"""Engine and session helpers for the read-only ``run365.db``."""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

from run365days.common import config
from run365days.export.sqlite import sqlite_url

DB_PATH_ENV = "RUN365_DB_PATH"
"""Environment variable that overrides the database location (used on Vercel)."""


def resolve_db_path(explicit: Path | str | None = None) -> Path:
    """Pick the database path: explicit argument, then env var, then config default."""
    if explicit:
        return Path(explicit)
    return Path(os.environ.get(DB_PATH_ENV, config.PROCESSED_DB))


def make_engine(path: Path) -> Engine:
    """Create a read-only SQLite engine for *path*.

    Raises:
        FileNotFoundError: If the database has not been exported yet.
    """
    if not path.exists():
        raise FileNotFoundError(f"{path} not found; run `run365-export` first")
    return create_engine(sqlite_url(path, read_only=True))


@contextmanager
def session_scope(engine: Engine) -> Iterator[Session]:
    """Yield a session that is always closed, never committed (read-only API)."""
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()
