#!/usr/bin/env python3
"""
db_utils.py — Reusable Database Utilities
==========================================
Centralised connection factory for the entire 90-day program.
Import this module in every script that needs a DB connection.

Usage:
    from db_utils import get_connection, get_engine, execute_query

Design:
    - Context managers ensure guaranteed connection cleanup (no leaks)
    - Passwords loaded from .env — never hardcoded
    - SQLAlchemy engine singleton — avoids creating multiple pools
    - psycopg2 ThreadedConnectionPool — safe for concurrent scripts
    - All public functions have type hints + docstrings
"""

import os
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator
from urllib.parse import quote_plus

import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ── Load .env from project root ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    _here = Path(__file__).resolve().parent
    for _candidate in [_here, _here.parent, _here.parent.parent,
                       Path.home() / "python-de-journey"]:
        _env = _candidate / ".env"
        if _env.is_file():
            load_dotenv(dotenv_path=_env, override=False)
            break
except ImportError:
    pass

log = logging.getLogger(__name__)


# ── DB config ─────────────────────────────────────────────────────────────────
def _db_config() -> dict[str, Any]:
    required = {"DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"}
    missing = required - set(os.environ)
    if missing:
        raise EnvironmentError(
            f"Missing env vars: {missing}. "
            f"Ensure .env is at project root with all DB_ keys."
        )
    return {
        "host":     os.environ["DB_HOST"],
        "port":     int(os.environ["DB_PORT"]),
        "dbname":   os.environ["DB_NAME"],
        "user":     os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }


# ── Connection pool singleton ─────────────────────────────────────────────────
_pool: pg_pool.ThreadedConnectionPool | None = None


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        cfg = _db_config()
        _pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=int(os.getenv("DB_POOL_SIZE", 5)),
            connect_timeout=10,
            **cfg,
        )
        log.debug("psycopg2 pool created (maxconn=%s)", os.getenv("DB_POOL_SIZE", 5))
    return _pool


@contextmanager
def get_connection(
    autocommit: bool = False,
    cursor_factory=None,
) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager: yields a psycopg2 connection from the pool.
    Commits on clean exit, rolls back on exception.
    Always returns connection to pool — guaranteed no leak.

    Args:
        autocommit:     Each statement commits immediately if True.
        cursor_factory: e.g. psycopg2.extras.RealDictCursor

    Example:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT now()")
                print(cur.fetchone())
    """
    pool = _get_pool()
    conn = pool.getconn()
    conn.cursor_factory = cursor_factory
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)          # ← always runs; prevents pool exhaustion


# ── SQLAlchemy engine singleton ───────────────────────────────────────────────
_engine: Engine | None = None


def get_engine() -> Engine:
    """
    Return the shared SQLAlchemy engine (created once, reused).
    Always call dispose_engine() at process exit to avoid leaks.
    """
    global _engine
    if _engine is None:
        cfg = _db_config()
        pwd = quote_plus(cfg["password"])   # encode @, !, #, etc.
        url = (
            f"postgresql+psycopg2://"
            f"{cfg['user']}:{pwd}"
            f"@{cfg['host']}:{cfg['port']}"
            f"/{cfg['dbname']}"
        )
        _engine = create_engine(
            url,
            pool_size=int(os.getenv("DB_POOL_SIZE", 5)),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 2)),
            pool_pre_ping=True,
            pool_recycle=int(os.getenv("DB_POOL_RECYCLE", 1800)),
            echo=False,
        )
        log.debug("SQLAlchemy engine created")
    return _engine


def dispose_engine() -> None:
    """Close all SQLAlchemy pool connections. Call at process exit."""
    global _engine
    if _engine:
        _engine.dispose()
        _engine = None


def close_pool() -> None:
    """Close all psycopg2 pool connections. Call at process exit."""
    global _pool
    if _pool and not _pool.closed:
        _pool.closeall()
        _pool = None


# ── Convenience helpers ───────────────────────────────────────────────────────
def execute_query(
    sql: str,
    params: tuple | dict | None = None,
    as_dict: bool = False,
) -> list[tuple] | list[dict]:
    """
    Execute a SELECT and return all rows.

    Args:
        sql:     SQL with %s or %(name)s placeholders — NEVER f-string user input
        params:  Query parameters (SQL-injection safe)
        as_dict: Return list of dicts instead of list of tuples

    Example:
        rows = execute_query(
            "SELECT title FROM film WHERE rating = %s LIMIT 5",
            params=("PG",), as_dict=True
        )
    """
    factory = psycopg2.extras.RealDictCursor if as_dict else None
    with get_connection(cursor_factory=factory) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [dict(r) for r in rows] if as_dict else rows


def execute_scalar(sql: str, params: tuple | dict | None = None) -> Any:
    """Execute a query and return a single scalar value."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
    return result[0] if result else None


def execute_dml(sql: str, params: tuple | dict | None = None) -> int:
    """
    Execute INSERT / UPDATE / DELETE and return affected row count.
    Commits automatically on success, rolls back on error.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount
