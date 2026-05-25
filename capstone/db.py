#!/usr/bin/env python3
"""
capstone/db.py — Database connection for ecommerce_db
Mirrors sprint-01/day-02/db_utils.py but for ecommerce_db.
"""
import os
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

try:
    from dotenv import load_dotenv
    for candidate in [Path(__file__).parent.parent, Path.home() / "python-de-journey"]:
        env = candidate / ".env"
        if env.is_file():
            load_dotenv(dotenv_path=env, override=False)
            break
except ImportError:
    pass

_engine: Engine | None = None

def get_ecommerce_engine() -> Engine:
    global _engine
    if _engine is None:
        pwd = quote_plus(os.environ.get("ECOMMERCE_DB_PASSWORD", ""))
        url = (
            f"postgresql+psycopg2://"
            f"{os.environ.get('ECOMMERCE_DB_USER', 'appuser')}:{pwd}"
            f"@{os.environ.get('ECOMMERCE_DB_HOST', '127.0.0.1')}"
            f":{os.environ.get('ECOMMERCE_DB_PORT', 5432)}"
            f"/{os.environ.get('ECOMMERCE_DB_NAME', 'ecommerce_db')}"
        )
        _engine = create_engine(
            url,
            pool_size=5,
            max_overflow=2,
            pool_pre_ping=True,
            pool_recycle=1800,
            echo=False,
        )
    return _engine

def dispose_ecommerce_engine() -> None:
    global _engine
    if _engine:
        _engine.dispose()
        _engine = None