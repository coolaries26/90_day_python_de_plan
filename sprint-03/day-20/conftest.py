"""
conftest.py — Shared pytest fixtures for Day 20 tests
======================================================
pytest automatically discovers this file and makes all
fixtures defined here available to every test in the folder.

No imports needed in test files — pytest injects fixtures by name.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

# Bootstrap paths
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))   # For ETLConfig dataclass
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))   # For logger
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-14"))   # For ETL protocols (ETLConfig)

from etl_protocols import ETLConfig
from logger import get_pipeline_logger

logger = get_pipeline_logger("conftest")

# ── Fixture 0: loguru → caplog bridge ───────────────────────────────────────
import logging
from loguru import logger as loguru_logger

@pytest.fixture(autouse=True)
def propagate_loguru_to_caplog(caplog):
    """
    Bridge loguru → stdlib logging so pytest's caplog can capture it.
    autouse=True means this runs automatically for every test.
    """
    handler_id = loguru_logger.add(
        lambda msg: logging.getLogger(msg.record["name"]).handle(
            logging.LogRecord(
                name=msg.record["name"],
                level=msg.record["level"].no,
                pathname=msg.record["file"].path,
                lineno=msg.record["line"],
                msg=msg.record["message"],
                args=(),
                exc_info=None,
            )
        ),
        format="{message}",
    )
    yield
    loguru_logger.remove(handler_id)


# ── Fixture 1: sample DataFrame ───────────────────────────────────────────────
@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Returns a random sample of rows from the DataFrame."""
    return pd.DataFrame({
        "film_id": [1, 2, 3, 4, 5],
        "title": ["Academy Dinosaur", "Ace Goldfinger", "Adaptation Holes", "Affair Prejudice", "African Egg"],
        "release_year": [2006, 2006, 2006, 2006, 2006],
        "language_id": [1, 1, 1, 1, 1],
        "rental_duration": [6, 3, 7, 5, 6], 
        "rental_rate": [0.99, 4.99, 2.99, 2.99, 2.99],
        "length": [86, 48, 50, 117, 130],
        "replacement_cost": [20.99, 12.99, 18.99, 26.99, 22.99],
        "rating": ["PG", "G", "NC-17", "G", "G"],
        "last_update": ["50:59.0", "50:59.0", "50:59.0", "50:59.0", "50:59.0"],
        "category": ["Documentary", "Horror", "Documentary", "Horror", "Family"],
        "value_tier": ["Budget", "Premium", "Standard", "Standard", "Standard"]
    }) 
# ── Fixture 2: ETLConfig with tmp_path ───────────────────────────────────────
@pytest.fixture
def etl_config(tmp_path: Path) -> ETLConfig:
    """
    ETLConfig pointing to a temporary directory for output.
    tmp_path is a pytest built-in fixture — creates a unique temp dir
    per test and cleans it up automatically after the test.

    This eliminates all hardcoded path issues in tests.
    """
    return ETLConfig(
        source_table="film",
        target_table="analytics_film_sprint_test",
        max_retries=2,
        retry_wait_s=0,     # ← no sleep in tests — speeds up retry tests
        output_dir=tmp_path / "output",
    )


# ── Fixture 3: mock engine ────────────────────────────────────────────────────
@pytest.fixture
def mock_engine() -> MagicMock:
    """
    A MagicMock standing in for the SQLAlchemy engine.
    Prevents any real DB connection in unit tests.
    """
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


# ── Fixture 4: mock pipeline (patches engine + read_sql + to_sql + export) ───
@pytest.fixture
def mock_pipeline(etl_config: ETLConfig, sample_df: pd.DataFrame):
    """
    A ResilientETLPipeline with all external calls mocked.
    Use this when testing pipeline behaviour, not specific methods.

    Yields the pipeline object — patches are active during the test
    and automatically removed after.
    """
    from film_etl_pipeline import FilmETLPipeline

    with patch("film_etl_pipeline.get_engine") as mock_get_engine, \
         patch("film_etl_pipeline.pd.read_sql", return_value=sample_df), \
         patch.object(FilmETLPipeline, "export_csv"):
        mock_get_engine.return_value = MagicMock()
        pipeline = FilmETLPipeline(max_retries=etl_config.max_retries)
        yield pipeline


# ── Fixture 5: caplog at INFO level ──────────────────────────────────────────
@pytest.fixture
def info_log(caplog):
    """
    Capture log output at INFO level during a test.
    Usage in test:
        def test_something(info_log):
            run_something()
            assert "expected message" in info_log.text
    """
    import logging
    with caplog.at_level(logging.INFO):
        yield caplog