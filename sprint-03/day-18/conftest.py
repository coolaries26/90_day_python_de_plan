"""
conftest.py — Shared pytest fixtures for Day 18 tests
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
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-12"))   # For resilient ETL pipeline
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
    """
    A small realistic DataFrame representing customer ETL output.
    Used in any test that needs a DataFrame without hitting the DB.
    Scope: function (fresh copy per test — prevents mutation between tests)
    """
    return pd.DataFrame({
        "customer_id":   [1, 2, 3, 148, 526],
        "full_name":     ["Alice A", "Bob B", "Carol C", "Eleanor Hunt", "Karl Seal"],
        "total_rentals": [10, 8, 15, 45, 42],
        "total_spend":   [45.50, 32.00, 67.25, 211.55, 208.58],
        "segment":       ["Bronze", "Bronze", "Silver", "Platinum", "Platinum"],
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
        source_table="customer",
        target_table="analytics_test_output",
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
    from etl_resilient import ResilientETLPipeline

    with patch("etl_resilient.get_engine") as mock_get_engine, \
         patch("etl_resilient.pd.read_sql", return_value=sample_df), \
         patch.object(ResilientETLPipeline, "export_csv"):
        mock_get_engine.return_value = MagicMock()
        pipeline = ResilientETLPipeline(max_retries=etl_config.max_retries)
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