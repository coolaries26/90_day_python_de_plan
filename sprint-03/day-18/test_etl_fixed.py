"""
test_etl_fixed.py — Day 18 | Fixed ETL Tests
=============================================
Fixes test_successful_etl and test_retry_on_failure from Day 13
using proper fixtures and complete mocking.

Run: pytest test_etl_fixed.py -v
Target: 3/3 passing
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pandas as pd
import pytest
import logging

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))   # for logger.py
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-12"))   # For resilient ETL pipeline

from etl_resilient import ResilientETLPipeline
from logger import get_pipeline_logger

logger = get_pipeline_logger("test_etl_fixed")

# ── Test 1: Fully provided — study thispytest.raisesrn ───────────────────────────────
def test_max_retries_exceeded():
    """
    Test 3: After max_retries, pipeline pytest.raises.raises exception.
    This test was already passing on Day 13 — kept here as reference.

    Pattern: patch as context managers → explicit control over scope
    """
    with patch("etl_resilient.get_engine"), \
         patch("etl_resilient.pd.read_sql",
               side_effect=Exception("Persistent DB error")), \
         patch.object(ResilientETLPipeline, "export_csv"), \
         patch("etl_resilient.time.sleep"):   # ← no real sleeping in tests

        pipeline = ResilientETLPipeline(max_retries=2)

        with pytest.raises(Exception, match="Persistent DB error"):
            pipeline.run()


# ── Test 2: Fix this yourself ─────────────────────────────────────────────────
def test_successful_etl(sample_df: pd.DataFrame, etl_config):
    """
    Test 1: Happy path — pipeline runs, loads data, exports CSV.

    HINTS:
      - Patch: "etl_resilient.get_engine" (no return value needed)
      - Patch: "etl_resilient.pd.read_sql" return_value=sample_df
      - Patch.object: ResilientETLPipeline, "load"    ← mock the DB write
      - Patch.object: ResilientETLPipeline, "export_csv" ← mock file write

    WHY patch load AND export_csv separately?
      - load() calls df.to_sql() → needs real engine → mock it
      - export_csv() writes to disk → use tmp_path via etl_config fixture
        OR mock it entirely to keep test focused on retry logic

    ASSERTIONS to make:
      - pipeline.run() does not raise
      - mock_read_sql was called exactly once (no retries on success)
      - mock_load was called exactly once
      - result is the sample_df (or check row count)

    HINTS for assertions:
      mock_read_sql.assert_called_once()
      mock_load.assert_called_once()
    """
    # YOUR CODE HERE
    with patch("etl_resilient.get_engine"), \
         patch("etl_resilient.pd.read_sql", return_value=sample_df), \
         patch.object(ResilientETLPipeline, "load"), \
         patch.object(ResilientETLPipeline, "export_csv"):

        pipeline = ResilientETLPipeline(max_retries=2)
        result = pipeline.run()  # Should succeed on first attempt
        pd.read_sql.assert_called_once()  # type: ignore[attr-defined]
        pipeline.load.assert_called_once()  # type: ignore[attr-defined]
        pipeline.export_csv.assert_called_once()  # type: ignore[attr-defined]



# ── Test 3: Fix this yourself ─────────────────────────────────────────────────
def test_retry_on_failure(sample_df: pd.DataFrame):
    """
    Test 2: Retry logic — fails first attempt, succeeds on second.

    HINTS:
      - Patch "etl_resilient.get_engine"
      - Patch "etl_resilient.pd.read_sql" with side_effect:
            [Exception("DB connection failed"), sample_df]
            ↑ fail first call, succeed second call
      - Patch.object ResilientETLPipeline, "load"
      - Patch.object ResilientETLPipeline, "export_csv"
      - Patch "etl_resilient.time.sleep"  ← prevents 2s wait in test

    ASSERTIONS:
      - pipeline.run() does not raise
      - mock_read_sql.call_count == 2  (called twice — one fail, one success)
      - mock_load.call_count == 1      (load only happens after successful extract)

    IMPORTANT: max_retries=3 so there is room for the retry to succeed.
    """
    # YOUR CODE HERE
    with patch("etl_resilient.get_engine"), \
             patch("etl_resilient.pd.read_sql", side_effect=[Exception("DB connection failed"), sample_df]), \
             patch.object(ResilientETLPipeline, "load"), \
             patch.object(ResilientETLPipeline, "export_csv"), \
             patch("etl_resilient.time.sleep"):

        pipeline = ResilientETLPipeline(max_retries=3)
        pipeline.run()  # Should succeed on 2nd attempt
        assert pd.read_sql.call_count == 2  # type: ignore[attr-defined]
        assert pipeline.load.call_count == 1  # type: ignore[attr-defined]
        assert pipeline.export_csv.call_count == 1  # type: ignore[attr-defined]

    

        pipeline.load.assert_called_once()  # type: ignore[attr-defined]    
  
# ── Parametrize example — fully provided ─────────────────────────────────────
@pytest.mark.parametrize("max_retries,expected_calls", [
    (1, 1),   # 1 retry allowed → read_sql called once then raises
    (2, 2),   # 2 retries → called twice then raises
    (3, 3),   # 3 retries → called three times then raises
])
def test_retry_count_matches_max_retries(max_retries: int, expected_calls: int):
    """
    Parametrize: verify retry count equals max_retries exactly.
    This single parametrized test replaces 3 separate test functions.
    """
    with patch("etl_resilient.get_engine"), \
         patch("etl_resilient.pd.read_sql",
               side_effect=Exception("always fails")), \
         patch.object(ResilientETLPipeline, "export_csv"), \
         patch("etl_resilient.time.sleep") as mock_sleep:

        pipeline = ResilientETLPipeline(max_retries=max_retries)

        with pytest.raises(Exception):
            pipeline.run()

        assert pd.read_sql.call_count == expected_calls   # type: ignore[attr-defined]
        # time.sleep called (max_retries - 1) times — no sleep after last failure
        assert mock_sleep.call_count == max_retries - 1

# ── caplog Test  ───────────────────────────────
def test_pipeline_logs_success(sample_df: pd.DataFrame, caplog):
#    import logging
    with patch("etl_resilient.get_engine"), \
         patch("etl_resilient.pd.read_sql", return_value=sample_df), \
         patch.object(ResilientETLPipeline, "load"), \
         patch.object(ResilientETLPipeline, "export_csv"), \
         patch("etl_resilient.time.sleep"):

        pipeline = ResilientETLPipeline(max_retries=2)

        with caplog.at_level(logging.INFO):
            pipeline.run()
        # Assert success message appears in logs
        messages = [r.message for r in caplog.records]
        assert any(
            "success" in record.message.lower() or
            "SUCCESS" in record.message
            for record in caplog.records
        ), f"Expected SUCCESS in logs. Got: {messages}"