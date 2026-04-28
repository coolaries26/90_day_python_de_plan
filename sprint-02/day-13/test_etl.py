#!/usr/bin/env python3
import sys
from pathlib import Path
import click
from click import testing
import pytest
from unittest.mock import patch, MagicMock

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))  # For logger
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-12" ))  # For resilient ETL pipeline

from logger import get_pipeline_logger
from etl_resilient import ResilientETLPipeline   # from Day 12

logger = get_pipeline_logger("test_etl")
#----------------------------------------------------------------------------------
# This test suite validates the functionality of the Resilient ETL Pipeline, including successful execution and retry logic on failure.
@patch.object(ResilientETLPipeline, "export_csv")
@patch("etl_resilient.pd.read_sql")
@patch("etl_resilient.get_engine")

def test_successful_etl(mock_get_engine, mock_read_sql, mock_export_csv):
    """Test 1: Happy path — pipeline runs successfully."""
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine
    mock_df = MagicMock()
    mock_read_sql.return_value = mock_df

    pipeline = ResilientETLPipeline(max_retries=2)
    result = pipeline.run()

    assert result is mock_df
    mock_read_sql.assert_called_once()
    mock_export_csv.assert_called_once()   # ← verify export was called
    print("✅ Test 1 passed: Successful ETL")

@patch("etl_resilient.time.sleep")           # no real sleeping
@patch.object(ResilientETLPipeline, "export_csv")  # no file writes
@patch("etl_resilient.pd.read_sql")
@patch("etl_resilient.get_engine")
def test_retry_on_failure(mock_get_engine, mock_read_sql, mock_export_csv, mock_sleep):
    """Test 2: Retry logic — should retry on failure and succeed on next attempt."""
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    # Fail first attempt, succeed on second
    mock_read_sql.side_effect = [Exception("DB connection failed"), MagicMock()]

    pipeline = ResilientETLPipeline(max_retries=3)
    result = pipeline.run()   # Should succeed on 2nd attempt

    assert result is not None
    assert mock_read_sql.call_count == 2
    assert mock_export_csv.call_count == 1   # export only on success
    mock_sleep.assert_called_once()          # slept once between attempts
    print("✅ Test 2 passed: Retry logic works")

@patch("etl_resilient.time.sleep")
@patch("etl_resilient.pd.read_sql")
@patch("etl_resilient.get_engine")
def test_max_retries_exceeded(mock_get_engine, mock_read_sql,  mock_sleep):
    """Test 3: After max retries, it should raise an exception."""
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    # All attempts fail
    mock_read_sql.side_effect = Exception("Persistent DB error")

    pipeline = ResilientETLPipeline(max_retries=2)

    with pytest.raises(Exception):
        pipeline.run()

    assert mock_read_sql.call_count == 2
    assert mock_sleep.call_count == 1   # slept once between attempt 1 and 2
    print("✅ Test 3 passed: Raises after max retries")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
    # Note: In a real test environment, you would run pytest from the command line rather than invoking it programmatically.    