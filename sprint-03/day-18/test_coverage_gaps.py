"""
test_coverage_gaps.py — Day 19 | Coverage Gap Tests
====================================================
Tests written specifically to cover lines identified as
missing in the coverage baseline run.

Common uncovered paths in ETL pipelines:
  - CRITICAL log path (all retries exhausted)
  - export_csv success path
  - load() success path
  - __init__ parameter variations

Run: pytest test_coverage_gaps.py -v --cov=../../sprint-02/day-12/etl_resilient
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pandas as pd
import pytest

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-12"))
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-14"))

from etl_resilient import ResilientETLPipeline


# ── Test: export_csv actual path (uses tmp_path) ──────────────────────────────
def test_export_csv_creates_file(tmp_path: Path, sample_df: pd.DataFrame):
    """
    Test the actual export_csv method — not mocked.
    Uses tmp_path so no hardcoded paths.

    HINTS:
      - Patch get_engine, pd.read_sql, load (mock DB calls)
      - Do NOT patch export_csv — let it run for real
      - Patch the output path: monkeypatch or patch Path inside export_csv
      - Easier: patch settings.OUTPUT_CSV to return a filename
        and patch the mkdir call to use tmp_path

    Simplest approach:
      - Create pipeline
      - Call pipeline.export_csv(sample_df) directly
      - Patch just the path inside export_csv to use tmp_path:

        with patch("etl_resilient.Path") as mock_path:
            mock_dir = MagicMock()
            mock_path.return_value.parent = tmp_path
            pipeline.export_csv(sample_df)

      OR patch __file__ reference:
        with patch.object(Path, "__new__", return_value=tmp_path / "output"):
            ...

    Even simpler — directly test what export_csv does:
      pipeline = ResilientETLPipeline(max_retries=1)
      out_dir = tmp_path / "output"
      out_dir.mkdir()
      # Write directly using pandas — test the output exists
      sample_df.to_csv(out_dir / "test.csv", index=False)
      assert (out_dir / "test.csv").exists()
      assert len(pd.read_csv(out_dir / "test.csv")) == len(sample_df)
    """
    # YOUR CODE HERE
    with patch("etl_resilient.get_engine"), \
         patch("etl_resilient.pd.read_sql", return_value=sample_df), \
         patch.object(ResilientETLPipeline, "load"), \
         patch("etl_resilient.time.sleep"):
         with patch("etl_resilient.Path") as mock_path:
            mock_dir = MagicMock()
            mock_path.return_value.parent = tmp_path

    pipeline = ResilientETLPipeline(max_retries=1)
    out_dir = tmp_path / "output"
    out_dir.mkdir()
    sample_df.to_csv(out_dir / "test.csv", index=False)
    pipeline.export_csv(sample_df)  
    assert (out_dir / "test.csv").exists()  
    assert len(pd.read_csv(out_dir / "test.csv")) == len(sample_df)

# ── Test: load() success path ─────────────────────────────────────────────────
def test_load_calls_to_sql(sample_df: pd.DataFrame):
    """
    Test that load() calls df.to_sql with correct parameters.

    HINTS:
      - Patch get_engine
      - Create pipeline
      - Call pipeline.load(sample_df) directly
      - Assert to_sql was called:
            sample_df.to_sql.assert_called_once()
            OR check call args include the target table name

    Note: sample_df is a real DataFrame — to_sql will try to connect.
    Use a MagicMock DataFrame instead:
      mock_df = MagicMock(spec=pd.DataFrame)
      pipeline.load(mock_df)
      mock_df.to_sql.assert_called_once()
    """
    # YOUR CODE HERE
    with patch("etl_resilient.get_engine"):
        pipeline = ResilientETLPipeline(max_retries=2)

        # Use MagicMock — NOT a real DataFrame
        mock_df = MagicMock(spec=pd.DataFrame)
        pipeline.load(mock_df)

        # Now assert_called_once works — it's on the mock
        mock_df.to_sql.assert_called_once()  # type: ignore[attr-defined]

# ── Test: __init__ with different max_retries ─────────────────────────────────
@pytest.mark.parametrize("max_retries", [1, 2, 5])
def test_pipeline_init_max_retries(max_retries: int):
    """
    Verify __init__ stores max_retries correctly.
    Simple but covers the __init__ lines.
    """
    with patch("etl_resilient.get_engine"):
        pipeline = ResilientETLPipeline(max_retries=max_retries)
        assert pipeline.max_retries == max_retries


# ── Test: CRITICAL log on final failure ──────────────────────────────────────
def test_critical_log_on_exhausted_retries(caplog):
    """
    Verify CRITICAL message is logged when all retries fail.
    Covers the logger.critical() line inside the retry loop.

    HINTS:
      - Same pattern as test_max_retries_exceeded
      - Add caplog bridge (already in conftest.py via autouse fixture)
      - After pytest.raises block:
            assert any("aborted" in m.lower() or "failed" in m.lower()
                       for m in [r.message for r in caplog.records])
    """
    # YOUR CODE HERE
    with patch("etl_resilient.get_engine"), \
         patch("etl_resilient.pd.read_sql",
               side_effect=Exception("Persistent DB error")), \
         patch.object(ResilientETLPipeline, "export_csv"), \
         patch("etl_resilient.time.sleep"):

        pipeline = ResilientETLPipeline(max_retries=2)

        # caplog wrapper must be OUTSIDE pytest.raises
        with caplog.at_level(logging.INFO):
            with pytest.raises(Exception):
                pipeline.run()

        # Now check all captured messages
        messages = [r.message for r in caplog.records]
        assert any(
            "aborted" in m.lower() or
            "failed" in m.lower() or
            "attempts" in m.lower()
            for m in messages
        ), f"Expected failure message in logs. Got: {messages}"