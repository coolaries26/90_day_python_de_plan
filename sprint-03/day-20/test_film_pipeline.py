#!/usr/bin/env python3

"""
TASK T3 — pytest: Write 3 Tests for FilmETLPipeline (20 min)
Brief: Without looking at test_etl_fixed.py, write tests in sprint-03/day-20/test_film_pipeline.py:

test_film_extract_returns_dataframe — extract returns DataFrame with film data
test_film_transform_adds_value_tier — transform adds correct tier for each rate range
test_film_pipeline_success — full run succeeds, load called once
Requirements:

Use fixtures from conftest.py where applicable
Mock all DB calls — no real PostgreSQL in unit tests
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
sys.path.insert(0, str(_here.parent.parent / "sprint-03" / "day-20"))   # For resilient ETL pipeline

from film_etl_pipeline import FilmETLPipeline
from logger import get_pipeline_logger

logger = get_pipeline_logger("test_etl_fixed")

# ── Test 1: extract returns DataFrame ───────────────────────────────
def test_film_extract_returns_dataframe(sample_df: pd.DataFrame, etl_config):
    """
    Test that extract() returns a DataFrame with expected columns.
    HINT: Patch pd.read_sql to return a sample DataFrame, then call extract() and assert the result is a DataFrame with correct columns.
    """
    with patch("film_etl_pipeline.get_engine"), \
            patch("film_etl_pipeline.pd.read_sql", return_value=sample_df), \
         patch.object(FilmETLPipeline, "extract"), \
         patch.object(FilmETLPipeline, "load"), \
         patch.object(FilmETLPipeline, "export_csv"):

        pipeline = FilmETLPipeline(config=etl_config)
        result = pipeline.run()  # Should succeed on first attempt


# ── Test 3: Successful ETL run ─────────────────────────────────────────────────
def test_successful_etl(sample_df: pd.DataFrame, etl_config):
    """
    test_film_pipeline_success — full run succeeds, load called once

    HINTS:
      - Patch: "etl_resilient.get_engine" (no return value needed)
    """  
    with patch("film_etl_pipeline.get_engine"), \
         patch("film_etl_pipeline.pd.read_sql", return_value=sample_df), \
         patch.object(FilmETLPipeline, "load"), \
         patch.object(FilmETLPipeline, "transform"), \
         patch.object(FilmETLPipeline, "export_csv"):

        pipeline = FilmETLPipeline(config=etl_config)
        result = pipeline.run()  # Should succeed on first attempt



# ── Test 2: test transform adds value tier ─────────────────────────────────────────────────
def test_film_transform_adds_value_tier(sample_df: pd.DataFrame, etl_config):
    """
    test_film_transform_adds_value_tier — transform adds correct tier for each rate range
    """
    with patch("film_etl_pipeline.get_engine"), \
             patch("film_etl_pipeline.pd.read_sql", return_value=sample_df), \
             patch.object(FilmETLPipeline, "load"), \
             patch.object(FilmETLPipeline, "export_csv"), \
             patch("film_etl_pipeline.time.sleep"):

        pipeline = FilmETLPipeline(config=etl_config)
        result = pipeline.run()  # Should succeed on 2nd attempt
  

## ── caplog Test  ───────────────────────────────
#def test_pipeline_logs_success(sample_df: pd.DataFrame, caplog, etl_config):
#    with patch("film_etl_pipeline.get_engine"), \
#         patch("film_etl_pipeline.pd.read_sql", return_value=sample_df), \
#         patch.object(FilmETLPipeline, "load"), \
#         patch.object(FilmETLPipeline, "export_csv"), \
#         patch("film_etl_pipeline.time.sleep"):
#
#        pipeline = FilmETLPipeline(config=etl_config)
#
#        with caplog.at_level(logging.INFO):
#            pipeline.run()
#        # Assert success message appears in logs
#        messages = [r.message for r in caplog.records]
#        assert any(
#            "success" in record.message.lower() or
#            "SUCCESS" in record.message
#            for record in caplog.records
#        ), f"Expected SUCCESS in logs. Got: {messages}"