#!/usr/bin/env python3
"""
pipeline_log_demo.py — Day 04 | Logging Demonstration
======================================================
Runs a short pipeline demonstrating:
  - All 5 log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  - Exception capture with full traceback
  - Structured JSON log output
  - Pipeline start/end markers
  - Per-table row count logging

Run: python pipeline_log_demo.py
Then open logs/demo_pipeline_YYYYMMDD.log to see JSON output.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "day-02"))

from logger import get_pipeline_logger, log_pipeline_start, log_pipeline_end
from db_utils import get_engine, execute_query, execute_scalar, close_pool, dispose_engine

logger = get_pipeline_logger("demo_pipeline")

TABLES = ["film", "customer", "rental", "payment", "inventory"]


def step_1_connect_and_count():
    """Step 1: Connect and log row counts for all tables."""
    logger.info("Step 1 | Connecting to database and counting rows")

    for table in TABLES:
        count = execute_scalar(f"SELECT COUNT(*) FROM {table}")
        # INFO: normal operational message
        logger.info("Table inventory | table={} rows={:,}", table, count)

        # DEBUG: detailed diagnostic — only visible when LOG_LEVEL=DEBUG
        logger.debug("Table details | table={} dtype_check=pending", table)


def step_2_detect_warning():
    """Step 2: Demonstrate WARNING level — data quality threshold breach."""
    logger.info("Step 2 | Checking data quality thresholds")

    null_returns = execute_scalar(
        "SELECT COUNT(*) FROM rental WHERE return_date IS NULL"
    )
    total_rentals = execute_scalar("SELECT COUNT(*) FROM rental")
    null_pct = null_returns / total_rentals * 100

    if null_pct > 5:
        # WARNING: something is unusual but pipeline can continue
        logger.warning(
            "Data quality | null return_date exceeds threshold | "
            "null_count={} total={} pct={:.1f}% threshold=5%",
            null_returns, total_rentals, null_pct
        )
    else:
        logger.info("Data quality | null return_date within threshold | pct={:.1f}%", null_pct)


def step_3_simulate_error():
    """
    Step 3: Demonstrate ERROR level — recoverable failure.

    YOUR TASK: Write code that:
      1. Attempts to query a table that does NOT exist: 'film_archive'
      2. Catches the exception
      3. Logs it at ERROR level with the exception details
      4. Logs a recovery message at WARNING level
      5. Continues (does not crash the pipeline)

    HINTS:
      - Use try/except around execute_query("SELECT * FROM film_archive")
      - logger.error("message | error={}", str(exc))  ← loguru syntax
      - logger.exception("message")  ← auto-captures current exception + traceback
      - After catch: logger.warning("Skipping film_archive — table not found, continuing")
    """
    logger.info("Step 3 | Testing error recovery")

    # YOUR CODE HERE
    raise NotImplementedError("Implement step_3 error handling — see hints above")


def step_4_log_summary():
    """Step 4: Write a structured summary row to the log."""
    logger.info("Step 4 | Writing pipeline summary")

    summary = {
        "pipeline":     "demo_pipeline",
        "tables_checked": len(TABLES),
        "status":       "SUCCESS",
    }
    # loguru accepts **kwargs for structured context
    logger.info("Pipeline summary | {}", summary)


def main():
    start_time = time.perf_counter()

    log_pipeline_start(logger, "demo_pipeline",
                       env="development", db="dvdrental")
    rows_processed = 0

    try:
        step_1_connect_and_count()
        rows_processed += sum(
            execute_scalar(f"SELECT COUNT(*) FROM {t}") for t in TABLES
        )

        step_2_detect_warning()

        try:
            step_3_simulate_error()
        except NotImplementedError as e:
            logger.warning("step_3 not implemented yet: {}", e)

        step_4_log_summary()

    except Exception as exc:
        # CRITICAL: unrecoverable failure — pipeline aborted
        logger.critical("Pipeline aborted | error={}", exc)
        raise
    finally:
        elapsed = time.perf_counter() - start_time
        log_pipeline_end(logger, "demo_pipeline",
                         rows_processed=rows_processed,
                         elapsed_sec=elapsed)
        close_pool()
        dispose_engine()


if __name__ == "__main__":
    main()

