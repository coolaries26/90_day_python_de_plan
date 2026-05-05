"""
dag_failure_test.py — Day 24 | Callback Testing DAG
====================================================
Deliberately fails tasks to verify:
  1. on_failure_callback fires and writes to etl_audit_log
  2. on_retry_callback fires on each retry
  3. Exponential backoff delays are applied

Run once, then check etl_audit_log for failure records.
DO NOT schedule this DAG — trigger manually only.
"""
from __future__ import annotations
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import subprocess
_ip = subprocess.run(
    ["bash", "-c",
     "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True,
).stdout.strip()
os.environ["DB_HOST"] = _ip or "172.18.144.1"

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-02"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-04"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint-03" / "day-16"))
sys.path.insert(0, str(Path(__file__).parent))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_callbacks import on_failure, on_retry
from db_utils import execute_query, close_pool

# ── Q1: Task that always fails — provided ────────────────────────────────
def always_fails(**context) -> None:
    """
    This task always raises an exception.
    Used to test that on_failure_callback fires correctly.
    """
    attempt = context["task_instance"].try_number
    print(f"always_fails: attempt {attempt} — about to raise")
    raise RuntimeError(
        f"Deliberate failure on attempt {attempt} "
        f"— testing retry and callback logic"
    )


# ── Q1: Task that fails twice then succeeds — provided ───────────────────
def fails_twice_then_succeeds(**context) -> None:
    """
    Fails on attempts 1 and 2, succeeds on attempt 3.
    Tests that retry logic and exponential backoff work correctly.
    """
    attempt = context["task_instance"].try_number
    print(f"fails_twice_then_succeeds: attempt {attempt}")

    if attempt < 3:
        raise ValueError(
            f"Simulated transient failure on attempt {attempt}"
        )
    print(f"Success on attempt {attempt} ✅")


# ── Q2: Task that checks failure was logged — WRITE THIS YOURSELF ─────────
def verify_failure_logged(**context) -> None:
    """
    Q2 — YOUR TASK:
    Verify that the on_failure_callback wrote a record to etl_audit_log.

    HINTS:
      - from db_utils import execute_query, close_pool
      - Query etl_audit_log WHERE status = 'failed'
        AND pipeline_name LIKE '%always_fails%'
        ORDER BY run_at DESC LIMIT 1
      - Assert len(rows) > 0
      - Print the failure record
      - close_pool()

    Expected: at least one row with status='failed' for always_fails task
    Note: this task only runs if always_fails has exhausted all retries
          so trigger_rule="all_done" is needed (see DAG definition below)
    """
    # YOUR CODE HERE
    from db_utils import execute_query, close_pool
    try:
        from models_compat import AuditLog   # SQLAlchemy 1.4 (airflow-venv)
    except ImportError:
        from models import AuditLog          # SQLAlchemy 2.0 (windows venv)
    query = """
        SELECT *
        FROM etl_audit_log
        WHERE status = 'failed'
          AND pipeline_name LIKE '%always_fails%'
        ORDER BY run_at DESC
        LIMIT 1
    """
    rows = execute_query(query)
    assert len(rows) > 0, "No failure record found for always_fails"
    print(f"Failure record found: {rows[0]}")
    close_pool()

with DAG(
    dag_id="failure_test",
    description="Tests retry callbacks and failure logging",
    default_args={
        "owner":                     "python-de-journey",
        "retries":                   2,
        "retry_delay":               timedelta(seconds=10),  # short for testing
        "retry_exponential_backoff": True,
        "on_failure_callback":       on_failure,
        "on_retry_callback":         on_retry,
    },
    schedule=None,          # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "callbacks", "sprint-04"],
) as dag:

    task_always_fails = PythonOperator(
        task_id="always_fails",
        python_callable=always_fails,
    )

    task_recovers = PythonOperator(
        task_id="fails_twice_then_succeeds",
        python_callable=fails_twice_then_succeeds,
    )

    task_verify = PythonOperator(
        task_id="verify_failure_logged",
        python_callable=verify_failure_logged,
        trigger_rule="all_done",   # runs even if upstream failed
    )

    # always_fails and fails_twice run in parallel
    # verify runs after both complete (regardless of outcome)
    [task_always_fails, task_recovers] >> task_verify