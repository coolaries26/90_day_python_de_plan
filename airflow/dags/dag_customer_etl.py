"""
dag_customer_etl.py — Day 22 | Upgraded with Branch + Sensor
=============================================================
New features:
  - Dynamic Windows IP (no hardcoded 172.x.x.x)
  - BranchPythonOperator: full load vs sample based on row count
  - FileSensor: wait for CSV before downstream processing
  - skip_if_recent: skip run if data is fresh (idempotency pattern)
"""
from __future__ import annotations

import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
# from airflow_callbacks import on_failure, on_retry, on_success
from datetime import timedelta
from airflow_callbacks import on_failure, on_retry


# ── Dynamic Windows IP ────────────────────────────────────────────────────
def _get_windows_ip() -> str:
    try:
        result = subprocess.run(
            ["bash", "-c",
             "ip route | grep default | awk '{print $3}'"],
            capture_output=True, text=True, timeout=5
        )
        ip = result.stdout.strip()
        return ip if ip else "172.18.144.1"
    except Exception:
        return "172.18.144.1"

WINDOWS_IP = _get_windows_ip()
os.environ["DB_HOST"] = WINDOWS_IP

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
OUTPUT_DIR   = PROJECT_ROOT / "airflow" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for p in [
    PROJECT_ROOT / "sprint-01" / "day-02",
    PROJECT_ROOT / "sprint-01" / "day-04",
    PROJECT_ROOT / "sprint-02" / "day-14",
    PROJECT_ROOT / "sprint-03" / "day-16",
]:
    sys.path.insert(0, str(p))

default_args = {
    "owner":              "python-de-journey",
    "retries":            3,
    "retry_delay":        timedelta(minutes=1),
    "retry_exponential_backoff": True,   # ← exponential backoff
    "max_retry_delay":    timedelta(minutes=10),  # ← cap at 10 min
    "on_failure_callback": on_failure,
    "on_retry_callback":   on_retry,
}

# ── Task functions ────────────────────────────────────────────────────────
def run_customer_etl(**context) -> int:
    from etl_protocols import ETLConfig
    from oop_etl import CustomerETLPipeline

    config = ETLConfig(
        source_table="customer",
        target_table = Variable.get(
            "customer_etl_target_table",
             default_var="analytics_customer_airflow"
            ),
        max_retries=2,
        output_dir=OUTPUT_DIR,
    )
    pipeline = CustomerETLPipeline(config=config)
    result   = pipeline.run()

    context["ti"].xcom_push(key="rows_loaded", value=result.rows_loaded)
    context["ti"].xcom_push(key="csv_path",
                            value=str(config.output_csv))
    print(f"ETL complete | rows={result.rows_loaded}")
    return result.rows_loaded


def branch_on_row_count(**context) -> str:
    """
    BranchPythonOperator function.
    Returns task_id to execute next based on row count.
    - Full load (≥500 rows)  → proceed to validate_full
    - Sample load (<500 rows) → proceed to notify_low_count
    """
    rows = context["ti"].xcom_pull(
        task_ids="run_customer_etl", key="rows_loaded"
    )
    print(f"Branching on row count: {rows}")
    if rows >= 500:
        return "validate_full_load"
    return "notify_low_count"


def validate_full_load(**context) -> None:
    rows = context["ti"].xcom_pull(
        task_ids="run_customer_etl", key="rows_loaded"
    )
    assert rows >= 500, f"Expected ≥500 rows, got {rows}"
    print(f"Full load validated: {rows} rows ✅")


def notify_low_count(**context) -> None:
    rows = context["ti"].xcom_pull(
        task_ids="run_customer_etl", key="rows_loaded"
    )
    print(f"WARNING: Low row count detected: {rows}")
    # In production: send Slack/email alert here


def write_audit(**context) -> None:
    """Write audit record after successful validation."""
    rows = context["ti"].xcom_pull(
        task_ids="run_customer_etl", key="rows_loaded"
    )
    print(f"Audit written | rows={rows} | dag=customer_etl_daily")


# ── DAG ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id="customer_etl_daily",
    max_active_runs=1,  # ← prevent overlapping runs
    description="Daily customer ETL — with branching and sensor",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "customer", "sprint-04"],
) as dag:

    # Task 1: Extract + Transform + Load
    task_etl = PythonOperator(
        task_id="run_customer_etl",
        python_callable=run_customer_etl,
        pool="etl_pool",
        priority_weight=10,          # ← highest priority
        weight_rule="absolute",
        sla=timedelta(minutes=5),   # ← must complete in 5 min
    )

    # Task 2: Wait for CSV file to exist before branching
    task_wait_csv = FileSensor(
        task_id="wait_for_csv",
        filepath=str(OUTPUT_DIR / "analytics_customer_airflow.csv"),
        fs_conn_id="fs_default",
        poke_interval=10,    # check every 10 seconds
        timeout=120,         # fail after 2 minutes
        mode="poke",
    )

    # Task 3: Branch based on row count
    task_branch = BranchPythonOperator(
        task_id="branch_on_row_count",
        python_callable=branch_on_row_count,
    )

    # Task 4a: Full load path
    task_validate_full = PythonOperator(
        task_id="validate_full_load",
        python_callable=validate_full_load,
    )

    # Task 4b: Low count path
    task_notify_low = PythonOperator(
        task_id="notify_low_count",
        python_callable=notify_low_count,
    )

    # Task 5: Converge both branches + write audit
    # trigger_rule="none_failed_min_one_success" runs if at least one
    # upstream succeeded (handles branch convergence)
    task_audit = PythonOperator(
        task_id="write_audit",
        python_callable=write_audit,
        trigger_rule="none_failed_min_one_success",
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    task_etl >> task_wait_csv >> task_branch
    task_branch >> task_validate_full >> task_audit
    task_branch >> task_notify_low    >> task_audit