"""
dag_dbt_pipeline.py — Day 55 | dbt Orchestration DAG
======================================================
Runs dbt models and tests after ecommerce ETL completes.
Triggered by dataset event from dag_ecommerce_etl.
"""
from __future__ import annotations
import os
GX_DIR = "/mnt/d/alsgit/90_day_python_de_plan/sprint-09/day-57/gx_project/great_expectations"

import subprocess
from datetime import datetime, timedelta
from pathlib import Path

_ip = subprocess.run(
    ["bash", "-c", "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True,
).stdout.strip()

DBT_PROJECT_DIR = "/mnt/d/alsgit/90_day_python_de_plan/sprint-08/day-51/ecommerce_dbt"
DBT_PROFILES_DIR = "/mnt/d/alsgit/90_day_python_de_plan/.dbt"

import sys
sys.path.insert(0, "/mnt/d/alsgit/90_day_python_de_plan/airflow/dags")
sys.path.insert(0, "/mnt/d/alsgit/90_day_python_de_plan/sprint-01/day-04")
sys.path.insert(0, "/mnt/d/alsgit/90_day_python_de_plan/sprint-01/day-02")
sys.path.insert(0, "/mnt/d/alsgit/90_day_python_de_plan/sprint-03/day-16")

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator #type: ignore
from airflow.operators.python import PythonOperator #type: ignore
from airflow_callbacks import on_failure

ECOMMERCE_DATASET = Dataset("postgresql://ecommerce_db/raw")
DBT_MART_DATASET  = Dataset("postgresql://ecommerce_db/dbt_dev_marts")

default_args = {
    "owner":               "python-de-journey",
    "retries":             1,
    "retry_delay":         timedelta(minutes=2),
    "on_failure_callback": on_failure,
}

def run_gx_checkpoint(**context) -> None:
    """
    Run GX checkpoint — validates all 3 expectation suites.
    Raises ValueError if any expectation fails.
    Airflow catches the exception → task marked FAILED
    → on_failure_callback fires → audit log entry written.
    """
    import great_expectations as gx

    # Set Windows IP for WSL2 → PostgreSQL connection
    import subprocess
    ip = subprocess.run(
        ["bash", "-c", "ip route | grep default | awk '{print $3}'"],
        capture_output=True, text=True,
    ).stdout.strip()

    # Patch the datasource connection string if needed
    # (GX reads from great_expectations.yml — update host if 127.0.0.1)
    gx_context = gx.data_context.DataContext(context_root_dir=GX_DIR)

    print("Running GX checkpoint: ecommerce_data_quality")
    result = gx_context.run_checkpoint(
        checkpoint_name="ecommerce_data_quality"
    )

    # Log per-suite results to task logs
    for key, val in result.run_results.items():
        vr    = val["validation_result"]
        suite = vr.meta["expectation_suite_name"]
        stats = vr.statistics
        status = "✅" if vr.success else "❌"
        print(
            f"  {status} {suite}: "
            f"{stats['successful_expectations']}/{stats['evaluated_expectations']} "
            f"({stats['success_percent']:.0f}%)"
        )

    # Push summary to XCom
    context["ti"].xcom_push(key="gx_success", value=result.success)
    context["ti"].xcom_push(key="gx_suites_run", value=3)

    if not result.success:
        raise ValueError(
            "GX checkpoint FAILED — data quality issues detected. "
            "Check Data Docs for details."
        )

    print(f"GX checkpoint passed: 29/29 expectations")

def log_dbt_run(**context) -> None:
    """Log the dbt run results to etl_audit_log."""
    try:
        from sqlalchemy.orm import Session
        from db_utils import get_engine, dispose_engine #type: ignore
        from models_compat import AuditLog  #type: ignore

        engine = get_engine()
        audit = AuditLog(
            pipeline_name="dag_dbt_pipeline",
            source_table="ecommerce_db.raw",
            target_table="ecommerce_db.dbt_dev_marts",
            rows_loaded=0,
            status="success",
        )
        with Session(engine) as session:
            session.add(audit)
            session.commit()
        dispose_engine()
        print("dbt run logged to etl_audit_log")
    except Exception as exc:
        print(f"Audit log failed (non-fatal): {exc}")


with DAG(
    dag_id="dag_dbt_pipeline",
    description="Run dbt models + tests after ecommerce ETL",
    default_args=default_args,
    schedule=[ECOMMERCE_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "capstone", "sprint-08"],
) as dag:

    # ── Task 1: dbt run — provided ────────────────────────────────────────
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --target wsl"
        ),
    )

    # ── Task 2: dbt test — provided ───────────────────────────────────────
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --target wsl"
        ),
    )

    # ── Task 3: dbt snapshot — WRITE THIS YOURSELF ────────────────────────
    # HINTS:
    # task_dbt_snapshot = BashOperator(
    #     task_id="dbt_snapshot",
    #     bash_command=(
    #         f"cd {DBT_PROJECT_DIR} && "
    #         f"dbt snapshot --profiles-dir {DBT_PROFILES_DIR} --target wsl"
    #     ),
    # )
    # YOUR CODE HERE
    task_dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt snapshot --target wsl"
        ),
    )
    # ── Task 4: log run — WRITE THIS YOURSELF ─────────────────────────────
    # HINTS:
    # task_log = PythonOperator(
    #     task_id="log_dbt_run",
    #     python_callable=log_dbt_run,
    #     outlets=[DBT_MART_DATASET],   # signals marts are fresh
    #     trigger_rule="all_done",
    # )
    # YOUR CODE HERE
    task_log = PythonOperator(
        task_id="log_dbt_run",
        python_callable=log_dbt_run,
        outlets=[DBT_MART_DATASET],   # signals marts are fresh
        trigger_rule="all_done",
    )
    # ── Task 5: run GX checkpoint — WRITE THIS YOURSELF ──────────────────
    # task_gx = PythonOperator(
    #     task_id="run_gx_checkpoint",
    #     python_callable=run_gx_checkpoint,
    #     trigger_rule="all_success",   # only run if dbt_test passed
    # )

    # Update dependency chain:
    # task_dbt_run >> task_dbt_test >> task_dbt_snapshot >> task_gx >> task_log
    task_gx = PythonOperator(
        task_id="run_gx_checkpoint",
        python_callable=run_gx_checkpoint,
        trigger_rule="all_success",   # only run if dbt_test passed
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    # YOUR TASK: chain all 4 tasks
    # task_dbt_run >> task_dbt_test >> task_dbt_snapshot >> task_log
    task_dbt_run >> task_dbt_test >> task_dbt_snapshot >> task_gx >> task_log