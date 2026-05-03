"""
dag_customer_etl.py — Day 21 | First Airflow DAG
=================================================
Orchestrates the CustomerETLPipeline on a daily schedule.
Demonstrates:
  - DAG definition with schedule
  - PythonOperator wrapping existing ETL code
  - Task dependencies (extract → transform → load)
  - XCom for passing data between tasks
  - PostgresOperator for SQL verification

DAG ID: customer_etl_daily
Schedule: Daily at midnight
"""

from __future__ import annotations

import os 
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

# ── Override DB_HOST before any project imports ───────────────────────────
# 127.0.0.1 = Windows localhost, not reachable from WSL2
# 172.18.144.1 = Windows host IP, reachable from WSL2
os.environ["DB_HOST"] = "172.18.144.1"

# ── Add project to Python path ─────────────────────────────────────────────
# Airflow runs DAGs in its own process — must add project paths explicitly
PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
#PROJECT_ROOT = Path("C:\90_day_python_de_plan")

for path in [
    PROJECT_ROOT / "sprint-01" / "day-02",
    PROJECT_ROOT / "sprint-01" / "day-04",
    PROJECT_ROOT / "sprint-02" / "day-14",
    PROJECT_ROOT / "sprint-03" / "day-16",
]:
    sys.path.insert(0, str(path))

# ── Default arguments — apply to all tasks unless overridden ───────────────
default_args = {
    "owner":            "python-de-journey",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
}

# ── Task functions ─────────────────────────────────────────────────────────
def run_customer_etl(**context) -> dict:
    """
    PythonOperator task: run CustomerETLPipeline.
    Returns result dict pushed to XCom automatically.
    context['ti'] = TaskInstance — use for XCom push/pull
    """
    from etl_protocols import ETLConfig
    from oop_etl import CustomerETLPipeline

    config = ETLConfig(
        source_table="customer",
        target_table="analytics_customer_airflow",
        max_retries=2,
        output_dir=PROJECT_ROOT / "airflow" / "output",
    )
    pipeline = CustomerETLPipeline(config=config)
    result = pipeline.run()

    # Push result summary to XCom — accessible by downstream tasks
    context["ti"].xcom_push(
        key="etl_result",
        value={
            "rows_loaded":  result.rows_loaded,
            "status":       result.status,
            "elapsed_s":    result.elapsed_seconds,
            "pipeline":     result.pipeline_name,
        }
    )
    return {"rows_loaded": result.rows_loaded, "status": result.status}


def log_run_summary(**context) -> None:
    """
    PythonOperator task: pull XCom result and log summary.
    Demonstrates XCom pull from upstream task.
    """
    import logging
    log = logging.getLogger(__name__)

    # Pull result from upstream task via XCom
    result = context["ti"].xcom_pull(
        task_ids="run_customer_etl",
        key="etl_result"
    )
    log.info(f"ETL Summary | pipeline={result['pipeline']} "
             f"rows={result['rows_loaded']} "
             f"status={result['status']} "
             f"elapsed={result['elapsed_s']:.2f}s")


def validate_row_count(**context) -> None:
    """
    PythonOperator task: verify row count matches expectation.
    Raises ValueError if count is wrong — fails the DAG run.
    """
    import sys
    sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-02"))
    from db_utils import execute_scalar, close_pool

    count = execute_scalar(
        "SELECT COUNT(*) FROM analytics_customer_airflow"
    )
    close_pool()

    if count < 500:
        raise ValueError(
            f"Row count validation FAILED: expected ≥500, got {count}"
        )
    print(f"Row count validation PASSED: {count} rows")


# ── DAG Definition ─────────────────────────────────────────────────────────
with DAG(
    dag_id="customer_etl_daily",
    description="Daily customer ETL pipeline — Python DE Journey",
    default_args=default_args,
    schedule="@daily",              # run once per day
    start_date=datetime(2026, 1, 1),
    catchup=False,                  # don't backfill historical runs
    tags=["etl", "customer", "sprint-04"],
    doc_md="""
    ## Customer ETL Daily DAG

    Runs the CustomerETLPipeline daily:
    1. Extracts customer + rental + payment data from dvdrental
    2. Transforms: adds value_segment, days_since_last_payment
    3. Loads to analytics_customer_airflow table
    4. Validates row count ≥ 500
    5. Logs summary via XCom
    """,
) as dag:

    # Task 1: Run the ETL pipeline
    task_run_etl = PythonOperator(
        task_id="run_customer_etl",
        python_callable=run_customer_etl,
    )

    # Task 2: Validate row count in DB
    task_validate = PythonOperator(
        task_id="validate_row_count",
        python_callable=validate_row_count,
    )

    # Task 3: Log summary
    task_log_summary = PythonOperator(
        task_id="log_run_summary",
        python_callable=log_run_summary,
    )

    # Task 4: Write audit record via SQL
    task_audit_sql = PostgresOperator(
        task_id="write_audit_record",
        postgres_conn_id="dvdrental_appuser",   # configure in Airflow UI
        sql="""
            INSERT INTO etl_audit_log
                (pipeline_name, source_table, target_table,
                 rows_loaded, status, elapsed_s)
            VALUES
                ('customer_etl_daily', 'customer',
                 'analytics_customer_airflow',
                 (SELECT COUNT(*) FROM analytics_customer_airflow),
                 'success', 0)
            ON CONFLICT DO NOTHING;
        """,
    )

    # ── Task Dependencies ──────────────────────────────────────────────────
    # run_etl → validate → log_summary → audit
    task_run_etl >> task_validate >> task_log_summary >> task_audit_sql

## ── Add project to Python path ─────────────────────────────────────────────
## Airflow runs DAGs in its own process — must add project paths explicitly
#PROJECT_ROOT = Path("C:/Users/Lenovo/python-de-journey")
#for path in [
#    PROJECT_ROOT / "sprint-01" / "day-02",
#    PROJECT_ROOT / "sprint-01" / "day-04",
#    PROJECT_ROOT / "sprint-02" / "day-14",
#    PROJECT_ROOT / "sprint-03" / "day-16",
#]:
#    sys.path.insert(0, str(path))
#
## ── Default arguments — apply to all tasks unless overridden ───────────────
#default_args = {
#    "owner":            "python-de-journey",
#    "depends_on_past":  False,
#    "email_on_failure": False,
#    "email_on_retry":   False,
#    "retries":          2,
#    "retry_delay":      timedelta(minutes=1),
#}
#
## ── Task functions ─────────────────────────────────────────────────────────
#def run_customer_etl(**context) -> dict:
#    """
#    PythonOperator task: run CustomerETLPipeline.
#    Returns result dict pushed to XCom automatically.
#    context['ti'] = TaskInstance — use for XCom push/pull
#    """
#    from etl_protocols import ETLConfig
#    from oop_etl import CustomerETLPipeline
#
#    config = ETLConfig(
#        source_table="customer",
#        target_table="analytics_customer_airflow",
#        max_retries=2,
#        output_dir=PROJECT_ROOT / "airflow" / "output",
#    )
#    pipeline = CustomerETLPipeline(config=config)
#    result = pipeline.run()
#
#    # Push result summary to XCom — accessible by downstream tasks
#    context["ti"].xcom_push(
#        key="etl_result",
#        value={
#            "rows_loaded":  result.rows_loaded,
#            "status":       result.status,
#            "elapsed_s":    result.elapsed_seconds,
#            "pipeline":     result.pipeline_name,
#        }
#    )
#    return {"rows_loaded": result.rows_loaded, "status": result.status}
#
#
#def log_run_summary(**context) -> None:
#    """
#    PythonOperator task: pull XCom result and log summary.
#    Demonstrates XCom pull from upstream task.
#    """
#    import logging
#    log = logging.getLogger(__name__)
#
#    # Pull result from upstream task via XCom
#    result = context["ti"].xcom_pull(
#        task_ids="run_customer_etl",
#        key="etl_result"
#    )
#    log.info(f"ETL Summary | pipeline={result['pipeline']} "
#             f"rows={result['rows_loaded']} "
#             f"status={result['status']} "
#             f"elapsed={result['elapsed_s']:.2f}s")
#
#
#def validate_row_count(**context) -> None:
#    """
#    PythonOperator task: verify row count matches expectation.
#    Raises ValueError if count is wrong — fails the DAG run.
#    """
#    import sys
#    sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-02"))
#    from db_utils import execute_scalar, close_pool
#
#    count = execute_scalar(
#        "SELECT COUNT(*) FROM analytics_customer_airflow"
#    )
#    close_pool()
#
#    if count < 500:
#        raise ValueError(
#            f"Row count validation FAILED: expected ≥500, got {count}"
#        )
#    print(f"Row count validation PASSED: {count} rows")
#
#
## ── DAG Definition ─────────────────────────────────────────────────────────
#with DAG(
#    dag_id="customer_etl_daily",
#    description="Daily customer ETL pipeline — Python DE Journey",
#    default_args=default_args,
#    schedule="@daily",              # run once per day
#    start_date=datetime(2026, 1, 1),
#    catchup=False,                  # don't backfill historical runs
#    tags=["etl", "customer", "sprint-04"],
#    doc_md="""
#    ## Customer ETL Daily DAG
#
#    Runs the CustomerETLPipeline daily:
#    1. Extracts customer + rental + payment data from dvdrental
#    2. Transforms: adds value_segment, days_since_last_payment
#    3. Loads to analytics_customer_airflow table
#    4. Validates row count ≥ 500
#    5. Logs summary via XCom
#    """,
#) as dag:
#
#    # Task 1: Run the ETL pipeline
#    task_run_etl = PythonOperator(
#        task_id="run_customer_etl",
#        python_callable=run_customer_etl,
#    )
#
#    # Task 2: Validate row count in DB
#    task_validate = PythonOperator(
#        task_id="validate_row_count",
#        python_callable=validate_row_count,
#    )
#
#    # Task 3: Log summary
#    task_log_summary = PythonOperator(
#        task_id="log_run_summary",
#        python_callable=log_run_summary,
#    )
#
#    # Task 4: Write audit record via SQL
#    task_audit_sql = PostgresOperator(
#        task_id="write_audit_record",
#        postgres_conn_id="dvdrental_appuser",   # configure in Airflow UI
#        sql="""
#            INSERT INTO etl_audit_log
#                (pipeline_name, source_table, target_table,
#                 rows_loaded, status, elapsed_s)
#            VALUES
#                ('customer_etl_daily', 'customer',
#                 'analytics_customer_airflow',
#                 (SELECT COUNT(*) FROM analytics_customer_airflow),
#                 'success', 0)
#            ON CONFLICT DO NOTHING;
#        """,
#    )
#
#    # ── Task Dependencies ──────────────────────────────────────────────────
#    # run_etl → validate → log_summary → audit
#    task_run_etl >> task_validate >> task_log_summary >> task_audit_sql