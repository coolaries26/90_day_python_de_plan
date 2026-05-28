"""
dag_ecommerce_etl.py — Day 46 | Capstone Pipeline DAG
=======================================================
Orchestrates the full e-commerce analytics pipeline:
  1. Load raw data (if new files detected)
  2. Run analytics ETL (raw → 5 analytics tables)
  3. Train churn model → write predictions
  4. Train delay model → write predictions

Schedule: @weekly (data is static — weekly refresh is appropriate)
Manual trigger: airflow dags trigger dag_ecommerce_etl
"""
from __future__ import annotations
import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

_ip = subprocess.run(
    ["bash", "-c", "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True,
).stdout.strip()
os.environ["DB_HOST"] = _ip or "172.18.144.1"
# Ecommerce DB uses same host, different DB name
os.environ["ECOMMERCE_DB_HOST"]     = _ip or "172.18.144.1"
os.environ["ECOMMERCE_DB_PORT"]     = "5432"
os.environ["ECOMMERCE_DB_NAME"]     = "ecommerce_db"
os.environ["ECOMMERCE_DB_USER"]     = "appuser"
os.environ["ECOMMERCE_DB_PASSWORD"] = "AppUser@2024!"

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
for p in [
    PROJECT_ROOT / "sprint-01" / "day-04",  # db_utils
    PROJECT_ROOT / "sprint-01" / "day-05",  # models
    PROJECT_ROOT / "capstone",  # shared utils
    PROJECT_ROOT / "capstone" / "etl", # raw → staging → analytics
    PROJECT_ROOT / "capstone" / "ml", # churn + delay models
    PROJECT_ROOT / "sprint-03" / "day-16", # airflow callbacks
]:
    sys.path.insert(0, str(p))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_callbacks import on_failure

default_args = {
    "owner":               "python-de-journey",
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": on_failure,
}


# ── Task 1: Run analytics ETL — provided ──────────────────────────────────
def run_analytics_etl(**context) -> dict:
    """
    Run all 5 analytics transforms: raw → analytics schema.
    """
    from analytics_etl import main as etl_main, run_etl, get_ecommerce_engine, dispose_ecommerce_engine, \
        CUSTOMER_LTV_SQL, ORDER_METRICS_SQL, SELLER_PERFORMANCE_SQL, \
        PRODUCT_ANALYTICS_SQL, MONTHLY_REVENUE_SQL

    engine = get_ecommerce_engine()
    results = {}
    tasks = [
        ("customer_ltv",        CUSTOMER_LTV_SQL),
        ("order_metrics",       ORDER_METRICS_SQL),
        ("seller_performance",  SELLER_PERFORMANCE_SQL),
        ("product_analytics",   PRODUCT_ANALYTICS_SQL),
        ("monthly_revenue",     MONTHLY_REVENUE_SQL),
    ]
    for table_name, sql in tasks:
        try:
            count = run_etl(sql, table_name, engine)
            results[table_name] = count
        except Exception as exc:
            print(f"❌ {table_name}: {exc}")
            results[table_name] = 0

    dispose_ecommerce_engine()
    context["ti"].xcom_push(key="etl_results", value=results)
    print(f"ETL complete: {sum(results.values()):,} total rows across {len(results)} tables")
    return results


# ── Task 2: Train churn model — provided ──────────────────────────────────
def run_churn_model(**context) -> dict:
    """Train churn pipeline and write predictions to ml.churn_predictions."""
    from churn_model import load_features, train_churn_pipeline, save_pipeline, write_churn_predictions

    X, y = load_features()
    pipeline = train_churn_pipeline(X, y)
    save_pipeline(pipeline, X, "churn_pipeline")
    write_churn_predictions(pipeline, X)

    result = {
        "n_samples":   int(len(X)),
        "churn_rate":  float(y.mean()),
        "status":      "success",
    }
    context["ti"].xcom_push(key="churn_result", value=result)
    return result


# ── Task 3: Train delay model — WRITE THIS YOURSELF ───────────────────────
def run_delay_model(**context) -> dict:
    """
    YOUR TASK: Train delay pipeline and write predictions.

    HINTS:
    from delay_model import load_features, train_delay_pipeline, save_pipeline, write_delay_predictions

    X, y = load_features()
    pipeline = train_delay_pipeline(X, y)
    save_pipeline(pipeline, X, "delay_pipeline")
    write_delay_predictions(pipeline, X)

    result = {
        "n_samples": int(len(X)),
        "late_rate": float(y.mean()),
        "status":    "success",
    }
    context["ti"].xcom_push(key="delay_result", value=result)
    return result
    """
    # YOUR CODE HERE
    from delay_model import load_features, train_model, save_pipeline, write_delay_predictions
    x, y = load_features()
    pipeline = train_model(x, y)
    save_pipeline(pipeline, x, "delay_pipeline")
    write_delay_predictions(pipeline, x)
    result = {
        "n_samples": int(len(x)),
        "late_rate": float(y.mean()),
        "status":    "success",
    }
    context["ti"].xcom_push(key="delay_result", value=result)
    return result


# ── Task 4: Log pipeline run — WRITE THIS YOURSELF ────────────────────────
def log_pipeline_run(**context) -> None:
    """
    YOUR TASK: Pull XCom results and log a summary audit entry.

    HINTS:
    etl     = context["ti"].xcom_pull(task_ids="run_analytics_etl", key="etl_results") or {}
    churn   = context["ti"].xcom_pull(task_ids="run_churn_model",   key="churn_result") or {}
    delay   = context["ti"].xcom_pull(task_ids="run_delay_model",   key="delay_result") or {}

    total_rows = sum(etl.values())

    from sqlalchemy.orm import Session
    from db_utils import get_engine, dispose_engine
    from models_compat import AuditLog

    engine = get_engine()
    audit = AuditLog(
        pipeline_name="dag_ecommerce_etl",
        source_table="ecommerce_db.raw",
        target_table="ecommerce_db.analytics + ecommerce_db.ml",
        rows_loaded=total_rows,
        status="success",
    )
    with Session(engine) as session:
        session.add(audit)
        session.commit()
    dispose_engine()

    print(f"Pipeline summary:")
    print(f"  ETL rows:    {total_rows:,}")
    print(f"  Churn rate:  {churn.get('churn_rate', 0):.1%}")
    print(f"  Late rate:   {delay.get('late_rate', 0):.1%}")
    """
    # YOUR CODE HERE
    etl     = context["ti"].xcom_pull(task_ids="run_analytics_etl", key="etl_results") or {}
    churn   = context["ti"].xcom_pull(task_ids="run_churn_model",   key="churn_result") or {}
    delay   = context["ti"].xcom_pull(task_ids="run_delay_model",   key="delay_result") or {}

    total_rows = sum(etl.values())

    from sqlalchemy.orm import Session
    from db_utils import get_engine, dispose_engine
    from models_compat import AuditLog

    engine = get_engine()
    audit = AuditLog(
        pipeline_name="dag_ecommerce_etl",
        source_table="ecommerce_db.raw",
        target_table="ecommerce_db.analytics + ecommerce_db.ml",
        rows_loaded=total_rows,
        status="success",
    )
    with Session(engine) as session:
        session.add(audit)
        session.commit()
    dispose_engine()

    print(f"Pipeline summary:")
    print(f"  ETL rows:    {total_rows:,}")
    print(f"  Churn rate:  {churn.get('churn_rate', 0):.1%}")
    print(f"  Late rate:   {delay.get('late_rate', 0):.1%}")

# Task 5 function — WRITE THIS YOURSELF:



with DAG(
    dag_id="dag_ecommerce_etl",
    description="Capstone: full e-commerce analytics pipeline",
    default_args=default_args,
#    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["capstone", "ecommerce", "etl", "ml", "sprint-07"],
) as dag:

    task_etl = PythonOperator(
        task_id="run_analytics_etl",
        python_callable=run_analytics_etl,
        pool="db_connection_pool",
    )

    task_churn = PythonOperator(
        task_id="run_churn_model",
        python_callable=run_churn_model,
        pool="db_connection_pool",
    )

    task_delay = PythonOperator(
        task_id="run_delay_model",
        python_callable=run_delay_model,
        pool="db_connection_pool",
    )

    task_log = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=log_pipeline_run,
        trigger_rule="all_done",
    )

    # ETL first, then churn + delay in parallel, then log
    task_etl >> [task_churn, task_delay] >> task_log