"""
dag_audit_taskflow.py — Day 25 | TaskFlow API Version
======================================================
Same logic as dag_audit_report.py but using @task decorator.
Demonstrates how TaskFlow simplifies XCom handling.
"""
from __future__ import annotations
import os
import sys
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import Dataset

# Define the datasets this DAG consumes:
CUSTOMER_DATASET = Dataset(
    "postgresql://dvdrental/analytics_customer_airflow"
)
FILM_DATASET = Dataset(
    "postgresql://dvdrental/analytics_film_airflow"
)
RENTAL_DATASET = Dataset(
    "postgresql://dvdrental/analytics_rental_airflow"
)


_ip = subprocess.run(
    ["bash", "-c", "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True,
).stdout.strip()
os.environ["DB_HOST"] = _ip or "172.18.144.1"

PROJECT_ROOT = Path("/mnt/d/alsgit/90_day_python_de_plan")
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-02"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-04"))

from airflow.decorators import dag, task
from airflow.models import Variable


@task
def read_audit_log() -> dict:
    """
    TaskFlow version — return value automatically becomes XCom.
    No context["ti"].xcom_push() needed.
    """
    import pandas as pd
    from db_utils import get_engine, dispose_engine

    engine = get_engine()
    df = pd.read_sql("""
        SELECT pipeline_name, status, rows_loaded, run_at
        FROM etl_audit_log
        ORDER BY run_at DESC LIMIT 20
    """, engine)
    dispose_engine()

    return {
        "total_runs":  int(len(df)),
        "successful":  int((df["status"] == "success").sum()),
        "failed":      int((df["status"] == "failed").sum()),
        "total_rows":  int(df["rows_loaded"].sum()),
        "latest_run":  str(df["run_at"].max()) if len(df) > 0 else None,
    }


@task
def check_counts() -> dict:
    """Check pipeline row counts from Variable config."""
    from db_utils import execute_scalar, close_pool

    config = json.loads(Variable.get("etl_pipeline_config", default_var="{}"))
    results = {}
    for name, cfg in config.items():
        try:
            count = execute_scalar(f"SELECT COUNT(*) FROM {cfg['target']}")
            results[name] = {
                "count":  int(count),
                "passed": count >= cfg["min_rows"],
            }
        except Exception as exc:
            results[name] = {"count": 0, "passed": False, "error": str(exc)}
    close_pool()
    return results


@task
def write_report(summary: dict, counts: dict) -> str:
    """
    TaskFlow: receives summary and counts as direct arguments.
    No xcom_pull needed — Airflow injects them automatically.
    Returns output path.
    """
    output_dir = PROJECT_ROOT / "airflow" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "audit_report_taskflow.md"

    with open(report_path, "w") as f:
        f.write(f"# Audit Report (TaskFlow)\n")
        f.write(f"Generated: {datetime.now()}\n\n")
        f.write(f"## Summary\n")
        f.write(f"- Total Runs: {summary['total_runs']}\n")
        f.write(f"- Successful: {summary['successful']}\n")
        f.write(f"- Failed: {summary['failed']}\n")
        f.write(f"- Total Rows: {summary['total_rows']:,}\n\n")
        f.write(f"## Pipeline Counts\n")
        for name, result in counts.items():
            status = "✅ PASS" if result["passed"] else "❌ FAIL"
            f.write(f"- **{name}**: {result['count']} rows {status}\n")

    print(f"Report written → {report_path}")
    return str(report_path)


@dag(
    dag_id="audit_report_taskflow",
    description="Audit report using TaskFlow API",
    schedule=[CUSTOMER_DATASET, FILM_DATASET, RENTAL_DATASET],  # ← event-driven
#    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["audit", "taskflow", "sprint-04"],
    default_args={
        "owner": "python-de-journey",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
)
def audit_pipeline():
    """
    TaskFlow DAG — task dependencies inferred from function arguments.
    write_report(summary, counts) automatically depends on
    read_audit_log() and check_counts() because their return values
    are passed as arguments.
    """
    summary = read_audit_log()
    counts  = check_counts()
    write_report(summary, counts)   # ← Airflow sees summary+counts as deps


# Instantiate the DAG
audit_dag = audit_pipeline()