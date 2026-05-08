"""
Requirements:

DAG ID: rental_summary_daily
Schedule: @daily, catchup=False, max_active_runs=1
Uses db_connection_pool pool
Uses on_failure_callback from airflow_callbacks.py
Has exactly 3 tasks using TaskFlow API (@task + @dag):
Task 1: extract_rental_stats
  - Query rental table: total_rentals, open_rentals (return_date IS NULL),
    returned_rentals, unique_customers
  - Return dict with these 4 keys

Task 2: validate_rental_stats(stats: dict)
  - Receives stats from Task 1 automatically (TaskFlow)
  - Assert total_rentals >= 15000
  - Assert unique_customers >= 500
  - Return "validation_passed"

Task 3: write_rental_report(stats: dict, validation: str)
  - Receives stats + validation result
  - Write to /mnt/c/90_day_python_de_plan/airflow/output/rental_report.md
  - Content: timestamp, all 4 stats, validation status
  - Return output file path
Pass criteria:

airflow dags list | grep rental_summary_daily
airflow dags trigger rental_summary_daily
# All 3 tasks green
cat airflow/output/rental_report.md
# Shows all 4 stats with correct values
# Validation status is "validation_passed
"""
#airflow etl pipeline: read from dvdrental>rental, write to target, then summarize in a report
from __future__ import annotations
import subprocess
from datetime import datetime, timedelta
import os
from pathlib import Path
import sys
from airflow import Dataset
from airflow.decorators import dag, task
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
# from .bashrc export

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
OUTPUT_DIR   = PROJECT_ROOT / "airflow" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for p in [
    PROJECT_ROOT / "sprint-01" / "day-02",
    PROJECT_ROOT / "sprint-01" / "day-04",
    PROJECT_ROOT / "sprint-02" / "day-14",
    PROJECT_ROOT / "sprint-03" / "day-14",  # FilmETLPipeline + ETLConfig
    PROJECT_ROOT / "sprint-03" / "day-16",
    PROJECT_ROOT / "sprint-03" / "day-20",  # FilmETLPipeline
    PROJECT_ROOT / "airflow" / "dags",  # audit_report_taskflow
]:
    sys.path.insert(0, str(p))


# ── Q1: run_film_etl — WRITE YOURSELF ─────────────────────────────────────
RENTAL_DATASET = Dataset(
    "postgresql://dvdrental/analytics_rental_airflow"   
)

@task
def extract_rental_stats() -> dict:
    import pandas as pd
    from db_utils import get_engine, dispose_engine

    engine = get_engine()
    df = pd.read_sql("SELECT * FROM rental", engine)
    dispose_engine()

    total_rentals = len(df)
    open_rentals = len(df[df["return_date"].isnull()])
    returned_rentals = len(df[df["return_date"].notnull()])
    unique_customers = df["customer_id"].nunique()

    return {
        "total_rentals": total_rentals,
        "open_rentals": open_rentals,
        "returned_rentals": returned_rentals,
        "unique_customers": unique_customers,
    }

@task
def validate_rental_stats(stats: dict) -> str:
    try:
        assert stats["total_rentals"] >= 15000, "Total rentals must be at least 15000"
        assert stats["unique_customers"] >= 500, "Unique customers must be at least 500"
        return "validation_passed"
    except AssertionError as e:
        print(f"Validation failed: {e}")
        return "validation_failed"  


@task
def write_rental_report(stats: dict, validation: str) -> str:
    output_path = "/mnt/c/90_day_python_de_plan/airflow/output/rental_report.md"
    with open(output_path, "w") as f:
        f.write(f"# Rental Summary Report\n")
        f.write(f"Generated at: {datetime.now()}\n\n")
        f.write(f"- Total Rentals: {stats['total_rentals']}\n")
        f.write(f"- Open Rentals: {stats['open_rentals']}\n")
        f.write(f"- Returned Rentals: {stats['returned_rentals']}\n")
        f.write(f"- Unique Customers: {stats['unique_customers']}\n\n")
        f.write(f"Validation Status: {validation}\n")
    return output_path

#@task
#def write_rental_audit(stats: dict, validation: str) -> None:
#    
#    if validation == "validation_passed": 
#        status = "Success"  # or "failed" based on actual logic
#        rows_loaded = stats["total_rentals"]
#    else:
#        status = "Failed"
#        rows_loaded = 0
#
#    from db_utils import execute_query, close_pool
#    sql = (f"""INSERT INTO etl_audit_log ( pipeline_name, source_table, target_table, status, rows_loaded, run_at
#            ) 
#           VALUES ('rental_summary_daily', 'rental', 'rental_summary', '{status}', {rows_loaded}, NOW()
#           )
#        """)
#    sql = sql.replace("\n", " ")
#    execute_query(sql)
#
#    close_pool()
#
#──── DAG ───────────────────────────────────────────────────────────────────

@dag(
    dag_id="rental_summary_daily",
    description="Daily rental summary report",
        default_args={
        "owner":                     "python-de-journey",
        "retries":                   2,
        "retry_delay":               timedelta(seconds=10),  # short for testing
        "retry_exponential_backoff": True,
        "on_failure_callback":       on_failure,
        "on_retry_callback":         on_retry,
    },

    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1
) 
def rental_pipeline():
    stats = extract_rental_stats()
    validation = validate_rental_stats(stats)
    report = write_rental_report(stats, validation)
    # audit = write_rental_audit(stats, validation)  # runs after report is written
    
    #Dependency: stats → validation → report
    stats >> validation >> report 

rental_pipeline()  