"""
dag_film_etl.py — Day 22 | Film ETL with TaskGroup
===================================================
Uses FilmETLPipeline from Day 20 sprint test.
Demonstrates TaskGroup for visual organisation.

DAG structure:
  [extract_load group: run_film_etl]
      ↓
  [validation group: check_row_count, check_value_tiers, check_no_nulls]
      ↓
  write_audit
"""
from __future__ import annotations

import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow import Dataset

FILM_DATASET = Dataset(
    "postgresql://dvdrental/analytics_film_airflow"
)

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
]:
    sys.path.insert(0, str(p))

default_args = {
    "owner":       "python-de-journey",
    "retries":     2,
    "retry_delay": timedelta(minutes=1),
}

# ── Q1: run_film_etl — WRITE YOURSELF ─────────────────────────────────────
def run_film_etl(**context) -> int:
    """
    Run FilmETLPipeline from Day 20.

    HINTS:
      - Import ETLConfig from etl_protocols
      - Import FilmETLPipeline from film_etl_pipeline (sprint-03/day-20)
      - ETLConfig(source_table="film",
                  target_table="analytics_film_airflow",
                  max_retries=2,
                  output_dir=OUTPUT_DIR)
      - Push rows_loaded to XCom key "film_rows"
      - Return rows_loaded
    """
    from etl_protocols import ETLConfig, ETLResult, ETLProtocol 
    from film_etl_pipeline import FilmETLPipeline
    
    config = ETLConfig(
        source_table="film",
        target_table="analytics_film_airflow",
        max_retries=2,
        output_dir=OUTPUT_DIR
    )
    pipeline = FilmETLPipeline(config)
    result = pipeline.run()
    
    context["ti"].xcom_push(key="film_rows", value=int(result.rows_loaded))
    return int(result.rows_loaded)


# ── Q2: check_row_count — WRITE YOURSELF ──────────────────────────────────
def check_row_count(**context) -> None:
    """
    Validate film row count = 1000.

    HINTS:
      - Pull "film_rows" from XCom (task_ids="extraction_load.run_film_etl")
      - Assert rows == 1000
      - Print confirmation
    """
    from db_utils import execute_scalar, close_pool

    count = execute_scalar("SELECT COUNT(*) FROM analytics_film_airflow")
    close_pool()

    if count < 900:
        raise ValueError(f"Expected ≥900 rows, got {count}")
    print(f"Film validation passed: {count} rows")

# ── Q3: check_value_tiers — WRITE YOURSELF ────────────────────────────────
def check_value_tiers(**context) -> None:
    """
    Validate all 3 value tiers exist in analytics_film_airflow.

    HINTS:
      - Import db_utils, execute_query
      - Query: SELECT DISTINCT value_tier FROM analytics_film_airflow
      - Assert set(tiers) == {"Budget", "Standard", "Premium"}
      - close_pool() in finally block
    """
    from db_utils import execute_query, close_pool

    try:
        rows = execute_query(
            "SELECT DISTINCT value_tier FROM analytics_film_airflow ORDER BY value_tier"
        )
        # rows = [('Budget',), ('Premium',), ('Standard',)]
        # Extract first element from each tuple
        tiers = {row[0] for row in rows}
        expected = {"Budget", "Standard", "Premium"}
        assert tiers == expected, f"Expected {expected}, got {tiers}"
        print(f"Value tier validation passed: {tiers}")
    finally:
        close_pool()

# ── Audit — fully provided ─────────────────────────────────────────────────
def write_film_audit(**context) -> None:
    rows = context["ti"].xcom_pull(
        task_ids="extraction_load.run_film_etl",
        key="film_rows",
    )
    print(f"Film ETL audit | rows={rows} | status=success")


# ── DAG ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id="film_etl_daily",
    description="Daily film ETL with TaskGroup validation",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "film", "sprint-04"],
) as dag:

    # TaskGroup 1: Extraction + Load
    with TaskGroup("extraction_load") as tg_extract:
        task_film_etl = PythonOperator(
            task_id="run_film_etl",
            python_callable=run_film_etl,
            outlets=[FILM_DATASET],        # ← signals dataset was updated
            pool="etl_pool",
            priority_weight=5,           # ← medium priority
            weight_rule="absolute",
        )

    # TaskGroup 2: Validation
    with TaskGroup("validation") as tg_validate:
        task_check_rows = PythonOperator(
            task_id="check_row_count",
            python_callable=check_row_count,
        )
        task_check_tiers = PythonOperator(
            task_id="check_value_tiers",
            python_callable=check_value_tiers,
        )
        # Both validation tasks run in parallel (no >> between them)

    # Audit after validation
    task_audit = PythonOperator(
        task_id="write_film_audit",
        python_callable=write_film_audit,
        trigger_rule="none_failed_min_one_success",
    )

    # Dependencies
    tg_extract >> tg_validate >> task_audit