"""
dag_table_profiler.py — Day 25 | Dynamic DAG
=============================================
Generates one profiling task per dvdrental table dynamically.
Tasks are created at DAG parse time from a config list.
Demonstrates: dynamic task generation, op_kwargs, parallel execution.
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
os.environ["DB_HOST"] = _ip or "172.28.224.1"

PROJECT_ROOT = Path("/mnt/d/alsgit/90_day_python_de_plan")
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-02"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd 
from db_utils import get_engine, execute_scalar, dispose_engine, close_pool #type: ignore
from airflow import DAG
from airflow.operators.python import PythonOperator #type: ignore
from airflow_callbacks import on_failure

# ── Table config — add/remove tables here to change task count ───────────
DVDRENTAL_TABLES = [
    {"table": "film",      "min_rows": 900,  "key_col": "film_id"},
    {"table": "customer",  "min_rows": 500,  "key_col": "customer_id"},
    {"table": "rental",    "min_rows": 15000,"key_col": "rental_id"},
    {"table": "payment",   "min_rows": 14000,"key_col": "payment_id"},
    {"table": "inventory", "min_rows": 4000, "key_col": "inventory_id"},
    {"table": "actor",     "min_rows": 150,  "key_col": "actor_id"},
    {"table": "category",  "min_rows": 10,   "key_col": "category_id"},
    {"table": "store",     "min_rows": 1,    "key_col": "store_id"},
]


# ── Q1: Profile one table — fully provided ────────────────────────────────
def profile_table(
    table: str,
    min_rows: int,
    key_col: str,
    **context,
) -> None:
    """
    Profile a single dvdrental table:
      - Row count (with min_rows validation)
      - Null count per column
      - Min/max of key column
    Pushes profile dict to XCom.
    """

    engine = get_engine()

    # Row count
    count = execute_scalar(f"SELECT COUNT(*) FROM {table}")
    print(f"Count: {count})")
    # Null counts
    null_sql = f"""
        SELECT column_name,
               COUNT(*) - COUNT({table}.*) AS null_count
        FROM information_schema.columns c
        CROSS JOIN {table}
        WHERE c.table_name = '{table}'
          AND c.table_schema = 'public'
        GROUP BY column_name
        HAVING COUNT(*) - COUNT({table}.*) > 0
    """
    try:
        null_df = pd.read_sql(f"""
            SELECT column_name,
                   SUM(CASE WHEN {key_col} IS NULL THEN 1 ELSE 0 END) AS nulls
            FROM {table}
            GROUP BY column_name
        """, engine)
        null_count = int(null_df["nulls"].sum()) if len(null_df) > 0 else 0
    except Exception:
        null_count = -1  # couldn't check

    # Key column range
    key_range = execute_scalar(
        f"SELECT MAX({key_col}) - MIN({key_col}) FROM {table}"
    )

    dispose_engine()
    close_pool()

    profile = {
        "table":     table,
        "row_count": int(count),
        "min_rows":  min_rows,
        "passed":    count >= min_rows,
        "null_count": null_count,
        "key_range": int(key_range) if key_range is not None else None,
    }

    status = "✅" if profile["passed"] else "❌"
    print(f"{status} {table}: {count:,} rows (min {min_rows:,})")

    context["ti"].xcom_push(key=f"profile_{table}", value=profile)


# ── Q2: Summarise all profiles — WRITE THIS YOURSELF ─────────────────────
def summarise_profiles(**context) -> None:
    """
    Q2 — YOUR TASK:
    Pull profile XCom from all upstream table tasks.
    Write a summary to airflow/output/table_profiles.md.

    HINTS:
    Step 1: Pull XCom for each table
        profiles = []
        for cfg in DVDRENTAL_TABLES:
            table = cfg["table"]
            profile = context["ti"].xcom_pull(
                task_ids=f"profile_{table}",
                key=f"profile_{table}"
            )
            if profile:
                profiles.append(profile)

    Step 2: Count passed/failed
        passed = sum(1 for p in profiles if p["passed"])
        failed = len(profiles) - passed

    Step 3: Write markdown table to:
        /mnt/d/alsgit/90_day_python_de_plan/airflow/output/table_profiles.md

        Columns: Table | Rows | Min Required | Status
        One row per table from profiles list

    Step 4: Print summary
        print(f"Profiling complete: {passed}/{len(profiles)} tables passed")

    Self-check:
      - 8 tables profiled
      - All 8 should show ✅ PASS
      - table_profiles.md exists in airflow/output/
    """
    # YOUR CODE HERE
    profiles = []
    for cfg in DVDRENTAL_TABLES:
        table = cfg["table"]
        profile = context["ti"].xcom_pull(
            task_ids=f"profile_{table}",
            key=f"profile_{table}"
        )
        pool="db_connection_pool",  # ← max 3 run simultaneously
        pool_slots=2,
        if profile:
            profiles.append(profile)
            passed = sum(1 for p in profiles if p["passed"])
            failed = len(profiles) - passed
            # Write markdown table
            with open("/mnt/d/alsgit/90_day_python_de_plan/airflow/output/table_profiles.md", "w") as f:
                f.write(f"# Generated at: {datetime.now()}\n\n")
                f.write("# Table Profiles\n\n")
                f.write("| Table | Rows | Min Required | Status |\n")
                f.write("|-------|------|--------------|--------|\n")
                for p in profiles:
                    status = "✅ PASS" if p["passed"] else "❌ FAIL"
                    f.write(f"| {p['table']} | {p['row_count']} | {p['min_rows']} | {status} |\n")  
    print(f"Profiling complete: {passed}/{len(profiles)} tables passed")
    

# ── DAG Definition — dynamic task generation ─────────────────────────────
with DAG(
    dag_id="dvdrental_table_profiler",
    description="Profile all dvdrental tables dynamically",
    default_args={
        "owner":               "python-de-journey",
        "retries":             1,
        "retry_delay":         timedelta(minutes=1),
        "on_failure_callback": on_failure,
    },
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["profiling", "dynamic", "sprint-04"],
    max_active_tasks=4,         # run max 4 profile tasks in parallel
    max_active_runs=1,          # ← prevent overlapping runs
) as dag:

    # ── Dynamic task generation ───────────────────────────────────────────
    # One PythonOperator per table — generated in a loop
    profile_tasks = []
    for cfg in DVDRENTAL_TABLES:
        task = PythonOperator(
            task_id=f"profile_{cfg['table']}",
            python_callable=profile_table,
            op_kwargs={                    # ← passes table-specific args
                "table":    cfg["table"],
                "min_rows": cfg["min_rows"],
                "key_col":  cfg["key_col"],
            },
            pool="db_connection_pool",  # ← max 3 run simultaneously
            pool_slots=5,
        )
        profile_tasks.append(task)

    # Summary task runs after ALL profile tasks complete
    summarise = PythonOperator(
        task_id="summarise_profiles",
        python_callable=summarise_profiles,
        trigger_rule="all_done",   # run even if some profiles failed
    )

    # All profile tasks → summarise
    profile_tasks >> summarise