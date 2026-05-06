"""
dag_priority_demo.py — Day 26 | Pool + Priority Demo
=====================================================
Creates 6 tasks that all compete for 2 pool slots.
High priority tasks should run before low priority tasks.
Demonstrates queuing behaviour visually in Airflow UI.
"""
from __future__ import annotations
from multiprocessing import context
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Q1: Task function — provided ──────────────────────────────────────────
def simulate_db_work(task_name: str, duration: int, **context) -> None:
    """Simulates a DB-heavy task that holds a pool slot."""
    attempt = context["task_instance"].try_number
    print(f"[{task_name}] Starting (attempt {attempt})")
    print(f"[{task_name}] Holding db_connection_pool slot for {duration}s")
    time.sleep(duration)
    print(f"[{task_name}] Complete ✅")

def print_execution_order(**context) -> None:
    """Prints the execution order of tasks based on their start times."""
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    print("\nExecution order based on start times:")
    for ti in sorted(task_instances, key=lambda x: x.start_date or datetime.max):
        print(f"{ti.task_id}: started={ti.start_date} duration={ti.duration}")

with DAG(
    dag_id="priority_demo",
    description="Demonstrates pool slots and priority queuing",
    schedule=None,             # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "pools", "sprint-04"],
    default_args={
        "owner": "python-de-journey",
        "retries": 0,
    },
) as dag:

    # 6 tasks competing for 2 pool slots
    # High priority (weight=10) should start before low priority (weight=1)
    TASK_CONFIG = [
        {"name": "critical_report",  "duration": 3, "priority": 10},
        {"name": "daily_etl",        "duration": 4, "priority": 8},
        {"name": "weekly_summary",   "duration": 3, "priority": 5},
        {"name": "audit_log",        "duration": 2, "priority": 3},
        {"name": "cleanup_old_data", "duration": 2, "priority": 2},
        {"name": "archive_csv",      "duration": 1, "priority": 1},
    ]

    tasks = []
    for cfg in TASK_CONFIG:
        t = PythonOperator(
            task_id=cfg["name"],
            python_callable=simulate_db_work,
            op_kwargs={
                "task_name": cfg["name"],
                "duration":  cfg["duration"],
            },
            pool="db_connection_pool",
            pool_slots=3,
            priority_weight=cfg["priority"],
            weight_rule="absolute",
        )
        tasks.append(t)

    # ── Q2: Add a summary task — WRITE THIS YOURSELF ──────────────────────
    # YOUR TASK: Add a final task that:
    # 1. Runs after ALL 6 tasks complete (trigger_rule="all_done")
    # 2. Pulls start/end times from each task via:
    #    ti = context["task_instance"]
    #    dag_run = context["dag_run"]
    #    task_instances = dag_run.get_task_instances()
    # 3. Prints execution order to show priority worked:
    #    for ti in sorted(task_instances, key=lambda x: x.start_date or datetime.max):
    #        print(f"{ti.task_id}: started={ti.start_date} duration={ti.duration}")
    # 4. Does NOT use db_connection_pool (no pool= argument)

    # YOUR CODE HERE:
    summary_task = PythonOperator(
        task_id="summary_task",
        trigger_rule="all_done",
        python_callable=print_execution_order,
    )
    tasks >> summary_task

    # For now — chain all tasks to run independently (no dependencies between them)
    # They will queue on the pool slots
    