"""
airflow_callbacks.py — Shared Airflow Callback Functions
=========================================================
Callbacks are imported by all DAGs in this project.
They log failure/retry/success events to etl_audit_log.

Usage in DAG:
    from airflow_callbacks import on_failure, on_retry, on_success
    
    default_args = {
        "on_failure_callback": on_failure,
        "on_retry_callback":   on_retry,
    }
"""

from __future__ import annotations
import os
import sys
from pathlib import Path
from datetime import datetime

# Dynamic Windows IP
import subprocess
_ip = subprocess.run(
    ["bash", "-c",
     "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True,
).stdout.strip()
os.environ.setdefault("DB_HOST", _ip or "172.18.144.1")

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-02"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-04"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint-03" / "day-16"))


def _write_audit_record(
    pipeline_name: str,
    source_table: str,
    target_table: str,
    status: str,
    rows_loaded: int = 0,
    elapsed_s: float = 0.0,
    error_message: str | None = None,
) -> None:
    """Write a record to etl_audit_log. Used by all callbacks."""
    try:
        from sqlalchemy.orm import Session
        from db_utils import get_engine, dispose_engine
        try:
            from models_compat import AuditLog   # SQLAlchemy 1.4 (airflow-venv)
        except ImportError:
            from models import AuditLog          # SQLAlchemy 2.0 (windows venv)

        # Build AuditLog manually (no ETLResult available in callbacks)
        audit = AuditLog(
            pipeline_name=pipeline_name,
            source_table=source_table,
            target_table=target_table,
            rows_loaded=rows_loaded,
            status=status,
            elapsed_s=elapsed_s,
            error_message=error_message,
        )
        engine = get_engine()
        with Session(engine) as session:
            session.add(audit)
            session.commit()
        dispose_engine()
    except Exception as exc:
        # Callbacks must NEVER raise — log to stdout only
        print(f"CALLBACK: audit write failed (non-fatal): {exc}")


def on_failure(context: dict) -> None:
    """
    Called when a task fails after all retries are exhausted.
    Logs failure record to etl_audit_log.
    """
# Fix : on_failure_callback — open airflow_callbacks.py in WSL2
# Add guard at top of on_failure():
    ti = context["task_instance"]
    if ti.try_number - 1 < ti.max_tries:
        return   # not final failure

    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context.get("run_id", "unknown")
    exc      = context.get("exception")
    error    = str(exc)[:500] if exc else "Unknown error"

    print(f"FAILURE CALLBACK | dag={dag_id} task={task_id} run={run_id}")
    print(f"  Error: {error}")

    _write_audit_record(
        pipeline_name=f"{dag_id}.{task_id}",
        source_table=dag_id,
        target_table="etl_audit_log",
        status="failed",
        error_message=f"[{run_id}] {error}",
    )


def on_retry(context: dict) -> None:
    """
    Called each time a task is retried.
    Logs retry attempt to stdout (not DB — retries are frequent).
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    try_num = context["task_instance"].try_number
    exc     = context.get("exception")

    print(
        f"RETRY CALLBACK | dag={dag_id} task={task_id} "
        f"attempt={try_num} error={exc}"
    )


def on_success(context: dict) -> None:
    """
    Called when a task succeeds.
    Lightweight — just prints, no DB write (success is already logged by pipeline).
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    print(f"SUCCESS CALLBACK | dag={dag_id} task={task_id}")


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """
    Called when an SLA deadline is missed.
    Note: sla_miss_callback is DAG-level, not task-level.
    Signature is different from other callbacks.
    """
    print(f"SLA MISS | dag={dag.dag_id} tasks={[t.task_id for t in task_list]}")
    for sla in slas:
        _write_audit_record(
            pipeline_name=f"{dag.dag_id}.SLA_MISS",
            source_table=dag.dag_id,
            target_table="etl_audit_log",
            status="sla_miss",
            error_message=f"SLA missed for task: {sla.task_id}",
        )