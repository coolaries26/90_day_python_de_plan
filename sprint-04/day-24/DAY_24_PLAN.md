# 📅 DAY 24 — Sprint 04 | Retry Strategies + SLA + Failure Alerting
## Exponential Backoff, SLA Deadlines, on_failure_callback, Simulated Failure

---

## 🔁 RETROSPECTIVE — Day 23

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Airflow Variables (6 keys) | ✅ Pass | JSON config pattern correct |
| read_audit_log + check_counts parallel | ✅ Pass | Fork pattern working |
| write_audit_report | ✅ Pass | MD + CSV clean, both PASS |
| airflow_meta_conn added | ✅ Pass | |
| Uncommitted files in commit | ⚠️ Minor | Check git status before staging |

### Pre-Start
```bash
# WSL2
source ~/.bashrc
airflow dags list

# Windows Git Bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-04/day-24-retry-sla-alerts
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-06: Workflow Orchestration                                |
| Story           | ST-24: Retry Strategies + SLA + Alerting                     |
| Task ID         | TASK-024                                                     |
| Sprint          | Sprint 04 (Days 21–28)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | airflow, retry, sla, alerting, callbacks, day-24             |
| Acceptance Criteria | Exponential backoff on retries; SLA set on critical task; on_failure_callback fires on simulated failure; alert logged to DB |

---

## 📚 BACKGROUND

### Why Retry Strategy Matters

```
Default retry (what you have now):
  retry_delay = timedelta(minutes=1)   ← flat retry, same wait every time

Problem:
  If DB is down for maintenance, retrying every 60s hammers it.
  If an API rate-limits you, flat retry makes it worse.

Production pattern — Exponential Backoff:
  Attempt 1 fails → wait 1 min
  Attempt 2 fails → wait 2 min
  Attempt 3 fails → wait 4 min
  Attempt 4 fails → wait 8 min
  Attempt 5 fails → alert + give up
```

### SLA — Service Level Agreement

```python
# SLA = maximum time a task is ALLOWED to take
# If exceeded → SLA miss event fires (logs, alert, but task continues)

task = PythonOperator(
    task_id="run_etl",
    sla=timedelta(minutes=5),   # must complete within 5 minutes
    ...
)
```

### Callbacks — Hook Into DAG/Task Events

```python
# Three callback types:
on_failure_callback   → task or DAG failed
on_success_callback   → task or DAG succeeded
on_retry_callback     → task is being retried
sla_miss_callback     → SLA was exceeded (DAG-level only)

# Callback signature:
def my_callback(context):
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    exception = context.get("exception")
```

### What "Alert" Means in This Program

Real Airflow alerts go to email or Slack. Since we haven't configured SMTP,
today's "alert" writes a structured failure record to `etl_audit_log`
(the same table you've been using) with `status='failed'`.
This is actually the correct production pattern — log first, notify second.

---

## 🎯 OBJECTIVES

1. Add exponential backoff to `customer_etl_daily`
2. Add SLA of 5 minutes to the ETL task
3. Write `on_failure_callback` that logs to `etl_audit_log`
4. Write `on_retry_callback` that logs retry attempt
5. Create `dag_failure_test.py` — deliberately fails to test callbacks
6. Verify failure record appears in `etl_audit_log`
7. Push clean `[DAY-024][S04]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 20 min   | Write callback functions in `airflow_callbacks.py` |
| B     | 25 min   | Update `customer_etl_daily` with retry + SLA       |
| C     | 35 min   | `dag_failure_test.py` — deliberate failure test    |
| D     | 20 min   | Trigger failure, verify DB record                  |
| E     | 20 min   | Git push + merge                                   |

---

## 📝 EXERCISES

---

### EXERCISE 1 — airflow_callbacks.py (Block A)
**[Fully provided — study carefully, used by all DAGs]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/airflow_callbacks.py`:

```python
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
     "cat /etc/resolv.conf | grep nameserver | awk '{print $2}'"],
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
        from models import AuditLog

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
```

---

### EXERCISE 2 — Update customer_etl_daily with Retry + SLA (Block B)
**[Full steps — add to existing DAG]**

Open `dag_customer_etl.py` and make these changes:

```python
# Add import at top:
from datetime import timedelta
from airflow_callbacks import on_failure, on_retry

# Replace default_args with exponential backoff version:
default_args = {
    "owner":              "python-de-journey",
    "retries":            3,
    "retry_delay":        timedelta(minutes=1),
    "retry_exponential_backoff": True,   # ← exponential backoff
    "max_retry_delay":    timedelta(minutes=10),  # ← cap at 10 min
    "on_failure_callback": on_failure,
    "on_retry_callback":   on_retry,
}

# Add SLA to the main ETL task:
task_etl = PythonOperator(
    task_id="run_customer_etl",
    python_callable=run_customer_etl,
    sla=timedelta(minutes=5),   # ← must complete in 5 min
)
```

**Verify DAG still loads:**
```bash
airflow dags list
# No import errors
airflow dags show customer_etl_daily
```

---

### EXERCISE 3 — dag_failure_test.py (Block C)
**[Q1 fully provided. Q2 write yourself]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/dag_failure_test.py`:

```python
"""
dag_failure_test.py — Day 24 | Callback Testing DAG
====================================================
Deliberately fails tasks to verify:
  1. on_failure_callback fires and writes to etl_audit_log
  2. on_retry_callback fires on each retry
  3. Exponential backoff delays are applied

Run once, then check etl_audit_log for failure records.
DO NOT schedule this DAG — trigger manually only.
"""
from __future__ import annotations
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import subprocess
_ip = subprocess.run(
    ["bash", "-c",
     "cat /etc/resolv.conf | grep nameserver | awk '{print $2}'"],
    capture_output=True, text=True,
).stdout.strip()
os.environ["DB_HOST"] = _ip or "172.18.144.1"

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-02"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-04"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint-03" / "day-16"))
sys.path.insert(0, str(Path(__file__).parent))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_callbacks import on_failure, on_retry


# ── Q1: Task that always fails — provided ────────────────────────────────
def always_fails(**context) -> None:
    """
    This task always raises an exception.
    Used to test that on_failure_callback fires correctly.
    """
    attempt = context["task_instance"].try_number
    print(f"always_fails: attempt {attempt} — about to raise")
    raise RuntimeError(
        f"Deliberate failure on attempt {attempt} "
        f"— testing retry and callback logic"
    )


# ── Q1: Task that fails twice then succeeds — provided ───────────────────
def fails_twice_then_succeeds(**context) -> None:
    """
    Fails on attempts 1 and 2, succeeds on attempt 3.
    Tests that retry logic and exponential backoff work correctly.
    """
    attempt = context["task_instance"].try_number
    print(f"fails_twice_then_succeeds: attempt {attempt}")

    if attempt < 3:
        raise ValueError(
            f"Simulated transient failure on attempt {attempt}"
        )
    print(f"Success on attempt {attempt} ✅")


# ── Q2: Task that checks failure was logged — WRITE THIS YOURSELF ─────────
def verify_failure_logged(**context) -> None:
    """
    Q2 — YOUR TASK:
    Verify that the on_failure_callback wrote a record to etl_audit_log.

    HINTS:
      - from db_utils import execute_query, close_pool
      - Query etl_audit_log WHERE status = 'failed'
        AND pipeline_name LIKE '%always_fails%'
        ORDER BY run_at DESC LIMIT 1
      - Assert len(rows) > 0
      - Print the failure record
      - close_pool()

    Expected: at least one row with status='failed' for always_fails task
    Note: this task only runs if always_fails has exhausted all retries
          so trigger_rule="all_done" is needed (see DAG definition below)
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement verify_failure_logged")


with DAG(
    dag_id="failure_test",
    description="Tests retry callbacks and failure logging",
    default_args={
        "owner":                     "python-de-journey",
        "retries":                   2,
        "retry_delay":               timedelta(seconds=10),  # short for testing
        "retry_exponential_backoff": True,
        "on_failure_callback":       on_failure,
        "on_retry_callback":         on_retry,
    },
    schedule=None,          # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "callbacks", "sprint-04"],
) as dag:

    task_always_fails = PythonOperator(
        task_id="always_fails",
        python_callable=always_fails,
    )

    task_recovers = PythonOperator(
        task_id="fails_twice_then_succeeds",
        python_callable=fails_twice_then_succeeds,
    )

    task_verify = PythonOperator(
        task_id="verify_failure_logged",
        python_callable=verify_failure_logged,
        trigger_rule="all_done",   # runs even if upstream failed
    )

    # always_fails and fails_twice run in parallel
    # verify runs after both complete (regardless of outcome)
    [task_always_fails, task_recovers] >> task_verify
```

---

### EXERCISE 4 — Trigger + Verify (Block D)

```bash
# Trigger the failure test
airflow dags unpause failure_test
airflow dags trigger failure_test

# Wait for retries to exhaust (~30 seconds with short retry_delay)
sleep 60

# Check run status
airflow dags list-runs -d failure_test --output table | tail -3

# Verify failure record in etl_audit_log
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_query, close_pool
rows = execute_query('''
    SELECT pipeline_name, status, error_message, run_at
    FROM etl_audit_log
    WHERE status = 'failed'
    ORDER BY run_at DESC
    LIMIT 5
''', as_dict=True)
for r in rows:
    print(r)
close_pool()
"
```

**Expected:**
```python
{'pipeline_name': 'failure_test.always_fails',
 'status': 'failed',
 'error_message': '[scheduled__...] Deliberate failure...',
 'run_at': datetime(...)}
```

---

### EXERCISE 5 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 24 --sprint 4 ^
    --message "Airflow: exponential backoff, SLA, on_failure/retry callbacks, failure_test DAG verified" ^
    --merge
```

---

## ✅ DAY 24 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `airflow_callbacks.py` created — on_failure, on_retry, on_success        | [ ]   |
| 2 | `_write_audit_record()` writes to etl_audit_log on failure               | [ ]   |
| 3 | `customer_etl_daily` uses exponential backoff + max_retry_delay          | [ ]   |
| 4 | SLA of 5 minutes on run_customer_etl task                                | [ ]   |
| 5 | `dag_failure_test.py` created                                            | [ ]   |
| 6 | `always_fails` task exhausts retries + on_failure_callback fires         | [ ]   |
| 7 | `fails_twice_then_succeeds` succeeds on attempt 3                        | [ ]   |
| 8 | **`verify_failure_logged` written by you — asserts DB record exists**    | [ ]   |
| 9 | `etl_audit_log` has failure record for `failure_test.always_fails`       | [ ]   |
|10 | `trigger_rule="all_done"` on verify task — runs after failed upstream    | [ ]   |
|11 | One clean `[DAY-024][S04]` commit via `daily_commit.py --merge`          | [ ]   |

---

## ⚠️ WATCH OUT FOR

**Callbacks must never raise exceptions:**
```python
def on_failure(context):
    try:
        _write_audit_record(...)
    except Exception as exc:
        print(f"Callback failed: {exc}")  # log only, never raise
```
If a callback raises, Airflow marks the task in an unexpected state and
the retry logic breaks. Always wrap callback internals in try/except.

**`trigger_rule="all_done"` vs `"all_success"`:**
```
all_success  (default) → only runs if ALL upstream tasks succeeded
all_done               → runs when ALL upstream tasks are done
                         (regardless of success/failure/skip)
none_failed            → runs if no upstream task failed
                         (skipped tasks are OK)
```
`verify_failure_logged` needs `all_done` because `always_fails` will
definitely fail — with the default `all_success`, verify would never run.

---

## 🔜 PREVIEW: DAY 25

**Topic:** Airflow backfill + catchup + dynamic DAGs  
**What you'll do:** Run historical backfill for the customer ETL DAG,
understand `catchup=True/False`, and write a dynamic DAG that generates
tasks from a list — one task per table in the dvdrental database.

---

*Day 24 | Sprint 04 | EP-06 | TASK-024*
