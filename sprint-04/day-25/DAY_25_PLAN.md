# 📅 DAY 25 — Sprint 04 | Backfill + Dynamic DAGs
## catchup, backfill CLI, TaskFlow API, Dynamic Task Generation

---

## 🔁 RETROSPECTIVE — Day 24

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| airflow_callbacks.py | ✅ Pass | on_failure writes to etl_audit_log |
| exponential backoff | ✅ Pass | retry_exponential_backoff=True |
| SLA on ETL task | ✅ Pass | 5 min SLA configured |
| failure_test DAG | ✅ Pass | attempt 3 failure logged correctly |
| models_compat.py | ✅ Pass | SQLAlchemy 1.4 shim working |
| ip route fix | ✅ Pass | resolv.conf removed from all DAGs |

### Pre-Start
```bash
# WSL2
source ~/.bashrc
airflow dags list

# Windows Git Bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-04/day-25-backfill-dynamic-dags
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-06: Workflow Orchestration                                |
| Story           | ST-25: Backfill + Dynamic DAG Generation                     |
| Task ID         | TASK-025                                                     |
| Sprint          | Sprint 04 (Days 21–28)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | airflow, backfill, catchup, dynamic-dags, taskflow, day-25   |
| Acceptance Criteria | Backfill runs successfully for 3 historical dates; dynamic DAG generates one task per dvdrental table; TaskFlow API used |

---

## 📚 BACKGROUND

### catchup vs backfill

```
catchup=True  (Airflow default):
  When a DAG is unpaused, Airflow runs ALL missed schedule intervals
  from start_date to now. A DAG started 30 days ago with @daily
  would immediately queue 30 runs.
  → DANGEROUS for new DAGs — always set catchup=False

catchup=False (what all your DAGs use):
  Only runs the most recent interval going forward.
  Past intervals are ignored unless you explicitly backfill.
  → SAFE — use this in all DAGs

backfill (CLI):
  Manually trigger runs for specific historical date ranges.
  Use when: new pipeline needs to process historical data,
            or a DAG was broken and needs re-processing.

airflow dags backfill \
    --start-date 2026-05-01 \
    --end-date 2026-05-03 \
    customer_etl_daily
```

### Dynamic DAGs — Generate Tasks at Parse Time

```python
# Static DAG — tasks hardcoded:
task_film     = PythonOperator(task_id="process_film", ...)
task_customer = PythonOperator(task_id="process_customer", ...)
task_rental   = PythonOperator(task_id="process_rental", ...)

# Dynamic DAG — tasks generated from a list:
TABLES = ["film", "customer", "rental", "payment", "inventory"]

for table in TABLES:
    PythonOperator(
        task_id=f"process_{table}",
        python_callable=process_table,
        op_kwargs={"table": table},
    )

# Result: 5 tasks auto-generated, one per table
# Add a table to TABLES → task appears automatically
```

### TaskFlow API — Modern Airflow Syntax

```python
# Old style (what you've used so far):
def my_task(**context):
    result = do_work()
    context["ti"].xcom_push(key="result", value=result)

task = PythonOperator(task_id="my_task", python_callable=my_task)

# TaskFlow API (Airflow 2.0+) — cleaner, automatic XCom:
from airflow.decorators import task, dag

@task
def my_task() -> dict:
    result = do_work()
    return result   # automatically pushed to XCom

# Return value is automatically available to downstream @task functions
```

---

## 🎯 OBJECTIVES

1. Run `airflow dags backfill` for 3 historical dates on `customer_etl_daily`
2. Write `dag_table_profiler.py` — dynamic DAG, one task per dvdrental table
3. Rewrite `dag_audit_report.py` using TaskFlow API (`@task` decorator)
4. Verify dynamic tasks appear in Airflow UI
5. Push clean `[DAY-025][S04]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 20 min   | Backfill exercise + verify                         |
| B     | 45 min   | `dag_table_profiler.py` — dynamic DAG              |
| C     | 35 min   | Rewrite audit report with TaskFlow API             |
| D     | 20 min   | Trigger + verify both DAGs                         |
| E     | ... | Git push                                           |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Backfill (Block A)
**[Full steps]**

```bash
# First check current run history
airflow dags list-runs -d customer_etl_daily --output table | head -10

# Run backfill for 3 specific dates
# Note: --reset-dagruns allows re-running dates that already ran
airflow dags backfill \
    --start-date 2026-05-01 \
    --end-date 2026-05-03 \
    --reset-dagruns \
    customer_etl_daily

# Watch runs appear
airflow dags list-runs -d customer_etl_daily --output table | head -10
```

**Expected:** 3 new runs appear (one per day: May 01, 02, 03) with state `success`.

**Verify audit log captured all 3:**
```bash
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_query, close_pool
rows = execute_query('''
    SELECT pipeline_name, status, rows_loaded, run_at
    FROM etl_audit_log
    WHERE pipeline_name LIKE '%customer%'
    ORDER BY run_at DESC LIMIT 5
''', as_dict=True)
for r in rows: print(r)
close_pool()
"
```

---

### EXERCISE 2 — dag_table_profiler.py: Dynamic DAG (Block B)
**[Q1 fully provided. Q2 write yourself]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/dag_table_profiler.py`:

```python
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
os.environ["DB_HOST"] = _ip or "172.18.144.1"

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-02"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).parent))

from airflow import DAG
from airflow.operators.python import PythonOperator
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
    import pandas as pd
    from db_utils import get_engine, execute_scalar, dispose_engine, close_pool

    engine = get_engine()

    # Row count
    count = execute_scalar(f"SELECT COUNT(*) FROM {table}")

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
        /mnt/c/90_day_python_de_plan/airflow/output/table_profiles.md

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
    raise NotImplementedError("Implement summarise_profiles")


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
    max_active_tasks=4,   # run max 4 profile tasks in parallel
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
```

---

### EXERCISE 3 — Rewrite audit report with TaskFlow API (Block C)
**[Q1 fully provided — compare with Day 23 version]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/dag_audit_taskflow.py`:

```python
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

_ip = subprocess.run(
    ["bash", "-c", "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True,
).stdout.strip()
os.environ["DB_HOST"] = _ip or "172.18.144.1"

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
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
    schedule="@daily",
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
```

---

### EXERCISE 4 — Trigger + Verify (Block D)

```bash
# Verify all 3 new DAGs load
airflow dags list | grep -E "profiler|taskflow|audit"

# Trigger profiler
airflow dags unpause dvdrental_table_profiler
airflow dags trigger dvdrental_table_profiler

# Trigger taskflow audit
airflow dags unpause audit_report_taskflow
airflow dags trigger audit_report_taskflow

# Wait and check
sleep 60
airflow dags list-runs -d dvdrental_table_profiler --output table | tail -3
airflow dags list-runs -d audit_report_taskflow --output table | tail -3

# Verify output files
ls /mnt/c/90_day_python_de_plan/airflow/output/
cat /mnt/c/90_day_python_de_plan/airflow/output/table_profiles.md
cat /mnt/c/90_day_python_de_plan/airflow/output/audit_report_taskflow.md
```

---

### EXERCISE 5 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 25 --sprint 4 ^
    --message "Airflow: backfill 3 dates, dynamic DAG 8 tables, TaskFlow API audit report" ^
    --merge
```

---

## ✅ DAY 25 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | Backfill ran for 3 dates on customer_etl_daily                           | [ ]   |
| 2 | 3 backfill runs show state=success in Airflow                            | [ ]   |
| 3 | `dag_table_profiler.py` created — 8 dynamic tasks                        | [ ]   |
| 4 | All 8 profile tasks visible in Airflow UI                                | [ ]   |
| 5 | **`summarise_profiles` written by you — MD report created**              | [ ]   |
| 6 | `table_profiles.md` shows all 8 tables passing                           | [ ]   |
| 7 | `dag_audit_taskflow.py` created with `@task` + `@dag` decorators         | [ ]   |
| 8 | `write_report` receives args directly (no xcom_pull)                     | [ ]   |
| 9 | `audit_report_taskflow.md` exists in output/                             | [ ]   |
|10 | One clean `[DAY-025][S04]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 KEY DIFFERENCE — Old vs TaskFlow API

```python
# OLD (PythonOperator):
def task_b(**context):
    data = context["ti"].xcom_pull(task_ids="task_a", key="return_value")
    # use data

# TaskFlow (@task):
@task
def task_a() -> dict:
    return {"key": "value"}   # auto XCom push

@task
def task_b(data: dict) -> None:
    # data is injected automatically — no xcom_pull needed
    pass

# In @dag:
result = task_a()
task_b(result)   # dependency + XCom wired in one line
```

---

## 🔜 PREVIEW: DAY 26

**Topic:** Airflow pools, priorities, and concurrency control  
**What you'll do:** Create connection pools to limit concurrent DB connections,
set task priorities, and configure DAG-level concurrency limits.
Prevent the table profiler from overwhelming PostgreSQL with 8 parallel queries.

---

*Day 25 | Sprint 04 | EP-06 | TASK-025*
