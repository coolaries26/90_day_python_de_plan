DAY_22_PLAN.md
# 📅 DAY 22 — Sprint 04 | Airflow Operators Deep-Dive
## BranchPythonOperator, Sensors, TaskGroups, dag_film_etl.py

---

## 🔁 RETROSPECTIVE — Day 21

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Airflow 2.9.3 in WSL2 | ✅ Pass | Clean install, no dependency conflicts |
| airflow_meta PostgreSQL DB | ✅ Pass | 30+ tables migrated |
| customer_etl_daily DAG | ✅ Pass | Both tasks green |
| analytics_customer_airflow | ✅ Pass | 599 rows confirmed |
| Git commit [DAY-021][S04] | ✅ Pass | Clean, merged to develop |
| Windows IP hardcoded in DAG | ⚠️ Note | 172.18.144.1 changes on restart — fix today |

### Pre-Start Actions
```bash
# WSL2 terminal
source ~/start_airflow.sh

# Check Windows IP hasn't changed since Day 21
WINDOWS_IP=$(cat /etc/resolv.conf | grep nameserver | awk '{print $2}')
echo "Current Windows IP: $WINDOWS_IP"
# If different from 172.18.144.1 — update DAG file and .env

# Branch setup (from Windows terminal)
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-04/day-22-airflow-operators
```

### Fix: Dynamic Windows IP in DAG
Replace the hardcoded IP with a dynamic lookup:
```python
# Add this helper at top of every DAG file:
def get_windows_ip() -> str:
    """Get Windows host IP dynamically — changes after WSL2 restart."""
    import subprocess
    result = subprocess.run(
        ["bash", "-c",
         "ip route | grep default | awk '{print $3}'"],
        capture_output=True, text=True
    )
    return result.stdout.strip() or "172.18.144.1"
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-06: Workflow Orchestration                                |
| Story           | ST-22: Airflow Operators — Branch, Sensor, TaskGroup         |
| Task ID         | TASK-022                                                     |
| Sprint          | Sprint 04 (Days 21–28)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | airflow, branch-operator, sensor, taskgroup, day-22          |
| Acceptance Criteria | BranchPythonOperator routes correctly; FileSensor detects output; TaskGroup organises tasks; dag_film_etl.py runs successfully |

---

## 📚 BACKGROUND

### Operators You Know vs Today's Operators

```
Day 21 (learned):              Day 22 (new):
─────────────────────────────  ──────────────────────────────────
PythonOperator                 BranchPythonOperator
  → run any Python function      → conditional routing (if/else in DAG)

PostgresOperator               FileSensor
  → run SQL                      → wait until a file exists

                               TaskGroup
                                 → organise tasks into visual folders
```

### BranchPythonOperator — DAG-level if/else

```python
# Without branching — always runs all tasks:
extract → transform → load → notify

# With branching — route based on row count:
extract → check_count ──► load_full    ──► notify_success
                      └──► load_sample ──► notify_warning
```

The branch function returns the `task_id` (or list of task_ids) to run next.
Tasks not selected are **skipped** (shown in UI as grey, not failed).

### FileSensor — wait for a condition

```python
# Without sensor:
# Task B runs immediately — fails if Task A's file not ready yet

# With sensor:
# FileSensor polls every 30s until file exists, then Task B runs
extract → [FileSensor: wait for CSV] → transform
```
To use filesensor need to create a connecton first 
Run below in WSL
```python
airflow connections add fs_default \
    --conn-type fs \
    --conn-extra '{"path": "/"}'
```

### TaskGroup — visual organisation

```python
# Without TaskGroup — flat list of 8 tasks in UI:
extract, validate_schema, validate_nulls, validate_counts,
transform, enrich, load, audit

# With TaskGroup — grouped in UI:
[extract] → [validation: schema, nulls, counts] → [transform] → [load+audit]
```

---

## 🎯 OBJECTIVES

1. Fix dynamic Windows IP in `dag_customer_etl.py`
2. Add `BranchPythonOperator` to customer ETL — route based on row count
3. Add `FileSensor` — wait for CSV before downstream tasks
4. Add `TaskGroup` — organise validation tasks
5. Write `dag_film_etl.py` using `FilmETLPipeline` from Day 20
6. Trigger both DAGs — all tasks green
7. Push clean `[DAY-022][S04]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                           |
|-------|----------|----------------------------------------------------|
| A     | 15 min   | IP fix + branch setup                              |
| B     | 35 min   | Upgrade dag_customer_etl.py with branch + sensor   |
| C     | 35 min   | dag_film_etl.py with TaskGroup                     |
| D     | 15 min   | Trigger both DAGs + verify                         |
| E     | 20 min   | Commit + merge                                     |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Upgrade dag_customer_etl.py (Block B)
**[Fully provided — study the branch pattern carefully]**

Replace `dag_customer_etl.py` with the upgraded version:

```python
"""
dag_customer_etl.py — Day 22 | Upgraded with Branch + Sensor
=============================================================
New features:
  - Dynamic Windows IP (no hardcoded 172.x.x.x)
  - BranchPythonOperator: full load vs sample based on row count
  - FileSensor: wait for CSV before downstream processing
  - skip_if_recent: skip run if data is fresh (idempotency pattern)
"""
from __future__ import annotations

import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator

# ── Dynamic Windows IP ────────────────────────────────────────────────────
def _get_windows_ip() -> str:
    try:
        result = subprocess.run(
            ["bash", "-c",
             "cat /etc/resolv.conf | grep nameserver | awk '{print $2}'"],
            capture_output=True, text=True, timeout=5
        )
        ip = result.stdout.strip()
        return ip if ip else "172.18.144.1"
    except Exception:
        return "172.18.144.1"

WINDOWS_IP = _get_windows_ip()
os.environ["DB_HOST"] = WINDOWS_IP

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
OUTPUT_DIR   = PROJECT_ROOT / "airflow" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for p in [
    PROJECT_ROOT / "sprint-01" / "day-02",
    PROJECT_ROOT / "sprint-01" / "day-04",
    PROJECT_ROOT / "sprint-02" / "day-14",
    PROJECT_ROOT / "sprint-03" / "day-16",
]:
    sys.path.insert(0, str(p))

default_args = {
    "owner":           "python-de-journey",
    "retries":         2,
    "retry_delay":     timedelta(minutes=1),
    "depends_on_past": False,
}

# ── Task functions ────────────────────────────────────────────────────────
def run_customer_etl(**context) -> int:
    from etl_protocols import ETLConfig
    from oop_etl import CustomerETLPipeline

    config = ETLConfig(
        source_table="customer",
        target_table="analytics_customer_airflow",
        max_retries=2,
        output_dir=OUTPUT_DIR,
    )
    pipeline = CustomerETLPipeline(config=config)
    result   = pipeline.run()

    context["ti"].xcom_push(key="rows_loaded", value=result.rows_loaded)
    context["ti"].xcom_push(key="csv_path",
                            value=str(config.output_csv))
    print(f"ETL complete | rows={result.rows_loaded}")
    return result.rows_loaded


def branch_on_row_count(**context) -> str:
    """
    BranchPythonOperator function.
    Returns task_id to execute next based on row count.
    - Full load (≥500 rows)  → proceed to validate_full
    - Sample load (<500 rows) → proceed to notify_low_count
    """
    rows = context["ti"].xcom_pull(
        task_ids="run_customer_etl", key="rows_loaded"
    )
    print(f"Branching on row count: {rows}")
    if rows >= 500:
        return "validate_full_load"
    return "notify_low_count"


def validate_full_load(**context) -> None:
    rows = context["ti"].xcom_pull(
        task_ids="run_customer_etl", key="rows_loaded"
    )
    assert rows >= 500, f"Expected ≥500 rows, got {rows}"
    print(f"Full load validated: {rows} rows ✅")


def notify_low_count(**context) -> None:
    rows = context["ti"].xcom_pull(
        task_ids="run_customer_etl", key="rows_loaded"
    )
    print(f"WARNING: Low row count detected: {rows}")
    # In production: send Slack/email alert here


def write_audit(**context) -> None:
    """Write audit record after successful validation."""
    rows = context["ti"].xcom_pull(
        task_ids="run_customer_etl", key="rows_loaded"
    )
    print(f"Audit written | rows={rows} | dag=customer_etl_daily")


# ── DAG ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id="customer_etl_daily",
    description="Daily customer ETL — with branching and sensor",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "customer", "sprint-04"],
) as dag:

    # Task 1: Extract + Transform + Load
    task_etl = PythonOperator(
        task_id="run_customer_etl",
        python_callable=run_customer_etl,
    )

    # Task 2: Wait for CSV file to exist before branching
    task_wait_csv = FileSensor(
        task_id="wait_for_csv",
        filepath=str(OUTPUT_DIR / "analytics_customer_airflow.csv"),
        fs_conn_id="fs_default",    # call the filesensor connection.
        poke_interval=10,    # check every 10 seconds
        timeout=120,         # fail after 2 minutes
        mode="poke",
    )

    # Task 3: Branch based on row count
    task_branch = BranchPythonOperator(
        task_id="branch_on_row_count",
        python_callable=branch_on_row_count,
    )

    # Task 4a: Full load path
    task_validate_full = PythonOperator(
        task_id="validate_full_load",
        python_callable=validate_full_load,
    )

    # Task 4b: Low count path
    task_notify_low = PythonOperator(
        task_id="notify_low_count",
        python_callable=notify_low_count,
    )

    # Task 5: Converge both branches + write audit
    # trigger_rule="none_failed_min_one_success" runs if at least one
    # upstream succeeded (handles branch convergence)
    task_audit = PythonOperator(
        task_id="write_audit",
        python_callable=write_audit,
        trigger_rule="none_failed_min_one_success",
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    task_etl >> task_wait_csv >> task_branch
    task_branch >> task_validate_full >> task_audit
    task_branch >> task_notify_low    >> task_audit
```

---

### EXERCISE 2 — dag_film_etl.py with TaskGroup (Block C)
**[Structure provided — task functions write yourself with hints]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/dag_film_etl.py`:

```python
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

# ── Dynamic Windows IP ────────────────────────────────────────────────────
def _get_windows_ip() -> str:
    try:
        result = subprocess.run(
            ["bash", "-c",
             "cat /etc/resolv.conf | grep nameserver | awk '{print $2}'"],
            capture_output=True, text=True, timeout=5
        )
        ip = result.stdout.strip()
        return ip if ip else "172.18.144.1"
    except Exception:
        return "172.18.144.1"

WINDOWS_IP = _get_windows_ip()
os.environ["DB_HOST"] = WINDOWS_IP

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
    raise NotImplementedError("Implement run_film_etl")


# ── Q2: check_row_count — WRITE YOURSELF ──────────────────────────────────
def check_row_count(**context) -> None:
    """
    Validate film row count = 1000.

    HINTS:
      - Pull "film_rows" from XCom (task_ids="extraction_load.run_film_etl")
      - Assert rows == 1000
      - Print confirmation
    """
    raise NotImplementedError("Implement check_row_count")


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
    raise NotImplementedError("Implement check_value_tiers")


# ── Audit — fully provided ─────────────────────────────────────────────────
def write_film_audit(**context) -> None:
    rows = context["ti"].xcom_pull(
        task_ids="extraction_load.run_film_etl",
        key="film_rows"
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
        trigger_rule="all_success",
    )

    # Dependencies
    tg_extract >> tg_validate >> task_audit
```

---

### EXERCISE 3 — Trigger + Verify Both DAGs (Block D)

```bash
# In WSL2
source ~/start_airflow.sh

# Reload DAGs (scheduler picks up new files automatically, but force check)
airflow dags list
# Should show: customer_etl_daily, film_etl_daily

# Trigger both
airflow dags unpause customer_etl_daily
airflow dags unpause film_etl_daily
airflow dags trigger customer_etl_daily
airflow dags trigger film_etl_daily

# Check states after 60 seconds
sleep 60
airflow dags list-runs -d customer_etl_daily --output table | tail -3
airflow dags list-runs -d film_etl_daily --output table | tail -3
```

**Verify from Windows:**
```bash
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_scalar, close_pool
for table in ['analytics_customer_airflow', 'analytics_film_airflow']:
    count = execute_scalar(f'SELECT COUNT(*) FROM {table}')
    print(f'{table}: {count} rows')
close_pool()
"
```

**Expected:**
```
analytics_customer_airflow: 599 rows
analytics_film_airflow: 1000 rows
```

---

### EXERCISE 4 — Git Push (Block E)

```bash
# Windows terminal
cd C:\Users\Lenovo\python-de-journey
python scripts/daily_commit.py --day 22 --sprint 4 ^
    --message "Airflow: BranchPythonOperator, FileSensor, TaskGroup, dag_film_etl.py, dynamic Windows IP" ^
    --merge
```

---

## ✅ DAY 22 COMPLETION CHECKLIST

| # | Task                                                                  | Done? |
|---|-----------------------------------------------------------------------|-------|
| 1 | Dynamic Windows IP helper in both DAGs                                | [ ]   |
| 2 | `dag_customer_etl.py` upgraded — FileSensor + BranchPythonOperator   | [ ]   |
| 3 | customer_etl_daily: branch goes to `validate_full_load` (≥500 rows)  | [ ]   |
| 4 | `wait_for_csv` sensor passes                                          | [ ]   |
| 5 | `write_audit` task runs with `none_failed_min_one_success`            | [ ]   |
| 6 | **`run_film_etl()` written by you — 1000 rows**                       | [ ]   |
| 7 | **`check_row_count()` written by you — asserts 1000**                 | [ ]   |
| 8 | **`check_value_tiers()` written by you — 3 tiers verified**           | [ ]   |
| 9 | `film_etl_daily` TaskGroup visible in Airflow UI                      | [ ]   |
|10 | Both DAGs green in UI                                                 | [ ]   |
|11 | `analytics_film_airflow` has 1000 rows in PostgreSQL                  | [ ]   |
|12 | One clean `[DAY-022][S04]` commit                                     | [ ]   |

---

## ⚠️ WATCH OUT FOR

**FileSensor on WSL2 paths:**
The `filepath` in `FileSensor` must be the path as seen by the Airflow scheduler
process — which runs in WSL2. So use `/mnt/c/...` not `C:\...`.

**BranchPythonOperator and trigger_rule:**
Tasks downstream of a branch that were NOT selected show as `skipped` (grey).
The convergence task (`write_audit`) needs `trigger_rule="none_failed_min_one_success"`
otherwise it won't run when one branch is skipped.

**XCom task_id with TaskGroup:**
When a task is inside a TaskGroup, its XCom task_id includes the group name:
```python
# Task "run_film_etl" inside TaskGroup "extraction_load":
xcom_pull(task_ids="extraction_load.run_film_etl", key="film_rows")
#                   ↑ group.task format
```

---

## 🔜 PREVIEW: DAY 23

**Topic:** Airflow variables, connections, and XCom patterns  
**What you'll do:** Move hardcoded config (Windows IP, table names, thresholds)
into Airflow Variables. Use `Variable.get()` inside tasks. Add a
`ShortCircuitOperator` to skip the entire DAG if data hasn't changed.

---

*Day 22 | Sprint 04 | EP-06 | TASK-022*