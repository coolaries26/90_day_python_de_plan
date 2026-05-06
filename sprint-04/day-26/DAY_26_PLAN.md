# 📅 DAY 26 — Sprint 04 | Pools, Priorities + Concurrency Control
## Prevent DB Overload, Task Queuing, Priority Weights

---

## 🔁 RETROSPECTIVE — Day 25

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Backfill 3 dates | ✅ Pass | customer_etl_daily ran correctly |
| Dynamic DAG 8 tasks | ✅ Pass | All 8 tables profiled and passing |
| summarise_profiles | ✅ Pass | MD table clean, all ✅ |
| TaskFlow @task/@dag | ✅ Pass | Dependency inference working |
| Failed: 3 in audit | ✅ Expected | failure_test Day 24 runs |

### Pre-Start
```bash
# WSL2
source ~/.bashrc
airflow dags list

# Windows Git Bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-04/day-26-pools-priorities
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-06: Workflow Orchestration                                |
| Story           | ST-26: Airflow Pools + Priority Weights                      |
| Task ID         | TASK-026                                                     |
| Sprint          | Sprint 04 (Days 21–28)                                       |
| Story Points    | 2                                                            |
| Priority        | HIGH                                                         |
| Labels          | airflow, pools, priority, concurrency, day-26                |
| Acceptance Criteria | db_connection_pool created with 3 slots; profiler tasks use pool; customer ETL has higher priority than film ETL; concurrency verified |

---

## 📚 BACKGROUND

### The Problem: 8 Parallel DB Queries

Your `dvdrental_table_profiler` DAG has `max_active_tasks=4` — but even 4
parallel PostgreSQL connections can overwhelm a small local server.
In production, you might have 50+ DAGs all hitting the same DB.

```
Without pools:
  Airflow runs as many tasks as it wants → DB gets 50 connections
  PostgreSQL max_connections=100 → connection refused errors
  Pipelines fail → data not loaded → dashboard shows stale data

With pools:
  Airflow pool "db_connections" has 3 slots
  Only 3 tasks can hold a DB connection simultaneously
  Task 4 waits in queue → no connection errors
```

### Airflow Pools

```python
# Create pool (CLI):
airflow pools set db_connection_pool 3 "Max 3 concurrent DB connections"
#                  ↑ pool name        ↑ slots  ↑ description

# Assign task to pool:
task = PythonOperator(
    task_id="query_db",
    pool="db_connection_pool",   # ← this task consumes 1 slot
    pool_slots=1,                # ← how many slots this task uses (default 1)
    ...
)

# Heavy task consuming 2 slots:
task = PythonOperator(
    task_id="heavy_query",
    pool="db_connection_pool",
    pool_slots=2,   # ← takes 2 of the 3 available slots
)
```

### Priority Weights

```python
# Higher weight = runs first when multiple tasks are queued
task = PythonOperator(
    task_id="critical_etl",
    priority_weight=10,    # runs before weight=1 tasks
    weight_rule="absolute",
)

# Weight rules:
# "downstream" (default) → weight = task weight + sum of downstream weights
# "upstream"             → weight = task weight + sum of upstream weights
# "absolute"             → weight = exactly task weight, ignoring dependencies
```

### DAG-Level Concurrency

```python
with DAG(
    dag_id="my_dag",
    max_active_tasks=3,     # max tasks running at once across all runs
    max_active_runs=1,      # only 1 DAG run at a time (no overlap)
    concurrency=3,          # alias for max_active_tasks
) as dag:
```

---

## 🎯 OBJECTIVES

1. Create `db_connection_pool` with 3 slots in Airflow
2. Update `dvdrental_table_profiler` to use the pool
3. Add priority weights — customer ETL > film ETL > audit report
4. Set `max_active_runs=1` on all ETL DAGs (prevent overlapping runs)
5. Write `dag_priority_demo.py` — demonstrates queuing behaviour
6. Push clean `[DAY-026][S04]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 20 min   | Create pools + verify in UI                        |
| B     | 30 min   | Update profiler + ETL DAGs with pool + priority    |
| C     | 35 min   | `dag_priority_demo.py`                             |
| D     | 15 min   | Trigger + verify queuing behaviour                 |
| E     | 20 min   | Git push + merge                                   |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Create Pools (Block A)
**[Full steps]**

```bash
# Create pools
airflow pools set db_connection_pool 3 \
    "Max 3 concurrent PostgreSQL connections"

airflow pools set etl_pool 2 \
    "Max 2 concurrent ETL pipeline runs"

# Verify
airflow pools list
```

**Expected:**
```
Pool                | Slots | Running | Queued | Scheduled | Open
====================+=======+=========+========+===========+=====
db_connection_pool  | 3     | 0       | 0      | 0         | 3
etl_pool            | 2     | 0       | 0      | 0         | 2
default_pool        | 128   | 0       | 0      | 0         | 128
```

---

### EXERCISE 2 — Update Profiler DAG with Pool (Block B)
**[Full steps]**

Open `dag_table_profiler.py` and make these changes:

```python
# 1. Add max_active_runs to DAG definition:
with DAG(
    dag_id="dvdrental_table_profiler",
    max_active_tasks=4,
    max_active_runs=1,          # ← prevent overlapping runs
    ...
) as dag:

# 2. Add pool to each dynamic profile task:
    profile_tasks = []
    for cfg in DVDRENTAL_TABLES:
        task = PythonOperator(
            task_id=f"profile_{cfg['table']}",
            python_callable=profile_table,
            op_kwargs={...},
            pool="db_connection_pool",  # ← max 3 run simultaneously
            pool_slots=1,
        )
        profile_tasks.append(task)
```

**Update all ETL DAGs with priority weights:**

In `dag_customer_etl.py`:
```python
# Add to DAG definition:
with DAG(
    dag_id="customer_etl_daily",
    max_active_runs=1,
    ...
) as dag:

# Add to ETL task:
    task_etl = PythonOperator(
        task_id="run_customer_etl",
        python_callable=run_customer_etl,
        pool="etl_pool",
        priority_weight=10,          # ← highest priority
        weight_rule="absolute",
        sla=timedelta(minutes=5),
    )
```

In `dag_film_etl.py`:
```python
    task_load = PythonOperator(
        task_id="transform_and_load_films",
        python_callable=transform_and_load_films,
        pool="etl_pool",
        priority_weight=5,           # ← medium priority
        weight_rule="absolute",
    )
```

In `dag_audit_report.py` and `dag_audit_taskflow.py`:
```python
# Audit tasks have lowest priority
# Add to default_args:
default_args = {
    ...
    "priority_weight": 1,            # ← lowest priority
    "weight_rule": "absolute",
}
```

---

### EXERCISE 3 — dag_priority_demo.py (Block C)
**[Q1 fully provided. Q2 write yourself]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/dag_priority_demo.py`:

```python
"""
dag_priority_demo.py — Day 26 | Pool + Priority Demo
=====================================================
Creates 6 tasks that all compete for 2 pool slots.
High priority tasks should run before low priority tasks.
Demonstrates queuing behaviour visually in Airflow UI.
"""
from __future__ import annotations
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
            pool_slots=1,
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
    # summary_task = PythonOperator(...)
    # tasks >> summary_task

    # For now — chain all tasks to run independently (no dependencies between them)
    # They will queue on the pool slots
```

---

### EXERCISE 4 — Verify Pool Queuing (Block D)

```bash
# Trigger priority demo
airflow dags unpause priority_demo
airflow dags trigger priority_demo

# Watch task states in real time (run this in a separate terminal)
watch -n 2 "airflow tasks states-for-dag-run priority_demo \
    \$(airflow dags list-runs -d priority_demo --output json 2>/dev/null | \
    python3 -c 'import json,sys; runs=json.load(sys.stdin); print(runs[0][\"run_id\"])') \
    2>/dev/null | grep -v WARNING"

# Check pool slot usage during run
airflow pools list
```

**In Airflow UI:**
```
Go to: Admin → Pools
Watch: db_connection_pool Running slots fill to max 2
       Queue builds up, then drains as tasks complete
       Higher priority tasks start before lower priority
```

---

### EXERCISE 5 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 26 --sprint 4 ^
    --message "Airflow: db_connection_pool, etl_pool, priority weights, max_active_runs=1, priority_demo DAG" ^
    --merge
```

---

## ✅ DAY 26 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `db_connection_pool` created — 3 slots                                   | [ ]   |
| 2 | `etl_pool` created — 2 slots                                             | [ ]   |
| 3 | `dvdrental_table_profiler` uses `db_connection_pool`                     | [ ]   |
| 4 | `customer_etl_daily` uses `etl_pool` + `priority_weight=10`              | [ ]   |
| 5 | `dag_film_etl` uses `etl_pool` + `priority_weight=5`                     | [ ]   |
| 6 | Audit DAGs have `priority_weight=1`                                      | [ ]   |
| 7 | All ETL DAGs have `max_active_runs=1`                                    | [ ]   |
| 8 | `dag_priority_demo.py` created — 6 tasks competing for 2 pool slots      | [ ]   |
| 9 | **Summary task written by you — execution order printed**                | [ ]   |
|10 | Priority demo triggers and completes successfully                        | [ ]   |
|11 | One clean `[DAY-026][S04]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 EXPECTED POOL BEHAVIOUR

```
db_connection_pool has 2 slots (for demo):
  t=0: critical_report (p=10) starts  → slot 1 taken
  t=0: daily_etl (p=8) starts         → slot 2 taken
  t=0: weekly_summary (p=5) queued    ← waiting for slot
  t=0: audit_log (p=3) queued         ← waiting for slot
  t=3: critical_report finishes       → slot 1 freed
  t=3: weekly_summary (p=5) starts    → slot 1 taken
  ...

High priority tasks run first because when a slot opens,
Airflow picks the highest-priority queued task next.
```

---

## 🔜 PREVIEW: DAY 27

**Topic:** Airflow data-aware scheduling + dataset triggers  
**What you'll do:** Use Airflow 2.4+ Datasets to trigger `pipeline_audit_report`
automatically when `customer_etl_daily` and `film_etl_daily` both complete.
Replace the manual schedule with event-driven triggering.

---

*Day 26 | Sprint 04 | EP-06 | TASK-026*
