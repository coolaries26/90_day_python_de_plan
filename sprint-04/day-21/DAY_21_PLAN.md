# 📅 DAY 21 — Sprint 04 Begins | Apache Airflow Setup
## Install Airflow, Configure PostgreSQL Metadata DB, Write First DAG

---

## 🔁 RETROSPECTIVE — Day 20 Sprint Test

### Final Score: 96/100 — PASS
| Item | Result | Fix Required |
|------|--------|--------------|
| T1 FilmETLPipeline | ✅ 20/20 | Remove custom __repr__, inherit from Base |
| T2 mypy clean | ✅ 10/10 | |
| T3 3/3 tests | ✅ 20/20 | |
| T4 Tier analysis | ✅ 15/15 | |
| T5 Alembic migration | ✅ 18/20 | Add sprint-03-complete tag |
| Code quality | ✅ 5/5 | |
| Git | 8/10 | Tag fix below |

### Pre-Start Actions
```bash
cd C:\Users\Lenovo\python-de-journey

# Fix missing sprint-03-complete tag
git tag sprint-03-complete
git push origin sprint-03-complete
git tag
# Should show: sprint-01-complete, sprint-02-complete, sprint-03-complete

# Create Day 21 branch
git checkout develop
git pull origin develop
git checkout -b sprint-04/day-21-airflow-setup
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-06: Workflow Orchestration                                |
| Story           | ST-21: Airflow Setup + First DAG                             |
| Task ID         | TASK-021                                                     |
| Sprint          | Sprint 04 (Days 21–28)                                       |
| Story Points    | 3                                                            |
| Priority        | CRITICAL                                                     |
| Labels          | airflow, dag, orchestration, postgresql, day-21              |
| Acceptance Criteria | Airflow running locally; PostgreSQL as metadata DB; first DAG runs CustomerETLPipeline successfully; visible in Airflow UI |

---

## 📚 BACKGROUND

### What Airflow Is

```
Airflow = a platform to programmatically schedule and monitor workflows.

Without Airflow:                With Airflow:
─────────────────────────────  ──────────────────────────────────────
python etl_resilient.py        Scheduled DAG runs automatically
Manual re-runs on failure      Automatic retry with backoff
No visibility                  Full UI: logs, status, history
Scripts run independently      Dependencies managed (A before B before C)
No alerting                    Email/Slack on failure
```

### Core Airflow Concepts

```
DAG (Directed Acyclic Graph)
  └── A Python file describing a workflow
  └── Has a schedule (daily, hourly, @once)
  └── Contains Tasks connected by dependencies

Task
  └── A single unit of work
  └── Uses an Operator to define what it does

Operator Types Used Today:
  PythonOperator    → run any Python function
  PostgresOperator  → run SQL directly
  BashOperator      → run shell commands

DAG Run
  └── One execution of the full DAG
  └── Has a logical date (execution_date)
  └── Tasks run in dependency order
```

### Airflow Architecture (local setup)

```
Browser → Airflow Webserver (port 8080)
               │
               ▼
         Airflow Scheduler (reads DAG files, triggers tasks)
               │
               ▼
         Airflow Metadata DB (PostgreSQL — tracks runs, logs, state)
               │
               ▼
         Your DAG code (sprint-04/dags/*.py)
               │
               ▼
         Your ETL pipelines (sprint-02, sprint-03 code)
```

### Why PostgreSQL as Metadata DB

Airflow defaults to SQLite — single-user, no concurrent writes, corrupts under load.
We use PostgreSQL (your existing `dvdrental` server) with a **separate database**
`airflow_meta` for Airflow's own tables. Never mix Airflow metadata with your ETL data.

---

## 🎯 OBJECTIVES

1. Install Apache Airflow 2.8.x with PostgreSQL provider
2. Create `airflow_meta` database + `airflow` user in PostgreSQL
3. Configure Airflow to use PostgreSQL metadata DB
4. Initialise Airflow database (`airflow db init`)
5. Create admin user
6. Write `dag_customer_etl.py` — first DAG running CustomerETLPipeline
7. Start webserver + scheduler, trigger DAG, verify in UI
8. Push clean — one commit `[DAY-021][S04]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                        |
|-------|----------|-------------------------------------------------|
| A     | 15 min   | Tag fix + branch + PostgreSQL airflow_meta setup |
| B     | 25 min   | Airflow install + configuration                 |
| C     | 20 min   | Airflow db init + admin user                    |
| D     | 30 min   | First DAG — dag_customer_etl.py                 |
| E     | 30 min   | Start services + trigger + verify + git push    |

---

## 📝 EXERCISES

---

### EXERCISE 1 — PostgreSQL: airflow_meta DB + airflow user (Block A)
**[Full steps — separate DB from dvdrental]**

```bash
# Connect as postgres superuser
psql -U postgres -h 127.0.0.1
```

```sql
-- Create dedicated Airflow user
CREATE USER airflow WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    PASSWORD 'Airflow@2024!';

-- Create Airflow metadata database
CREATE DATABASE airflow_meta
    WITH OWNER = airflow
    ENCODING = 'UTF8'
    TEMPLATE = template0;

-- Grant full access to airflow user on its own DB
GRANT ALL PRIVILEGES ON DATABASE airflow_meta TO airflow;

-- Connect to airflow_meta and grant schema permissions
\c airflow_meta
GRANT ALL ON SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL ON SEQUENCES TO airflow;

\q
```

**Verify:**
```bash
psql -h 127.0.0.1 -U airflow -d airflow_meta -W
# Enter: Airflow@2024!
# \conninfo → should show airflow connected to airflow_meta
# \q
```

---

### EXERCISE 2 — Install Airflow (Block B)
**[Full steps — Airflow install is complex, follow exactly]**

```bash
.venv\Scripts\activate
cd C:\Users\Lenovo\python-de-journey

# Set Airflow home — all config, logs, dags stored here
set AIRFLOW_HOME=C:\Users\Lenovo\python-de-journey\airflow

# Install Airflow 2.8.1 with PostgreSQL + constraints
# Constraints file pins all dependencies to known-working versions
pip install apache-airflow==2.8.1 ^
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.12.txt"

# Install PostgreSQL provider
pip install apache-airflow-providers-postgres==5.10.0

# Verify
airflow version
# Should output: 2.8.1
```

**Configure Airflow to use PostgreSQL:**
```bash
# Airflow init creates the config file on first run
# Run it once with SQLite to generate airflow.cfg, then switch to PostgreSQL
airflow db init

# Edit the config file
# Location: C:\Users\Lenovo\python-de-journey\airflow\airflow.cfg
```

Open `airflow\airflow.cfg` and change these values:

```ini
# Find and replace the sql_alchemy_conn line:
# BEFORE (SQLite default):
sql_alchemy_conn = sqlite:////C:/Users/Lenovo/python-de-journey/airflow/airflow.db

# AFTER (PostgreSQL):
sql_alchemy_conn = postgresql+psycopg2://airflow:Airflow%402024%21@127.0.0.1:5432/airflow_meta

# Also set:
executor = LocalExecutor          # allows parallel task execution
load_examples = False             # don't clutter UI with example DAGs
dags_folder = C:\Users\Lenovo\python-de-journey\airflow\dags

# Set parallelism (optional but good practice):
parallelism = 4
max_active_runs_per_dag = 1
```

**Re-initialise with PostgreSQL:**
```bash
# Drop SQLite db and reinit with PostgreSQL
airflow db init

# Verify tables created in PostgreSQL
psql -h 127.0.0.1 -U airflow -d airflow_meta -c "\dt" -W
# Should show 30+ Airflow tables
```

---

### EXERCISE 3 — Create Admin User + DAGs Folder (Block C)
**[Full steps]**

```bash
# Create Airflow admin user
airflow users create ^
    --username admin ^
    --firstname Admin ^
    --lastname User ^
    --role Admin ^
    --email admin@python-de-journey.local ^
    --password Admin@2024!

# Create DAGs folder
mkdir C:\Users\Lenovo\python-de-journey\airflow\dags

# Verify user created
airflow users list
```

---

### EXERCISE 4 — First DAG: dag_customer_etl.py (Block D)
**[Fully provided — study structure carefully]**

Create `airflow\dags\dag_customer_etl.py`:

```python
"""
dag_customer_etl.py — Day 21 | First Airflow DAG
=================================================
Orchestrates the CustomerETLPipeline on a daily schedule.
Demonstrates:
  - DAG definition with schedule
  - PythonOperator wrapping existing ETL code
  - Task dependencies (extract → transform → load)
  - XCom for passing data between tasks
  - PostgresOperator for SQL verification

DAG ID: customer_etl_daily
Schedule: Daily at midnight
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

# ── Add project to Python path ─────────────────────────────────────────────
# Airflow runs DAGs in its own process — must add project paths explicitly
PROJECT_ROOT = Path("C:/Users/Lenovo/python-de-journey")
for path in [
    PROJECT_ROOT / "sprint-01" / "day-02",
    PROJECT_ROOT / "sprint-01" / "day-04",
    PROJECT_ROOT / "sprint-02" / "day-14",
    PROJECT_ROOT / "sprint-03" / "day-16",
]:
    sys.path.insert(0, str(path))

# ── Default arguments — apply to all tasks unless overridden ───────────────
default_args = {
    "owner":            "python-de-journey",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
}

# ── Task functions ─────────────────────────────────────────────────────────
def run_customer_etl(**context) -> dict:
    """
    PythonOperator task: run CustomerETLPipeline.
    Returns result dict pushed to XCom automatically.
    context['ti'] = TaskInstance — use for XCom push/pull
    """
    from etl_protocols import ETLConfig
    from oop_etl import CustomerETLPipeline

    config = ETLConfig(
        source_table="customer",
        target_table="analytics_customer_airflow",
        max_retries=2,
        output_dir=PROJECT_ROOT / "airflow" / "output",
    )
    pipeline = CustomerETLPipeline(config=config)
    result = pipeline.run()

    # Push result summary to XCom — accessible by downstream tasks
    context["ti"].xcom_push(
        key="etl_result",
        value={
            "rows_loaded":  result.rows_loaded,
            "status":       result.status,
            "elapsed_s":    result.elapsed_seconds,
            "pipeline":     result.pipeline_name,
        }
    )
    return {"rows_loaded": result.rows_loaded, "status": result.status}


def log_run_summary(**context) -> None:
    """
    PythonOperator task: pull XCom result and log summary.
    Demonstrates XCom pull from upstream task.
    """
    import logging
    log = logging.getLogger(__name__)

    # Pull result from upstream task via XCom
    result = context["ti"].xcom_pull(
        task_ids="run_customer_etl",
        key="etl_result"
    )
    log.info(f"ETL Summary | pipeline={result['pipeline']} "
             f"rows={result['rows_loaded']} "
             f"status={result['status']} "
             f"elapsed={result['elapsed_s']:.2f}s")


def validate_row_count(**context) -> None:
    """
    PythonOperator task: verify row count matches expectation.
    Raises ValueError if count is wrong — fails the DAG run.
    """
    import sys
    sys.path.insert(0, str(PROJECT_ROOT / "sprint-01" / "day-02"))
    from db_utils import execute_scalar, close_pool

    count = execute_scalar(
        "SELECT COUNT(*) FROM analytics_customer_airflow"
    )
    close_pool()

    if count < 500:
        raise ValueError(
            f"Row count validation FAILED: expected ≥500, got {count}"
        )
    print(f"Row count validation PASSED: {count} rows")


# ── DAG Definition ─────────────────────────────────────────────────────────
with DAG(
    dag_id="customer_etl_daily",
    description="Daily customer ETL pipeline — Python DE Journey",
    default_args=default_args,
    schedule="@daily",              # run once per day
    start_date=datetime(2026, 1, 1),
    catchup=False,                  # don't backfill historical runs
    tags=["etl", "customer", "sprint-04"],
    doc_md="""
    ## Customer ETL Daily DAG

    Runs the CustomerETLPipeline daily:
    1. Extracts customer + rental + payment data from dvdrental
    2. Transforms: adds value_segment, days_since_last_payment
    3. Loads to analytics_customer_airflow table
    4. Validates row count ≥ 500
    5. Logs summary via XCom
    """,
) as dag:

    # Task 1: Run the ETL pipeline
    task_run_etl = PythonOperator(
        task_id="run_customer_etl",
        python_callable=run_customer_etl,
    )

    # Task 2: Validate row count in DB
    task_validate = PythonOperator(
        task_id="validate_row_count",
        python_callable=validate_row_count,
    )

    # Task 3: Log summary
    task_log_summary = PythonOperator(
        task_id="log_run_summary",
        python_callable=log_run_summary,
    )

    # Task 4: Write audit record via SQL
    task_audit_sql = PostgresOperator(
        task_id="write_audit_record",
        postgres_conn_id="dvdrental_appuser",   # configure in Airflow UI
        sql="""
            INSERT INTO etl_audit_log
                (pipeline_name, source_table, target_table,
                 rows_loaded, status, elapsed_s)
            VALUES
                ('customer_etl_daily', 'customer',
                 'analytics_customer_airflow',
                 (SELECT COUNT(*) FROM analytics_customer_airflow),
                 'success', 0)
            ON CONFLICT DO NOTHING;
        """,
    )

    # ── Task Dependencies ──────────────────────────────────────────────────
    # run_etl → validate → log_summary → audit
    task_run_etl >> task_validate >> task_log_summary >> task_audit_sql
```

---

### EXERCISE 5 — Configure DB Connection in Airflow UI + Start Services (Block E)

**Step 1: Add PostgreSQL connection in Airflow UI**
```bash
# Add dvdrental connection via CLI (before starting UI)
airflow connections add dvdrental_appuser ^
    --conn-type postgres ^
    --conn-host 127.0.0.1 ^
    --conn-port 5432 ^
    --conn-schema dvdrental ^
    --conn-login appuser ^
    --conn-password "AppUser@2024!"
```

**Step 2: Start Airflow services (two separate terminals)**
```bash
# Terminal 1 — Webserver
set AIRFLOW_HOME=C:\Users\Lenovo\python-de-journey\airflow
airflow webserver --port 8080

# Terminal 2 — Scheduler
set AIRFLOW_HOME=C:\Users\Lenovo\python-de-journey\airflow
airflow scheduler
```

**Step 3: Open UI and trigger DAG**
```
1. Open browser: http://localhost:8080
2. Login: admin / Admin@2024!
3. Find DAG: customer_etl_daily
4. Toggle ON (unpause)
5. Click "Trigger DAG" (play button)
6. Watch tasks: run_customer_etl → validate → log_summary → write_audit_record
7. Click each task → View Log → verify output
```

**Step 4: Verify via Python**
```bash
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_scalar, close_pool
count = execute_scalar('SELECT COUNT(*) FROM analytics_customer_airflow')
print(f'analytics_customer_airflow rows: {count}')
close_pool()
"
# Expected: 599
```

---

### EXERCISE 6 — Git Push

```bash
cd C:\Users\Lenovo\python-de-journey

python scripts/daily_commit.py --day 21 --sprint 4 ^
    --message "Airflow setup: PostgreSQL metadata DB, first DAG customer_etl_daily, all 4 tasks green" ^
    --merge
```

---

## ✅ DAY 21 COMPLETION CHECKLIST

| # | Task                                                                  | Done? |
|---|-----------------------------------------------------------------------|-------|
| 1 | `sprint-03-complete` tag created and pushed                           | [ ]   |
| 2 | `airflow_meta` database created with `airflow` user                   | [ ]   |
| 3 | Airflow 2.8.1 installed — `airflow version` shows 2.8.1              | [ ]   |
| 4 | `airflow.cfg` uses PostgreSQL connection string                       | [ ]   |
| 5 | `airflow db init` runs against PostgreSQL — 30+ tables created        | [ ]   |
| 6 | Admin user created — can log in at http://localhost:8080              | [ ]   |
| 7 | `dag_customer_etl.py` created in `airflow/dags/`                     | [ ]   |
| 8 | DAG visible in Airflow UI without import errors                       | [ ]   |
| 9 | `dvdrental_appuser` connection configured in Airflow                  | [ ]   |
|10 | All 4 tasks green on first trigger                                    | [ ]   |
|11 | `analytics_customer_airflow` table has 599 rows in PostgreSQL         | [ ]   |
|12 | One clean `[DAY-021][S04]` commit via `daily_commit.py --merge`       | [ ]   |

---

## ⚠️ WINDOWS-SPECIFIC AIRFLOW NOTES

**Airflow on Windows has known issues:**
Airflow is primarily designed for Linux/macOS. On Windows, the recommended approach
is to use WSL2 (Windows Subsystem for Linux) or Docker. If you hit errors:

**Option A — WSL2 (recommended):**
```bash
# Install WSL2 if not already present (PowerShell as Admin):
wsl --install
# Then install Ubuntu, then follow Linux install steps inside WSL2
```

**Option B — Continue on Windows with workarounds:**
```bash
# If you get "no module named 'fcntl'" error:
pip install apache-airflow[postgres]==2.8.1 ^
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.12.txt"

# If SequentialExecutor is forced (Windows limitation):
# Change in airflow.cfg:
executor = SequentialExecutor   # single-threaded, fine for learning
```

**Option C — Docker (cleanest for Windows):**
```bash
# If WSL2 and native both fail, use Docker
# I'll provide a docker-compose.yml on request
```

Let me know immediately if you hit Windows-specific errors — Airflow on Windows
requires specific fixes that I'll provide inline.

---

## 🔜 PREVIEW: DAY 22

**Topic:** Airflow operators deep-dive  
**What you'll do:** Add `BranchPythonOperator` (conditional task routing),
`SensorOperator` (wait for condition), and task groups to the customer ETL DAG.
Also write `dag_film_etl.py` using your `FilmETLPipeline` from the sprint test.

---

*Day 21 | Sprint 04 | EP-06 | TASK-021*
