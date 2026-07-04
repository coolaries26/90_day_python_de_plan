# 📅 DAY 55 — Sprint 08 | dbt + Airflow + Streamlit Integration
## Orchestrate dbt from Airflow, Point Dashboard at dbt Marts

---

## 🔁 RETROSPECTIVE — Day 54

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Index 1 (state+revenue) | ✅ Pass | 0.5x — correctly identified overhead on small table |
| Index 2 (month+late) | ✅ Pass | 5.9x speedup |
| Index 3 (segment+spend) | ✅ Pass | 11.6x speedup |
| Partitioning | ✅ Pass | 2.1x single month, 1.1x quarter |
| optimisation_report.md | ✅ Pass | Documented index overhead finding |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-08/day-55-integration
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-11: Advanced SQL & dbt |
| Story           | ST-55: dbt + Airflow + Streamlit Integration |
| Task ID         | TASK-055 |
| Sprint          | Sprint 08 (Days 49–56) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | dbt, airflow, streamlit, integration, day-55 |
| Acceptance Criteria | Airflow DAG runs dbt; Streamlit dashboard reads dbt marts; full pipeline runs end-to-end |

---

## 📚 BACKGROUND

### Why Orchestrate dbt with Airflow?

```
Without orchestration:
  You manually run `dbt run` after ETL updates raw data
  Easy to forget, no scheduling, no failure alerting

With Airflow:
  raw data loads → dbt run triggers automatically → dbt test validates
  → failure triggers your existing on_failure_callback → alert logged
  Same observability you built in Sprint 04, now covering dbt too
```

### BashOperator for dbt

```python
from airflow.operators.bash import BashOperator

# Airflow can run dbt via shell command — simplest integration method
task_dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=(
        "cd /mnt/d/alsgit/90_day_python_de_plan/sprint-08/day-51/ecommerce_dbt && "
        "dbt run --profiles-dir /mnt/d/alsgit/90_day_python_de_plan/.dbt"
    ),
)
```

### Dashboard Migration: raw analytics → dbt marts

```
Before (capstone/dashboard/db.py):
  SELECT * FROM analytics.customer_ltv     ← Python ETL output

After (today):
  SELECT * FROM dbt_dev_marts.mart_customer_ltv  ← dbt output

Both contain similar data, but dbt marts have:
  - Built-in tests (data quality guaranteed)
  - Documentation (self-describing)
  - Lineage tracking (you know exactly where data comes from)
```

---

## 🎯 OBJECTIVES

1. Create `dag_dbt_pipeline.py` — runs dbt via Airflow
2. Connect it to the existing `dag_ecommerce_etl` via dataset trigger
3. Update `capstone/dashboard/db.py` to read from dbt marts
4. Run the full chain: ETL → dbt run → dbt test → Streamlit refresh
5. Push clean `[DAY-055][S08]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 15 min | Branch + WSL2 dbt install |
| B | 45 min | `dag_dbt_pipeline.py` |
| C | 30 min | Update dashboard db.py |
| D | 20 min | Trigger + verify end-to-end |
| E | 10 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Install dbt in WSL2 (Block A)

Airflow runs in WSL2, so dbt needs to be available there too:

```bash
# WSL2
source ~/airflow-venv/bin/activate
pip install dbt-postgres==1.7.4

# Verify
dbt --version

# Test connection from WSL2 — uses Windows mounted path
cd /mnt/d/alsgit/90_day_python_de_plan/sprint-08/day-51/ecommerce_dbt
dbt debug --profiles-dir /mnt/d/alsgit/90_day_python_de_plan/.dbt
```

If `dbt debug` fails with connection error, the `.dbt/profiles.yml` host needs the WSL2 dynamic IP:

```bash
# Check current profiles.yml host setting
cat /mnt/d/alsgit/90_day_python_de_plan/.dbt/profiles.yml | grep host

# If it's 127.0.0.1, that won't work from WSL2 — needs Windows IP
WINDOWS_IP=$(ip route | grep default | awk '{print $3}')
echo "Use this IP in profiles.yml: $WINDOWS_IP"

# Update profiles.yml host line manually, or create a WSL2-specific target:
```

**Add a WSL2 target to profiles.yml:**
```yaml
ecommerce_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: 127.0.0.1     # for Windows-side dbt runs
      port: 5432
      user: appuser
      password: "AppUser@2024!"
      dbname: ecommerce_db
      schema: dbt_dev
      threads: 4
    wsl:
      type: postgres
      host: "172.18.144.1"   # ← replace with your actual WSL2 Windows IP
      port: 5432
      user: appuser
      password: "AppUser@2024!"
      dbname: ecommerce_db
      schema: dbt_dev
      threads: 4
```

```bash
# Test WSL2 target
dbt debug --target wsl --profiles-dir /mnt/d/alsgit/90_day_python_de_plan/.dbt
```

---

### EXERCISE 2 — dag_dbt_pipeline.py (Block B)
**[Tasks 1-2 provided. Tasks 3-4 write yourself]**

Create `/mnt/d/alsgit/90_day_python_de_plan/airflow/dags/dag_dbt_pipeline.py`:

```python
"""
dag_dbt_pipeline.py — Day 55 | dbt Orchestration DAG
======================================================
Runs dbt models and tests after ecommerce ETL completes.
Triggered by dataset event from dag_ecommerce_etl.
"""
from __future__ import annotations
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

_ip = subprocess.run(
    ["bash", "-c", "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True,
).stdout.strip()

DBT_PROJECT_DIR = "/mnt/d/alsgit/90_day_python_de_plan/sprint-08/day-51/ecommerce_dbt"
DBT_PROFILES_DIR = "/mnt/d/alsgit/90_day_python_de_plan/.dbt"

import sys
sys.path.insert(0, "/mnt/d/alsgit/90_day_python_de_plan/airflow/dags")
sys.path.insert(0, "/mnt/d/alsgit/90_day_python_de_plan/sprint-01/day-04")
sys.path.insert(0, "/mnt/d/alsgit/90_day_python_de_plan/sprint-01/day-02")
sys.path.insert(0, "/mnt/d/alsgit/90_day_python_de_plan/sprint-03/day-16")

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow_callbacks import on_failure

ECOMMERCE_DATASET = Dataset("postgresql://ecommerce_db/raw")
DBT_MART_DATASET  = Dataset("postgresql://ecommerce_db/dbt_dev_marts")

default_args = {
    "owner":               "python-de-journey",
    "retries":             1,
    "retry_delay":         timedelta(minutes=2),
    "on_failure_callback": on_failure,
}


def log_dbt_run(**context) -> None:
    """Log the dbt run results to etl_audit_log."""
    try:
        from sqlalchemy.orm import Session
        from db_utils import get_engine, dispose_engine
        from models_compat import AuditLog

        engine = get_engine()
        audit = AuditLog(
            pipeline_name="dag_dbt_pipeline",
            source_table="ecommerce_db.raw",
            target_table="ecommerce_db.dbt_dev_marts",
            rows_loaded=0,
            status="success",
        )
        with Session(engine) as session:
            session.add(audit)
            session.commit()
        dispose_engine()
        print("dbt run logged to etl_audit_log")
    except Exception as exc:
        print(f"Audit log failed (non-fatal): {exc}")


with DAG(
    dag_id="dag_dbt_pipeline",
    description="Run dbt models + tests after ecommerce ETL",
    default_args=default_args,
    schedule=[ECOMMERCE_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "capstone", "sprint-08"],
) as dag:

    # ── Task 1: dbt run — provided ────────────────────────────────────────
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --target wsl"
        ),
    )

    # ── Task 2: dbt test — provided ───────────────────────────────────────
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --target wsl"
        ),
    )

    # ── Task 3: dbt snapshot — WRITE THIS YOURSELF ────────────────────────
    # HINTS:
    # task_dbt_snapshot = BashOperator(
    #     task_id="dbt_snapshot",
    #     bash_command=(
    #         f"cd {DBT_PROJECT_DIR} && "
    #         f"dbt snapshot --profiles-dir {DBT_PROFILES_DIR} --target wsl"
    #     ),
    # )
    # YOUR CODE HERE

    # ── Task 4: log run — WRITE THIS YOURSELF ─────────────────────────────
    # HINTS:
    # task_log = PythonOperator(
    #     task_id="log_dbt_run",
    #     python_callable=log_dbt_run,
    #     outlets=[DBT_MART_DATASET],   # signals marts are fresh
    #     trigger_rule="all_done",
    # )
    # YOUR CODE HERE

    # ── Dependencies ───────────────────────────────────────────────────────
    # YOUR TASK: chain all 4 tasks
    # task_dbt_run >> task_dbt_test >> task_dbt_snapshot >> task_log
```

---

### EXERCISE 3 — Update Dashboard db.py (Block C)
**[Write yourself]**

Update `capstone/dashboard/db.py` to read from dbt marts instead of analytics schema:

```python
# YOUR TASK: Update these functions to use dbt marts

@st.cache_data(ttl=300)
def load_customer_ltv() -> pd.DataFrame:
    # CHANGE FROM:
    # return pd.read_sql("SELECT * FROM analytics.customer_ltv", _engine())
    # TO:
    return pd.read_sql("SELECT * FROM dbt_dev_marts.mart_customer_ltv", _engine())


@st.cache_data(ttl=300)
def load_order_metrics() -> pd.DataFrame:
    # CHANGE FROM:
    # return pd.read_sql("SELECT * FROM analytics.order_metrics WHERE ...", _engine())
    # TO:
    return pd.read_sql("""
        SELECT * FROM dbt_dev_marts.mart_order_metrics
        WHERE delivery_days IS NOT NULL
    """, _engine())

# Keep load_seller_performance, load_product_analytics, load_monthly_revenue
# pointing at analytics schema for now (no dbt models built for these yet)
# This demonstrates a REALISTIC hybrid state — not everything migrates at once
```

---

### EXERCISE 4 — Trigger Full Chain + Verify (Block D)

```bash
# WSL2 — trigger the full pipeline
airflow dags unpause dag_dbt_pipeline
airflow dags trigger dag_ecommerce_etl

# This should cascade:
# dag_ecommerce_etl completes → ECOMMERCE_DATASET updated
#   → dag_dbt_pipeline triggers automatically
#     → dbt_run → dbt_test → dbt_snapshot → log_dbt_run

sleep 120
airflow dags list-runs -d dag_dbt_pipeline --output table | tail -3

# Verify dashboard works with new data source
streamlit run capstone/dashboard/app.py --server.port 8502 --server.headless true &
sleep 5
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8502
pkill -f "capstone/dashboard" 2>/dev/null
```

---

### EXERCISE 5 — Git Push

```bash
cd C:\90_day_python_de_plan
python scripts/daily_commit.py --day 55 --sprint 8 ^
    --message "Integration: Airflow runs dbt run/test/snapshot, Streamlit reads dbt marts" ^
    --merge
```

---

## ✅ DAY 55 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | dbt installed in WSL2 airflow-venv | [ ] |
| 2 | `dbt debug --target wsl` passes | [ ] |
| 3 | `dag_dbt_pipeline.py` created with 4 tasks | [ ] |
| 4 | **Task 3: dbt_snapshot BashOperator written** | [ ] |
| 5 | **Task 4: log_dbt_run PythonOperator written** | [ ] |
| 6 | Dataset trigger chains ETL → dbt pipeline | [ ] |
| 7 | **`db.py` updated — customer_ltv + order_metrics read from dbt marts** | [ ] |
| 8 | Full chain triggers and completes successfully | [ ] |
| 9 | Streamlit dashboard still works with new data source | [ ] |
|10 | One clean `[DAY-055][S08]` commit | [ ] |

---

## 🔜 PREVIEW: DAY 56 — Sprint 08 Test

**Sprint 08 final test** — 4 tasks covering window functions, dbt, and optimisation.
Close sprint with `sprint-08-complete` tag.
Sprint 09 begins Day 57.

---

*Day 55 | Sprint 08 | EP-11 | TASK-055*
