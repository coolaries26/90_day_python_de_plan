# 📅 DAY 23 — Sprint 04 | XCom, Variables, Connections
## Pass Data Between Tasks, Store Config in Variables, Multi-DB Connections

---

## 🔁 RETROSPECTIVE — Day 22

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| BranchPythonOperator | ✅ Pass | Full/incremental routing correct |
| FileSensor | ✅ Pass | fs_default connection created |
| TaskGroup film ETL | ✅ Pass | 1000 rows confirmed |
| Dynamic Windows IP | ✅ Pass | No hardcoded IP — production pattern |
| trigger_rule on audit | ✅ Pass | none_failed_min_one_success correct |
| Branch naming | ⚠️ Minor | "your-topic" placeholder — fix from today |

### Pre-Start
```bash
# WSL2
source ~/.bashrc
airflow dags list   # both DAGs should still be visible

# Windows Git Bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-04/day-23-xcom-variables
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-06: Workflow Orchestration                                |
| Story           | ST-23: XCom + Variables + Connection Management              |
| Task ID         | TASK-023                                                     |
| Sprint          | Sprint 04 (Days 21–28)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | airflow, xcom, variables, connections, day-23                |
| Acceptance Criteria | XCom passing pipeline metadata between 3 tasks; config stored in Airflow Variables not hardcoded; audit DAG reads from Variables |

---

## 📚 BACKGROUND

### XCom — Cross-Communication Between Tasks

```
XCom = a key-value store in the Airflow metadata DB.
Tasks push values → other tasks pull them.

Rules you learned on Day 22 (reinforced today):
  ✅ Only push primitives: int, str, float, bool, list[primitive], dict[primitive]
  ❌ Never push: DataFrame, SQLAlchemy objects, custom classes

XCom storage limit: ~48KB per value
For larger data: write to DB/file, push the path or row count via XCom
```

### Airflow Variables — Config Without Hardcoding

```python
# WITHOUT Variables — hardcoded, brittle:
TARGET_TABLE = "analytics_customer_airflow"
MAX_RETRIES = 3

# WITH Variables — stored in Airflow metadata DB, changeable in UI:
from airflow.models import Variable
TARGET_TABLE = Variable.get("customer_etl_target_table",
                            default_var="analytics_customer_airflow")
MAX_RETRIES  = int(Variable.get("etl_max_retries", default_var="3"))
```

Variables can be:
- Set via CLI: `airflow variables set key value`
- Set via UI: Admin → Variables
- Set via JSON: `airflow variables import vars.json`
- Retrieved with default: `Variable.get("key", default_var="fallback")`

### Connection Management

```
Airflow Connections store:
  - DB credentials (host, port, user, password, schema)
  - API keys
  - SSH credentials

You already have: dvdrental_appuser, fs_default

Today you add:
  - airflow_meta_conn    → the airflow metadata DB itself
  - analytics_conn       → a second schema for analytics output
```

---

## 🎯 OBJECTIVES

1. Store pipeline config in Airflow Variables — no more hardcoded values
2. Build `dag_audit_report.py` — reads from Variables, pulls XCom from other DAGs
3. Use `Variable.get()` with JSON for structured config
4. Add `airflow_meta_conn` connection
5. Write pipeline run history to a report using XCom + Variables
6. Push clean `[DAY-023][S04]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 20 min   | Set up Airflow Variables + update existing DAGs    |
| B     | 40 min   | `dag_audit_report.py` — reads XCom + Variables     |
| C     | 20 min   | Add connections, test Variable retrieval           |
| D     | 20 min   | Trigger audit DAG, verify report written           |
| E     | 20 min   | Git push + merge                                   |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Set Airflow Variables (Block A)
**[Full steps]**

```bash
# In WSL2 — set pipeline config as Variables
airflow variables set customer_etl_target_table "analytics_customer_airflow"
airflow variables set film_etl_target_table "analytics_film_airflow"
airflow variables set etl_max_retries "3"
airflow variables set etl_min_row_count "500"
airflow variables set windows_db_host "172.18.144.1"
airflow variables set project_root "/mnt/c/90_day_python_de_plan"

# Set a JSON variable — structured config for the audit DAG
airflow variables set etl_pipeline_config '{
    "customer": {
        "source": "customer",
        "target": "analytics_customer_airflow",
        "min_rows": 500
    },
    "film": {
        "source": "film",
        "target": "analytics_film_airflow",
        "min_rows": 900
    }
}'

# Verify
airflow variables list
airflow variables get etl_pipeline_config
```

**Update `dag_customer_etl.py` to use Variables:**
```python
# Add at top of file, after imports:
from airflow.models import Variable

# Then replace hardcoded values:
# BEFORE:
TARGET_TABLE = "analytics_customer_airflow"

# AFTER:
TARGET_TABLE = Variable.get(
    "customer_etl_target_table",
    default_var="analytics_customer_airflow"
)
```

---

### EXERCISE 2 — dag_audit_report.py (Block B)
**[Q1 fully provided. Q2 write yourself — hints given]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/dag_audit_report.py`:

```python
"""
dag_audit_report.py — Day 23 | Pipeline Audit Report DAG
=========================================================
Runs after customer and film ETL DAGs complete.
Reads pipeline config from Variables.
Queries etl_audit_log table.
Writes summary report to CSV.
"""
from __future__ import annotations
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

# Dynamic Windows IP
import subprocess
_ip = subprocess.run(
    ["bash", "-c",
     "cat /etc/resolv.conf | grep nameserver | awk '{print $2}'"],
    capture_output=True, text=True
).stdout.strip()
os.environ["DB_HOST"] = _ip or "172.18.144.1"

PROJECT_ROOT = Path(os.environ.get(
    "project_root", "/mnt/c/90_day_python_de_plan"
))
for p in [
    PROJECT_ROOT / "sprint-01" / "day-02",
    PROJECT_ROOT / "sprint-01" / "day-04",
]:
    sys.path.insert(0, str(p))

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "python-de-journey",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# ── Q1: Read audit log — fully provided ───────────────────────────────────
def read_audit_log(**context) -> None:
    """
    Read recent pipeline runs from etl_audit_log.
    Push summary dict to XCom for downstream tasks.
    """
    import pandas as pd
    from db_utils import get_engine, dispose_engine

    engine = get_engine()
    sql = """
        SELECT
            pipeline_name,
            source_table,
            target_table,
            rows_loaded,
            status,
            elapsed_s,
            run_at
        FROM etl_audit_log
        ORDER BY run_at DESC
        LIMIT 20
    """
    df = pd.read_sql(sql, engine)
    dispose_engine()

    # Build summary — primitives only for XCom
    summary = {
        "total_runs":    int(len(df)),
        "successful":    int((df["status"] == "success").sum()),
        "failed":        int((df["status"] == "failed").sum()),
        "total_rows":    int(df["rows_loaded"].sum()),
        "pipelines":     df["pipeline_name"].unique().tolist(),
        "latest_run":    str(df["run_at"].max()) if len(df) > 0 else None,
    }

    context["ti"].xcom_push(key="audit_summary", value=summary)
    print(f"Audit log read | {summary}")


# ── Q1: Check pipeline row counts — fully provided ────────────────────────
def check_pipeline_counts(**context) -> None:
    """
    Read pipeline config from Variable (JSON).
    Verify each target table has expected row count.
    Push pass/fail per pipeline to XCom.
    """
    from db_utils import execute_scalar, close_pool

    # Read structured config from Variable
    config_json = Variable.get("etl_pipeline_config", default_var="{}")
    config = json.loads(config_json)

    results = {}
    for name, cfg in config.items():
        try:
            count = execute_scalar(
                f"SELECT COUNT(*) FROM {cfg['target']}"
            )
            passed = count >= cfg["min_rows"]
            results[name] = {
                "table":    cfg["target"],
                "count":    int(count),
                "min_rows": cfg["min_rows"],
                "passed":   passed,
            }
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{name}: {count} rows {status}")
        except Exception as exc:
            results[name] = {"error": str(exc), "passed": False}
            print(f"{name}: ERROR — {exc}")

    close_pool()
    context["ti"].xcom_push(key="count_checks", value=results)


# ── Q2: Write report — WRITE THIS YOURSELF ───────────────────────────────
def write_audit_report(**context) -> None:
    """
    Q2 — YOUR TASK:
    Pull XCom from upstream tasks and write a CSV + Markdown report.

    HINTS:
    Step 1: Pull audit_summary from read_audit_log task
        summary = context["ti"].xcom_pull(
            task_ids="read_audit_log", key="audit_summary"
        )

    Step 2: Pull count_checks from check_pipeline_counts task
        counts = context["ti"].xcom_pull(
            task_ids="check_pipeline_counts", key="count_checks"
        )

    Step 3: Get output dir from Variable
        output_dir = Path(Variable.get(
            "project_root",
            default_var="/mnt/c/90_day_python_de_plan"
        )) / "airflow" / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

    Step 4: Write Markdown report to output_dir / "audit_report.md"
        Content should include:
          - Report timestamp
          - Summary: total_runs, successful, failed, total_rows
          - Per-pipeline count check (pass/fail)

    Step 5: Write CSV of count_checks to output_dir / "count_checks.csv"
        import pandas as pd
        df = pd.DataFrame(counts).T.reset_index()
        df.columns = ["pipeline"] + list(df.columns[1:])
        df.to_csv(output_dir / "count_checks.csv", index=False)

    Step 6: Print confirmation with file paths

    Self-check:
      - audit_report.md exists in airflow/output/
      - count_checks.csv exists in airflow/output/
      - Both pipelines show ✅ PASS in the report
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement write_audit_report")


with DAG(
    dag_id="pipeline_audit_report",
    description="Daily audit report across all ETL pipelines",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["audit", "report", "sprint-04"],
) as dag:

    task_read_log = PythonOperator(
        task_id="read_audit_log",
        python_callable=read_audit_log,
    )

    task_check_counts = PythonOperator(
        task_id="check_pipeline_counts",
        python_callable=check_pipeline_counts,
    )

    task_write_report = PythonOperator(
        task_id="write_audit_report",
        python_callable=write_audit_report,
    )

    # read_log and check_counts run in parallel, then write_report
    [task_read_log, task_check_counts] >> task_write_report
```

---

### EXERCISE 3 — Add airflow_meta connection + test (Block C)

```bash
# Add connection to Airflow metadata DB itself
WINDOWS_IP=$(cat /etc/resolv.conf | grep nameserver | awk '{print $2}')

airflow connections add airflow_meta_conn \
    --conn-type postgres \
    --conn-host $WINDOWS_IP \
    --conn-port 5432 \
    --conn-schema airflow_meta \
    --conn-login airflow \
    --conn-password "Airflow@2024!"

# Verify all connections
airflow connections list | grep -E "dvdrental|airflow|fs_default"
```

---

### EXERCISE 4 — Trigger + Verify (Block D)

```bash
# Trigger audit report DAG
airflow dags unpause pipeline_audit_report
airflow dags trigger pipeline_audit_report

sleep 30
airflow dags list-runs -d pipeline_audit_report --output table | tail -3

# Verify report files exist
ls /mnt/c/90_day_python_de_plan/airflow/output/
# Should show: audit_report.md, count_checks.csv

cat /mnt/c/90_day_python_de_plan/airflow/output/audit_report.md
```

---

### EXERCISE 5 — Git Push

```bash
# Windows Git Bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 23 --sprint 4 ^
    --message "Airflow XCom+Variables: pipeline config in Variables, audit report DAG, 3 connections" ^
    --merge
```

---

## ✅ DAY 23 COMPLETION CHECKLIST

| # | Task                                                                  | Done? |
|---|-----------------------------------------------------------------------|-------|
| 1 | 6 Airflow Variables set including JSON config                         | [ ]   |
| 2 | `customer_etl_daily` reads target table from Variable                 | [ ]   |
| 3 | `dag_audit_report.py` created — 3 tasks                              | [ ]   |
| 4 | `read_audit_log` reads etl_audit_log, pushes summary to XCom         | [ ]   |
| 5 | `check_pipeline_counts` reads JSON Variable, checks both tables       | [ ]   |
| 6 | **`write_audit_report` written by you — MD + CSV files created**      | [ ]   |
| 7 | `read_audit_log` + `check_pipeline_counts` run in parallel            | [ ]   |
| 8 | `airflow_meta_conn` connection added                                  | [ ]   |
| 9 | `audit_report.md` exists with both pipelines showing PASS            | [ ]   |
|10 | `count_checks.csv` exists with correct data                          | [ ]   |
|11 | One clean `[DAY-023][S04]` commit via `daily_commit.py --merge`       | [ ]   |

---

## 🔍 SELF-CHECK — audit_report.md should contain:

```markdown
# Pipeline Audit Report
Generated: 2026-05-04 ...

## Summary
- Total runs: X
- Successful: X
- Failed: 0
- Total rows processed: X

## Pipeline Count Checks
| Pipeline | Table | Count | Min Required | Status |
|----------|-------|-------|--------------|--------|
| customer | analytics_customer_airflow | 599 | 500 | ✅ PASS |
| film     | analytics_film_airflow     | 1000| 900 | ✅ PASS |
```

---

## 🔜 PREVIEW: DAY 24

**Topic:** Airflow retry strategies + SLA + alerting  
**What you'll do:** Configure exponential backoff retries, set SLA deadlines
on tasks, add `on_failure_callback` to send alerts when DAGs fail.
Simulate a failure and verify the alert fires.

---

*Day 23 | Sprint 04 | EP-06 | TASK-023*
