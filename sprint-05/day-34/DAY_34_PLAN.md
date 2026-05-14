# 📅 DAY 34 — Sprint 05 | Airflow Chart Generation DAG
## Auto-Generate Charts After ETL + Dataset Trigger Integration

---

## 🔁 RETROSPECTIVE — Day 33

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Download buttons (4 total) | ✅ Pass | Overview has 2 — good initiative |
| Title search on films | ✅ Pass | str.contains case-insensitive |
| Raw data expander | ✅ Pass | |
| README 1.8KB | ✅ Pass | Real content |
| run_dashboard.bat | ✅ Pass | 478 bytes — correct |
| App serving HTTP 200 | ✅ Pass | Headless smoke test passed |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-05/day-34-chart-dag
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-07: Data Visualization & Reporting                        |
| Story           | ST-34: Airflow Chart Generation DAG                          |
| Task ID         | TASK-034                                                     |
| Sprint          | Sprint 05 (Days 29–35)                                       |
| Story Points    | 2                                                            |
| Priority        | MEDIUM                                                       |
| Labels          | airflow, charts, dag, dataset-trigger, day-34                |
| Acceptance Criteria | dag_chart_generator.py triggers after ETL datasets update; charts regenerated in output/; audit log entry written |

---

## 📚 BACKGROUND

### The Problem Without This DAG

```
Current flow:
  customer_etl_daily runs → updates analytics_customer_airflow
  film_etl_daily runs     → updates analytics_film_airflow
  Dashboard shows new data ✅

BUT: charts.py and plotly_charts.py still show OLD data
  → Static PNGs in sprint-05/day-29/output/ are stale
  → Embedded chart images in audit_report.md are stale
  → Reports sent by email would show yesterday's charts

With chart generation DAG:
  ETL runs → datasets updated → chart DAG triggers →
  fresh PNGs + HTMLs generated → reports always current
```

### Architecture

```
customer_etl_daily  ──┐
                       ├──[Dataset trigger]──► dag_chart_generator
film_etl_daily     ──┘                              │
                                              generate_static_charts (charts.py)
                                              generate_interactive_charts (plotly_charts.py)
                                              write_chart_manifest
                                              notify_dashboard_refresh
```

---

## 🎯 OBJECTIVES

1. Create `dag_chart_generator.py` triggered by customer + film datasets
2. Wrap `charts.py` and `plotly_charts.py` functions as Airflow tasks
3. Write chart manifest JSON (list of generated files + timestamps)
4. Add `CHART_DATASET` so downstream DAGs know charts are fresh
5. Verify charts regenerate after ETL trigger
6. Push clean `[DAY-034][S05]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 15 min   | Branch setup + path planning                       |
| B     | 45 min   | `dag_chart_generator.py` — full DAG                |
| C     | 20 min   | Trigger ETL → verify charts regenerate             |
| D     | 20 min   | Check manifest + audit log entry                   |
| E     | 20 min   | Git push                                           |

---

## 📝 EXERCISES

---

### EXERCISE 1 — dag_chart_generator.py (Block B)
**[Q1 structure + static charts fully provided. Q2 interactive charts — write yourself]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/dag_chart_generator.py`:

```python
"""
dag_chart_generator.py — Day 34 | Chart Generation DAG
=======================================================
Triggered automatically when customer + film ETL datasets update.
Regenerates all static (PNG) and interactive (HTML) charts.
Writes a manifest of generated files.

Trigger: Dataset events from customer_etl_daily + film_etl_daily
"""
from __future__ import annotations
import os
import sys
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# Dynamic Windows IP
_ip = subprocess.run(
    ["bash", "-c", "ip route | grep default | awk '{print $3}'"],
    capture_output=True, text=True,
).stdout.strip()
os.environ["DB_HOST"] = _ip or "172.18.144.1"

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")

# Add all required paths
for p in [
    PROJECT_ROOT / "sprint-01" / "day-02",
    PROJECT_ROOT / "sprint-01" / "day-04",
    PROJECT_ROOT / "sprint-05" / "day-29",
    PROJECT_ROOT / "sprint-05" / "day-30",
    PROJECT_ROOT / "sprint-03" / "day-16",
]:
    sys.path.insert(0, str(p))

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow_callbacks import on_failure

# ── Datasets ──────────────────────────────────────────────────────────────
CUSTOMER_DATASET = Dataset("postgresql://dvdrental/analytics_customer_airflow")
FILM_DATASET     = Dataset("postgresql://dvdrental/analytics_film_airflow")
CHART_DATASET    = Dataset("file:///mnt/c/90_day_python_de_plan/airflow/output/charts")

# Output directory for Airflow-managed charts
CHART_OUTPUT = PROJECT_ROOT / "airflow" / "output" / "charts"
CHART_OUTPUT.mkdir(parents=True, exist_ok=True)

default_args = {
    "owner":               "python-de-journey",
    "retries":             1,
    "retry_delay":         timedelta(minutes=2),
    "on_failure_callback": on_failure,
}


# ── Q1: Generate static charts — fully provided ───────────────────────────
def generate_static_charts(**context) -> dict:
    """
    Run all 5 matplotlib/seaborn charts from charts.py.
    Saves PNGs to airflow/output/charts/static/
    """
    import matplotlib
    matplotlib.use("Agg")

    # Import chart functions
    sys.path.insert(0, str(PROJECT_ROOT / "sprint-05" / "day-29"))

    # Override OUTPUT_DIR in charts module
    import charts as charts_module
    static_dir = CHART_OUTPUT / "static"
    static_dir.mkdir(exist_ok=True)
    charts_module.OUTPUT_DIR = static_dir

    generated = []
    chart_functions = [
        charts_module.chart_customer_segments,
        charts_module.chart_monthly_revenue,
        charts_module.chart_film_value_tiers,
        charts_module.chart_pipeline_history,
        charts_module.chart_customer_spend_distribution,
    ]

    for fn in chart_functions:
        try:
            path = fn()
            generated.append(str(path))
            print(f"✅ {fn.__name__} → {path.name}")
        except Exception as exc:
            print(f"❌ {fn.__name__} failed: {exc}")

    print(f"\nStatic charts: {len(generated)}/5 generated")
    context["ti"].xcom_push(key="static_charts", value=generated)
    return {"count": len(generated), "paths": generated}


# ── Q2: Generate interactive charts — WRITE THIS YOURSELF ─────────────────
def generate_interactive_charts(**context) -> dict:
    """
    Q2 — YOUR TASK:
    Run all 4 Plotly charts from plotly_charts.py.
    Saves HTML + PNG to airflow/output/charts/interactive/

    HINTS:
    Step 1: Import plotly_charts module
        sys.path.insert(0, str(PROJECT_ROOT / "sprint-05" / "day-30"))
        import plotly_charts as plotly_module

    Step 2: Override OUTPUT_DIR in the module
        interactive_dir = CHART_OUTPUT / "interactive"
        interactive_dir.mkdir(exist_ok=True)
        plotly_module.OUTPUT_DIR = interactive_dir

    Step 3: Run each chart function
        chart_functions = [
            plotly_module.p1_customer_segments,
            plotly_module.p2_monthly_revenue,
            plotly_module.p3_spend_vs_rentals,
            plotly_module.p4_category_treemap,
        ]
        generated = []
        for fn in chart_functions:
            try:
                fig = fn()
                generated.append(fn.__name__)
                print(f"✅ {fn.__name__}")
            except Exception as exc:
                print(f"❌ {fn.__name__}: {exc}")

    Step 4: Push to XCom
        context["ti"].xcom_push(key="interactive_charts", value=generated)

    Step 5: Return dict {"count": len(generated), "charts": generated}

    Self-check: 4 HTML files appear in airflow/output/charts/interactive/
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement generate_interactive_charts")


# ── Write manifest ────────────────────────────────────────────────────────
def write_chart_manifest(**context) -> None:
    """
    Write a JSON manifest of all generated charts.
    Used by downstream DAGs and the Streamlit app to know which charts exist.
    """
    static    = context["ti"].xcom_pull(
        task_ids="generate_static_charts",    key="static_charts"
    ) or []
    interactive = context["ti"].xcom_pull(
        task_ids="generate_interactive_charts", key="interactive_charts"
    ) or []

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "static_charts":      static,
        "interactive_charts": interactive,
        "total_charts":       len(static) + len(interactive),
        "chart_output_dir":   str(CHART_OUTPUT),
    }

    manifest_path = CHART_OUTPUT / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"Manifest written → {manifest_path}")
    print(f"Total charts: {manifest['total_charts']}")

    # Write audit log entry
    try:
        from sqlalchemy.orm import Session
        from db_utils import get_engine, dispose_engine
        from models_compat import AuditLog

        engine = get_engine()
        audit = AuditLog(
            pipeline_name="dag_chart_generator",
            source_table="analytics_customer_airflow,analytics_film_airflow",
            target_table="charts_output",
            rows_loaded=manifest["total_charts"],
            status="success",
        )
        with Session(engine) as session:
            session.add(audit)
            session.commit()
        dispose_engine()
        print("Audit log entry written")
    except Exception as exc:
        print(f"Audit log failed (non-fatal): {exc}")


# ── DAG ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_chart_generator",
    description="Auto-generate charts when ETL datasets update",
    default_args=default_args,
    schedule=[CUSTOMER_DATASET, FILM_DATASET],   # dataset trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["charts", "visualization", "sprint-05"],
) as dag:

    task_static = PythonOperator(
        task_id="generate_static_charts",
        python_callable=generate_static_charts,
        pool="db_connection_pool",
    )

    task_interactive = PythonOperator(
        task_id="generate_interactive_charts",
        python_callable=generate_interactive_charts,
        pool="db_connection_pool",
    )

    task_manifest = PythonOperator(
        task_id="write_chart_manifest",
        python_callable=write_chart_manifest,
        outlets=[CHART_DATASET],         # signals charts are fresh
        trigger_rule="all_done",
    )

    # Static and interactive charts run in parallel → manifest after both
    [task_static, task_interactive] >> task_manifest
```

---

### EXERCISE 2 — Trigger + Verify (Block C + D)

```bash
# WSL2 — verify DAG loads
airflow dags list | grep chart_generator

# Trigger by running ETL DAGs (which will trigger chart DAG via datasets)
airflow dags trigger customer_etl_daily
sleep 30
airflow dags trigger film_etl_daily
sleep 60

# Check chart generator triggered
airflow dags list-runs -d dag_chart_generator --output table | tail -3

# Verify chart files generated
ls /mnt/c/90_day_python_de_plan/airflow/output/charts/static/
ls /mnt/c/90_day_python_de_plan/airflow/output/charts/interactive/
cat /mnt/c/90_day_python_de_plan/airflow/output/charts/manifest.json
```

**Expected manifest:**
```json
{
  "generated_at": "2026-05-14T...",
  "static_charts": [
    "/mnt/c/.../charts/static/c1_customer_segments.png",
    ...
  ],
  "interactive_charts": ["p1_customer_segments", "p2_monthly_revenue", ...],
  "total_charts": 9
}
```

---

### EXERCISE 3 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 34 --sprint 5 ^
    --message "Airflow chart DAG: dataset trigger, static+interactive charts, manifest JSON, CHART_DATASET outlet" ^
    --merge
```

---

## ✅ DAY 34 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `dag_chart_generator.py` created in airflow/dags/                       | [ ]   |
| 2 | DAG visible in `airflow dags list`                                       | [ ]   |
| 3 | Triggered by `[CUSTOMER_DATASET, FILM_DATASET]` schedule                 | [ ]   |
| 4 | `generate_static_charts` runs — 5 PNGs in charts/static/                | [ ]   |
| 5 | **`generate_interactive_charts` written by you — 4 HTMLs generated**    | [ ]   |
| 6 | `write_chart_manifest` creates manifest.json                             | [ ]   |
| 7 | `CHART_DATASET` outlet declared on manifest task                         | [ ]   |
| 8 | Chart DAG triggers after ETL DAGs complete                               | [ ]   |
| 9 | `manifest.json` shows `total_charts: 9`                                  | [ ]   |
|10 | Audit log entry written for chart generation run                         | [ ]   |
|11 | One clean `[DAY-034][S05]` commit via `daily_commit.py --merge`          | [ ]   |

---

## ⚠️ WATCH OUT FOR

**Overriding OUTPUT_DIR in the chart modules:**
The chart modules use `OUTPUT_DIR = Path(__file__).parent / "output"` as a module-level
variable. When you do `charts_module.OUTPUT_DIR = new_path`, you're changing it for
all subsequent calls in that Python process. This works but is fragile.

A more robust pattern:
```python
# Pass output_dir as parameter to each chart function
# If chart functions don't accept output_dir, patch it at module level
# before calling — reset after:
original_dir = charts_module.OUTPUT_DIR
charts_module.OUTPUT_DIR = static_dir
try:
    path = fn()
finally:
    charts_module.OUTPUT_DIR = original_dir  # restore
```

**matplotlib Agg backend in Airflow workers:**
Always set `matplotlib.use("Agg")` BEFORE importing pyplot in any Airflow task.
Airflow workers have no display, so the interactive backends will fail silently.

---

## 🔜 PREVIEW: DAY 35 — Sprint 05 Test

**Sprint 05 final test** — 4 tasks covering visualization, Streamlit, and chart automation.
Close sprint with `sprint-05-complete` tag.
Sprint 06 begins Day 36: ML Foundations with scikit-learn.

---

*Day 34 | Sprint 05 | EP-07 | TASK-034*
