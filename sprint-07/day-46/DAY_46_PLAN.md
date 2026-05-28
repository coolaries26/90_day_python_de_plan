# 📅 DAY 46 — Sprint 07 | Capstone Airflow Orchestration
## Full Pipeline DAG: ETL → ML → Predictions, Fix Data Leakage

---

## 🔁 RETROSPECTIVE — Day 45

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Churn definition fixed | ✅ Pass | 2,886 retained vs 93,332 churned |
| churn_model.py | ⚠️ Fix | CV F1=1.0 = data leakage — total_orders in features |
| delay_model.py | ✅ Pass | CV F1=0.15 expected with weak features |
| ml.churn_predictions 96k | ✅ Pass | |
| ml.delay_predictions 96k | ✅ Pass | |
| delay_pipeline 407MB | ⚠️ Fix | Reduce n_estimators + SMOTE sampling_strategy |

### Two Fixes Before Starting

**Fix 1 — Remove total_orders from churn features:**
```python
# capstone/ml/churn_model.py — load_features()
# DELETE this line:
# features["total_orders"] = df["total_orders"].fillna(0)

# Retrain and verify CV F1 < 1.0:
python capstone/ml/churn_model.py 2>&1 | grep "CV F1"
# Expected: CV F1: ~0.75-0.90 (not 1.0)
```

**Fix 2 — Reduce delay model size:**
```python
# capstone/ml/delay_model.py — ImbPipeline
# Change SMOTE:
SMOTE(sampling_strategy=0.3, random_state=42, k_neighbors=5)
# Change RF:
RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42, n_jobs=-1)

# Retrain and verify:
python capstone/ml/delay_model.py
ls -lh capstone/ml/models/delay_pipeline_latest.pkl
# Should be < 10MB
```

```bash
git checkout develop
git pull origin develop
git checkout -b sprint-07/day-46-airflow
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-10: Capstone & Job Readiness                              |
| Story           | ST-46: Airflow Orchestration for Capstone                    |
| Task ID         | TASK-046                                                     |
| Sprint          | Sprint 07 (Days 43–48)                                       |
| Story Points    | 3                                                            |
| Priority        | CRITICAL                                                     |
| Labels          | airflow, dag, orchestration, capstone, day-46                |
| Acceptance Criteria | dag_ecommerce_etl.py runs all pipeline stages; all 4 tasks green; uses WSL2 Airflow instance |

---

## 🎯 OBJECTIVES

1. Fix churn leakage + delay model size
2. Create `dag_ecommerce_etl.py` — orchestrates full pipeline
3. Add ecommerce DB connection to Airflow
4. Trigger DAG and verify all stages complete
5. Push clean `[DAY-046][S07]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 20 min | Fix models + branch |
| B | 50 min | `dag_ecommerce_etl.py` |
| C | 20 min | Add Airflow connection + trigger |
| D | 10 min | Verify all tasks green |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — dag_ecommerce_etl.py (Block B)
**[Tasks 1-2 provided. Tasks 3-4 write yourself]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/dag_ecommerce_etl.py`:

```python
"""
dag_ecommerce_etl.py — Day 46 | Capstone Pipeline DAG
=======================================================
Orchestrates the full e-commerce analytics pipeline:
  1. Load raw data (if new files detected)
  2. Run analytics ETL (raw → 5 analytics tables)
  3. Train churn model → write predictions
  4. Train delay model → write predictions

Schedule: @weekly (data is static — weekly refresh is appropriate)
Manual trigger: airflow dags trigger dag_ecommerce_etl
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
# Ecommerce DB uses same host, different DB name
os.environ["ECOMMERCE_DB_HOST"]     = _ip or "172.18.144.1"
os.environ["ECOMMERCE_DB_PORT"]     = "5432"
os.environ["ECOMMERCE_DB_NAME"]     = "ecommerce_db"
os.environ["ECOMMERCE_DB_USER"]     = "appuser"
os.environ["ECOMMERCE_DB_PASSWORD"] = "AppUser@2024!"

PROJECT_ROOT = Path("/mnt/c/90_day_python_de_plan")
for p in [
    PROJECT_ROOT / "sprint-01" / "day-04",
    PROJECT_ROOT / "capstone",
    PROJECT_ROOT / "capstone" / "etl",
    PROJECT_ROOT / "capstone" / "ml",
    PROJECT_ROOT / "sprint-03" / "day-16",
]:
    sys.path.insert(0, str(p))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_callbacks import on_failure

default_args = {
    "owner":               "python-de-journey",
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": on_failure,
}


# ── Task 1: Run analytics ETL — provided ──────────────────────────────────
def run_analytics_etl(**context) -> dict:
    """
    Run all 5 analytics transforms: raw → analytics schema.
    """
    from analytics_etl import main as etl_main, run_etl, get_ecommerce_engine, dispose_ecommerce_engine, \
        CUSTOMER_LTV_SQL, ORDER_METRICS_SQL, SELLER_PERFORMANCE_SQL, \
        PRODUCT_ANALYTICS_SQL, MONTHLY_REVENUE_SQL

    engine = get_ecommerce_engine()
    results = {}
    tasks = [
        ("customer_ltv",        CUSTOMER_LTV_SQL),
        ("order_metrics",       ORDER_METRICS_SQL),
        ("seller_performance",  SELLER_PERFORMANCE_SQL),
        ("product_analytics",   PRODUCT_ANALYTICS_SQL),
        ("monthly_revenue",     MONTHLY_REVENUE_SQL),
    ]
    for table_name, sql in tasks:
        try:
            count = run_etl(sql, table_name, engine)
            results[table_name] = count
        except Exception as exc:
            print(f"❌ {table_name}: {exc}")
            results[table_name] = 0

    dispose_ecommerce_engine()
    context["ti"].xcom_push(key="etl_results", value=results)
    print(f"ETL complete: {sum(results.values()):,} total rows across {len(results)} tables")
    return results


# ── Task 2: Train churn model — provided ──────────────────────────────────
def run_churn_model(**context) -> dict:
    """Train churn pipeline and write predictions to ml.churn_predictions."""
    from churn_model import load_features, train_churn_pipeline, save_pipeline, write_churn_predictions

    X, y = load_features()
    pipeline = train_churn_pipeline(X, y)
    save_pipeline(pipeline, X, "churn_pipeline")
    write_churn_predictions(pipeline, X)

    result = {
        "n_samples":   int(len(X)),
        "churn_rate":  float(y.mean()),
        "status":      "success",
    }
    context["ti"].xcom_push(key="churn_result", value=result)
    return result


# ── Task 3: Train delay model — WRITE THIS YOURSELF ───────────────────────
def run_delay_model(**context) -> dict:
    """
    YOUR TASK: Train delay pipeline and write predictions.

    HINTS:
    from delay_model import load_features, train_delay_pipeline, save_pipeline, write_delay_predictions

    X, y = load_features()
    pipeline = train_delay_pipeline(X, y)
    save_pipeline(pipeline, X, "delay_pipeline")
    write_delay_predictions(pipeline, X)

    result = {
        "n_samples": int(len(X)),
        "late_rate": float(y.mean()),
        "status":    "success",
    }
    context["ti"].xcom_push(key="delay_result", value=result)
    return result
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement run_delay_model")


# ── Task 4: Log pipeline run — WRITE THIS YOURSELF ────────────────────────
def log_pipeline_run(**context) -> None:
    """
    YOUR TASK: Pull XCom results and log a summary audit entry.

    HINTS:
    etl     = context["ti"].xcom_pull(task_ids="run_analytics_etl", key="etl_results") or {}
    churn   = context["ti"].xcom_pull(task_ids="run_churn_model",   key="churn_result") or {}
    delay   = context["ti"].xcom_pull(task_ids="run_delay_model",   key="delay_result") or {}

    total_rows = sum(etl.values())

    from sqlalchemy.orm import Session
    from db_utils import get_engine, dispose_engine
    from models_compat import AuditLog

    engine = get_engine()
    audit = AuditLog(
        pipeline_name="dag_ecommerce_etl",
        source_table="ecommerce_db.raw",
        target_table="ecommerce_db.analytics + ecommerce_db.ml",
        rows_loaded=total_rows,
        status="success",
    )
    with Session(engine) as session:
        session.add(audit)
        session.commit()
    dispose_engine()

    print(f"Pipeline summary:")
    print(f"  ETL rows:    {total_rows:,}")
    print(f"  Churn rate:  {churn.get('churn_rate', 0):.1%}")
    print(f"  Late rate:   {delay.get('late_rate', 0):.1%}")
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement log_pipeline_run")


with DAG(
    dag_id="dag_ecommerce_etl",
    description="Capstone: full e-commerce analytics pipeline",
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["capstone", "ecommerce", "etl", "ml", "sprint-07"],
) as dag:

    task_etl = PythonOperator(
        task_id="run_analytics_etl",
        python_callable=run_analytics_etl,
        pool="db_connection_pool",
    )

    task_churn = PythonOperator(
        task_id="run_churn_model",
        python_callable=run_churn_model,
        pool="db_connection_pool",
    )

    task_delay = PythonOperator(
        task_id="run_delay_model",
        python_callable=run_delay_model,
        pool="db_connection_pool",
    )

    task_log = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=log_pipeline_run,
        trigger_rule="all_done",
    )

    # ETL first, then churn + delay in parallel, then log
    task_etl >> [task_churn, task_delay] >> task_log
```

---

### EXERCISE 2 — Add Airflow Connection for ecommerce_db (Block C)

```bash
# WSL2
source ~/.bashrc
WINDOWS_IP=$(ip route | grep default | awk '{print $3}')

airflow connections add ecommerce_db \
    --conn-type postgres \
    --conn-host $WINDOWS_IP \
    --conn-port 5432 \
    --conn-schema ecommerce_db \
    --conn-login appuser \
    --conn-password "AppUser@2024!"

# Verify
airflow connections get ecommerce_db

# Install imbalanced-learn in WSL2 airflow-venv (needed for ImbPipeline)
source ~/airflow-venv/bin/activate
pip install imbalanced-learn==0.11.0

# Verify DAG loads
airflow dags list | grep ecommerce
```

---

### EXERCISE 3 — Trigger + Verify (Block C + D)

```bash
# Trigger the full pipeline
airflow dags unpause dag_ecommerce_etl
airflow dags trigger dag_ecommerce_etl

# Watch progress (runs 10-15 minutes due to model training)
watch -n 30 "airflow dags list-runs -d dag_ecommerce_etl --output table 2>/dev/null | tail -3"

# After completion — check all 4 tasks green
airflow tasks states-for-dag-run dag_ecommerce_etl \
    $(airflow dags list-runs -d dag_ecommerce_etl --output json 2>/dev/null | \
      python3 -c "import json,sys; runs=json.load(sys.stdin); print(runs[0]['run_id'])")

# Verify tables updated
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
for t in ['customer_ltv','order_metrics','churn_predictions','delay_predictions']:
    schema = 'analytics' if t in ['customer_ltv','order_metrics'] else 'ml'
    count = pd.read_sql(f'SELECT COUNT(*) FROM {schema}.{t}', engine).iloc[0,0]
    print(f'{schema}.{t}: {count:,}')
dispose_ecommerce_engine()
"
```

---

### EXERCISE 4 — Git Push

```bash
cd C:\90_day_python_de_plan
python scripts/daily_commit.py --day 46 --sprint 7 ^
    --message "Capstone Airflow: dag_ecommerce_etl, all 4 tasks green, churn leakage fixed, delay model <10MB" ^
    --merge
```

---

## ✅ DAY 46 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | total_orders removed from churn features — CV F1 < 1.0 | [ ] |
| 2 | delay_pipeline.pkl < 10MB after reducing n_estimators | [ ] |
| 3 | `dag_ecommerce_etl.py` created in airflow/dags/ | [ ] |
| 4 | DAG visible in `airflow dags list` | [ ] |
| 5 | **`run_delay_model` Task 3 written** | [ ] |
| 6 | **`log_pipeline_run` Task 4 written** | [ ] |
| 7 | All 4 tasks green on trigger | [ ] |
| 8 | All 4 tables updated after run | [ ] |
| 9 | One clean `[DAY-046][S07]` commit | [ ] |

---

## ⚠️ WATCH OUT FOR

**imbalanced-learn in WSL2:**
```bash
source ~/airflow-venv/bin/activate
pip install imbalanced-learn scikit-learn matplotlib pandas
```

**Large model file in git:**
```bash
# delay_pipeline_latest.pkl should be < 10MB after fix
# Add to .gitignore if still large:
echo "capstone/ml/models/*.pkl" >> .gitignore
# Keep metadata.json files only (not the binary models)
echo "!capstone/ml/models/*_metadata.json" >> .gitignore
```

**`from analytics_etl import ...` in DAG:**
The import works because `PROJECT_ROOT / "capstone" / "etl"` is in sys.path.
Test the import before triggering:
```bash
python3 -c "
import sys
sys.path.insert(0, '/mnt/c/90_day_python_de_plan/capstone/etl')
from analytics_etl import run_etl
print('Import OK')
"
```

---

## 🔜 PREVIEW: DAY 47

**Topic:** Streamlit capstone dashboard  
**What you'll do:** Build a 5-page dashboard using the e-commerce analytics tables.
Pages: Overview (KPIs), Orders (delivery trends), Customers (LTV + churn risk),
Sellers (performance), ML Insights (churn + delay predictions).
This is your portfolio demo piece.

---

*Day 46 | Sprint 07 | EP-10 | TASK-046*
