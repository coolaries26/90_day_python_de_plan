# 📅 DAY 39 — Sprint 06 | Airflow ML Retraining DAG
## Auto-Retrain Model After ETL + Version Model Artifacts

---

## 🔁 RETROSPECTIVE — Day 38

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| SMOTE applied | ✅ Pass | Before/After logged |
| run_comparison() 4 rows | ✅ Pass | LR/RF × no-SMOTE/SMOTE |
| churn_recall improvement | ✅ Expected | RF 0.0→0.33 with SMOTE |
| ImbPipeline trained + saved | ✅ Pass | |
| Load + predict verified | ✅ Pass | 599 predictions |
| LR+SMOTE churn_recall=0 | ✅ Understood | Only 3 test samples — unreliable |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-06/day-39-ml-dag
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-09: ML Foundations & Scikit-learn                         |
| Story           | ST-39: Airflow ML Retraining DAG                             |
| Task ID         | TASK-039                                                     |
| Sprint          | Sprint 06 (Days 36–42)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | airflow, ml, retrain, dag, dataset-trigger, day-39           |
| Acceptance Criteria | dag_ml_retrain.py triggers after ETL; pipeline retrained; new version saved with timestamp; predictions written to DB |

---

## 📚 BACKGROUND

### Why Auto-Retraining Matters

```
Without auto-retrain:
  Model trained once → data changes → model becomes stale
  Predictions reflect old patterns → wrong business decisions

With auto-retrain:
  ETL runs (new data) → model retrains → fresh predictions
  Always based on current customer behaviour

Model drift:
  When real-world patterns change and the model doesn't adapt.
  Detection: compare current accuracy vs baseline accuracy.
  If accuracy drops > 5% → alert + force retrain.
```

### Versioning ML Artifacts

```
Bad practice (overwrites previous model):
  joblib.dump(model, "churn_model.pkl")   ← can't roll back

Good practice (versioned with timestamp):
  churn_pipeline_20260518_103045.pkl   ← specific version
  churn_pipeline_latest.pkl           ← always points to current

In production:
  Use MLflow or DVC for artifact tracking
  For this program: simple filesystem versioning is sufficient
```

---

## 🎯 OBJECTIVES

1. Create `dag_ml_retrain.py` — triggered after analytics data updates
2. Tasks: extract features → train pipeline → evaluate → save versioned artifact
3. Write predictions to `ml_predictions` PostgreSQL table
4. Add `ML_MODEL_DATASET` outlet so Streamlit knows model is fresh
5. Verify retrain runs end-to-end after ETL trigger
6. Push clean `[DAY-039][S06]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 10 min   | Branch setup                                       |
| B     | 50 min   | `dag_ml_retrain.py` — full DAG                    |
| C     | 20 min   | Create `ml_predictions` table + verify             |
| D     | 20 min   | Trigger + verify end-to-end                        |
| E     | 20 min   | Git push                                           |

---

## 📝 EXERCISES

---

### EXERCISE 1 — dag_ml_retrain.py (Block B)
**[Task 1 + 2 fully provided. Task 3 + 4 write yourself]**

Create `/mnt/c/90_day_python_de_plan/airflow/dags/dag_ml_retrain.py`:

```python
"""
dag_ml_retrain.py — Day 39 | ML Retraining DAG
================================================
Triggers after customer analytics dataset updates.
Retrains churn model, saves versioned artifact,
writes predictions to PostgreSQL.
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
for p in [
    PROJECT_ROOT / "sprint-01" / "day-02",
    PROJECT_ROOT / "sprint-01" / "day-04",
    PROJECT_ROOT / "sprint-06" / "day-36",
    PROJECT_ROOT / "sprint-06" / "day-38",
    PROJECT_ROOT / "sprint-03" / "day-16",
]:
    sys.path.insert(0, str(p))

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow_callbacks import on_failure

CUSTOMER_DATASET  = Dataset("postgresql://dvdrental/analytics_customer_airflow")
ML_MODEL_DATASET  = Dataset("file:///mnt/c/90_day_python_de_plan/sprint-06/day-39/models/latest")

PIPELINE_DIR = PROJECT_ROOT / "sprint-06" / "day-39" / "models"
PIPELINE_DIR.mkdir(parents=True, exist_ok=True)

default_args = {
    "owner":               "python-de-journey",
    "retries":             1,
    "retry_delay":         timedelta(minutes=2),
    "on_failure_callback": on_failure,
}


# ── Task 1: Extract features — provided ───────────────────────────────────
def extract_features(**context) -> dict:
    """Extract features from analytics tables and push stats to XCom."""
    from feature_engineering import main as get_features

    X, y = get_features()

    stats = {
        "n_samples":      int(len(X)),
        "n_features":     int(len(X.columns)),
        "n_active":       int(y.sum()),
        "n_churned":      int((y == 0).sum()),
        "churn_rate":     float((y == 0).mean()),
        "feature_names":  list(X.columns),
    }
    context["ti"].xcom_push(key="feature_stats", value=stats)
    print(f"Features extracted: {stats['n_samples']} samples, "
          f"{stats['n_features']} features, "
          f"churn rate: {stats['churn_rate']:.1%}")
    return stats


# ── Task 2: Train pipeline — provided ─────────────────────────────────────
def train_pipeline(**context) -> dict:
    """Retrain ImbPipeline (SMOTE + Scaler + RF) and save versioned artifact."""
    import joblib
    from feature_engineering import main as get_features
    from ml_pipeline import build_and_train_pipeline

    X, y = get_features()
    pipeline = build_and_train_pipeline(X, y)

    # Versioned save
    timestamp     = datetime.now().strftime("%Y%m%d_%H%M%S")
    versioned     = PIPELINE_DIR / f"churn_pipeline_{timestamp}.pkl"
    latest        = PIPELINE_DIR / "churn_pipeline_latest.pkl"

    joblib.dump(pipeline, versioned)
    joblib.dump(pipeline, latest)

    # Metadata
    meta = {
        "trained_at":    datetime.now().isoformat(),
        "versioned_file":versioned.name,
        "latest_file":   latest.name,
        "n_samples":     int(len(X)),
        "features":      list(X.columns),
    }
    meta_path = PIPELINE_DIR / "model_metadata.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    context["ti"].xcom_push(key="model_path",  value=str(latest))
    context["ti"].xcom_push(key="model_meta",  value=meta)
    print(f"Model saved → {versioned.name}")
    return meta


# ── Task 3: Write predictions to DB — WRITE THIS YOURSELF ─────────────────
def write_predictions_to_db(**context) -> None:
    """
    Q3 — YOUR TASK:
    Load latest pipeline, generate predictions, write to ml_predictions table.

    HINTS:
    Step 1: Pull model path from XCom
        model_path = context["ti"].xcom_pull(
            task_ids="train_pipeline", key="model_path"
        )

    Step 2: Load pipeline + features
        import joblib
        pipeline = joblib.load(model_path)
        from feature_engineering import main as get_features
        X, y = get_features()

    Step 3: Generate predictions
        predictions   = pipeline.predict(X)
        probabilities = pipeline.predict_proba(X)[:, 1]  # prob of active

    Step 4: Build DataFrame
        import pandas as pd
        df = pd.DataFrame({
            "prediction_date":  datetime.now().strftime("%Y-%m-%d"),
            "predicted_active": predictions.tolist(),
            "churn_probability": (1 - probabilities).round(4).tolist(),
            "actual_active":    y.values.tolist(),
        })

    Step 5: Write to PostgreSQL using SQLAlchemy
        from db_utils import get_engine, dispose_engine
        engine = get_engine()
        df.to_sql("ml_predictions", engine,
                  if_exists="replace", index=True,  # index = row number
                  index_label="customer_idx",
                  method="multi")
        dispose_engine()

    Step 6: Push row count to XCom and print confirmation
        context["ti"].xcom_push(key="predictions_written", value=len(df))
        print(f"Predictions written: {len(df)} rows to ml_predictions")
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement write_predictions_to_db")


# ── Task 4: Log model metrics to audit table — WRITE THIS YOURSELF ─────────
def log_model_metrics(**context) -> None:
    """
    Q4 — YOUR TASK:
    Pull XCom results and write a summary to etl_audit_log.

    HINTS:
    Step 1: Pull from XCom
        feature_stats = context["ti"].xcom_pull(
            task_ids="extract_features", key="feature_stats"
        )
        model_meta = context["ti"].xcom_pull(
            task_ids="train_pipeline", key="model_meta"
        )
        n_predictions = context["ti"].xcom_pull(
            task_ids="write_predictions_to_db", key="predictions_written"
        ) or 0

    Step 2: Write to etl_audit_log using models_compat.AuditLog
        from sqlalchemy.orm import Session
        from db_utils import get_engine, dispose_engine
        from models_compat import AuditLog

        engine = get_engine()
        audit = AuditLog(
            pipeline_name="dag_ml_retrain",
            source_table="analytics_customer_airflow",
            target_table="ml_predictions",
            rows_loaded=n_predictions,
            status="success",
            elapsed_s=None,
        )
        with Session(engine) as session:
            session.add(audit)
            session.commit()
        dispose_engine()
        print("Model metrics logged to etl_audit_log")
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement log_model_metrics")


with DAG(
    dag_id="dag_ml_retrain",
    description="Auto-retrain churn model after customer ETL updates",
    default_args=default_args,
    schedule=[CUSTOMER_DATASET],     # trigger after customer ETL
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "retrain", "churn", "sprint-06"],
) as dag:

    task_features = PythonOperator(
        task_id="extract_features",
        python_callable=extract_features,
        pool="db_connection_pool",
    )

    task_train = PythonOperator(
        task_id="train_pipeline",
        python_callable=train_pipeline,
    )

    task_predict = PythonOperator(
        task_id="write_predictions_to_db",
        python_callable=write_predictions_to_db,
        outlets=[ML_MODEL_DATASET],   # signals model is fresh
        pool="db_connection_pool",
    )

    task_log = PythonOperator(
        task_id="log_model_metrics",
        python_callable=log_model_metrics,
        pool="db_connection_pool",
        trigger_rule="all_done",
    )

    task_features >> task_train >> task_predict >> task_log
```

---

### EXERCISE 2 — Trigger + Verify (Block C + D)

```bash
# WSL2 — verify DAG loads
airflow dags list | grep ml_retrain

# Trigger manually first to test
airflow dags unpause dag_ml_retrain
airflow dags trigger dag_ml_retrain

sleep 90
airflow dags list-runs -d dag_ml_retrain --output table | tail -3

# Verify ml_predictions table created
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_scalar, execute_query, close_pool
count = execute_scalar('SELECT COUNT(*) FROM ml_predictions')
print(f'ml_predictions rows: {count}')
sample = execute_query(
    'SELECT * FROM ml_predictions ORDER BY customer_idx LIMIT 3',
    as_dict=True
)
for r in sample: print(r)
close_pool()
"
```

---

### EXERCISE 3 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 39 --sprint 6 ^
    --message "Airflow ML DAG: auto-retrain, versioned model, predictions to DB, audit log" ^
    --merge
```

---

## ✅ DAY 39 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `dag_ml_retrain.py` created in airflow/dags/                            | [ ]   |
| 2 | DAG visible in `airflow dags list`                                       | [ ]   |
| 3 | `extract_features` runs — feature stats pushed to XCom                  | [ ]   |
| 4 | `train_pipeline` trains — versioned pkl file created with timestamp     | [ ]   |
| 5 | **`write_predictions_to_db` written — ml_predictions table populated**  | [ ]   |
| 6 | **`log_model_metrics` written — audit log entry written**               | [ ]   |
| 7 | All 4 tasks green on first trigger                                       | [ ]   |
| 8 | `ml_predictions` table has 599 rows in PostgreSQL                        | [ ]   |
| 9 | `ML_MODEL_DATASET` outlet on predict task                                | [ ]   |
|10 | One clean `[DAY-039][S06]` commit via `daily_commit.py --merge`          | [ ]   |

---

## ⚠️ WATCH OUT FOR

**imbalanced-learn in WSL2 airflow-venv:**
```bash
source ~/airflow-venv/bin/activate
pip install imbalanced-learn==0.11.0
```

**`ml_pipeline.py` path in DAG:**
The DAG imports `from ml_pipeline import build_and_train_pipeline`.
Make sure `PROJECT_ROOT / "sprint-06" / "day-38"` is in `sys.path`.

**`k_neighbors` parameter in SMOTE:**
```python
k_neighbors=min(5, y_train.value_counts().min() - 1)
```
With only 15 churned customers, `k_neighbors` must be < 15.
If the training fold has fewer churned samples, reduce k_neighbors further.

---

## 🔜 PREVIEW: DAY 40

**Topic:** Model evaluation + drift detection  
**What you'll do:** Write a model evaluation module that compares current
model accuracy vs baseline. If accuracy drops > 5%, log a WARNING to
etl_audit_log and the Streamlit dashboard shows a drift alert.

---

*Day 39 | Sprint 06 | EP-09 | TASK-039*
