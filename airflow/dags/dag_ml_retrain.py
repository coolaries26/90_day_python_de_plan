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
    PROJECT_ROOT / "sprint-06" / "day-40",
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

logger = None  # global logger will be set in on_failure callback

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
    #Step 1: Pull model path from XCom
    model_path = context["ti"].xcom_pull(
        task_ids="train_pipeline", key="model_path"
    )
    #Step 2: Load pipeline + features
    import joblib
    pipeline = joblib.load(model_path)
    from feature_engineering import main as get_features
    X, y = get_features()
    #Step 3: Generate predictions
    predictions   = pipeline.predict(X)
    probabilities = pipeline.predict_proba(X)[:, 1]  # prob of active
    #Step 4: Build DataFrame
    import pandas as pd
    df = pd.DataFrame({
        "prediction_date":  datetime.now().strftime("%Y-%m-%d"),
        "predicted_active": predictions.tolist(),
        "churn_probability": (1 - probabilities).round(4).tolist(),
        "actual_active":    y.values.tolist(),
    })
    #Step 5: Write to PostgreSQL using SQLAlchemy
    from db_utils import get_engine, dispose_engine
    engine = get_engine()
    df.to_sql("ml_predictions", engine,
              if_exists="replace", index=True,  # index = row number
              index_label="customer_idx",
              method="multi")
    dispose_engine()

    #Step 6: Push row count to XCom and print confirmation
    context["ti"].xcom_push(key="predictions_written", value=len(df))
    print(f"Predictions written: {len(df)} rows to ml_predictions")


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
    #Step 1: Pull from XCom
    feature_stats = context["ti"].xcom_pull(
        task_ids="extract_features", key="feature_stats"
    )
    model_meta = context["ti"].xcom_pull(
        task_ids="train_pipeline", key="model_meta"
    )
    n_predictions = context["ti"].xcom_pull(
        task_ids="write_predictions_to_db", key="predictions_written"
    ) or 0
    #Step 2: Write to etl_audit_log using models_compat.AuditLog
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

# Task 5 function — WRITE THIS YOURSELF:
def evaluate_and_detect_drift(**context) -> None:
    """
    Evaluate current model accuracy and check for drift.

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

    Step 3: Evaluate on 20% test set (same split as training)
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score
        _, X_test, _, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        y_pred = pipeline.predict(X_test)
        current_accuracy = accuracy_score(y_test, y_pred)

    Step 4: Load baseline + detect drift
        sys.path.insert(0, str(PROJECT_ROOT / "sprint-06" / "day-40"))
        from drift_detector import detect_drift, load_baseline_accuracy, save_baseline_accuracy

        meta_path = PIPELINE_DIR / "model_metadata.json"
        baseline  = load_baseline_accuracy(meta_path)
        result    = detect_drift(current_accuracy, baseline)

    Step 5: If this is the first run, save current as baseline
        if baseline == 0.90:   # default value = first run
            save_baseline_accuracy(meta_path, current_accuracy)

    Step 6: Write to ml_drift_log if drift or warning
        if result.drift_detected or result.warning_only:
            from sqlalchemy.orm import Session
            from db_utils import get_engine, dispose_engine
            engine = get_engine()
            # INSERT into ml_drift_log (create table if needed)
            with engine.connect() as conn:
                conn.execute(text(""
                    INSERT INTO ml_drift_log
                        (checked_at, baseline_accuracy, current_accuracy,
                         delta, status, message)
                    VALUES (:checked_at, :baseline, :current, :delta, :status, :message)
                ""), {
                    "checked_at": result.checked_at,
                    "baseline":   result.baseline_accuracy,
                    "current":    result.current_accuracy,
                    "delta":      result.delta,
                    "status":     result.status,
                    "message":    result.message,
                })
                conn.commit()
            dispose_engine()

    Step 7: Push result to XCom
        context["ti"].xcom_push(key="drift_result", value=result.as_dict)
        print(f"Drift check: {result.status} — {result.message}")
    """
    # YOUR CODE HERE
    #Step 1: Pull model path from XCom
    model_path = context["ti"].xcom_pull(
        task_ids="train_pipeline", key="model_path"
    )
    #Step 2: Load pipeline + features
    import joblib
    pipeline = joblib.load(model_path)
    from feature_engineering import main as get_features
    X, y = get_features()
    #Step 3: Evaluate on 20% test set (same split as training)
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score
    _, X_test, _, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    y_pred = pipeline.predict(X_test)
    current_accuracy = accuracy_score(y_test, y_pred)
    #Step 4: Load baseline + detect drift
    sys.path.insert(0, str(PROJECT_ROOT / "sprint-06" / "day-40"))
    from drift_detector import detect_drift, load_baseline_accuracy, save_baseline_accuracy
    meta_path = PIPELINE_DIR / "model_metadata.json"
    baseline  = load_baseline_accuracy(meta_path)
    result    = detect_drift(current_accuracy, baseline)
    #Step 5: If this is the first run, save current as baseline
    if baseline == 0.90:   # default value = first run
        save_baseline_accuracy(meta_path, current_accuracy)
    #Step 6: Write to ml_drift_log if drift or warning
    if result.drift_detected or result.warning_only:
        from sqlalchemy.orm import Session
        from db_utils import get_engine, dispose_engine
        from sqlalchemy import text
        engine = get_engine()
        # INSERT into ml_drift_log (create table if needed)
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ml_drift_log
                    (checked_at, baseline_accuracy, current_accuracy,
                     delta, status, message)
                VALUES (:checked_at, :baseline, :current, :delta, :status, :message)
            """), {
                "checked_at": result.checked_at,
                "baseline":   result.baseline_accuracy,
                "current":    result.current_accuracy,
                "delta":      result.delta,
                "status":     result.status,
                "message":    result.message,
            })
            conn.commit()
        dispose_engine()
    #Step 7: Push result to XCom
    context["ti"].xcom_push(key="drift_result", value=result.as_dict())
    print(f"Drift check: {result.status} — {result.message}")



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
    task_drift = PythonOperator(
        task_id="evaluate_and_detect_drift",
        python_callable=evaluate_and_detect_drift,
        pool="db_connection_pool",
        trigger_rule="all_done",
    )   

    task_features >> task_train >> task_predict >> task_log >> task_drift