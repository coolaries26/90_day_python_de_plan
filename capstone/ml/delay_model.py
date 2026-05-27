"""
delay_model.py — Day 45 | Delivery Delay Prediction
=====================================================
YOUR TASK: Predict whether an order will be delivered late.
Target: is_late (from analytics.order_metrics)
Source: analytics.order_metrics

Features to use:
  - delivery_days_estimated
  - payment_value
  - product_count
  - review_score (use 3.0 as default for missing)

HINTS — follow churn_model.py pattern exactly:

Step 1: load_features()
  SELECT delivery_days_estimated, payment_value,
         product_count, review_score, is_late
  FROM analytics.order_metrics
  WHERE is_late IS NOT NULL

Step 2: engineer features
  features["delivery_days_estimated"] = df["delivery_days_estimated"].fillna(df["delivery_days_estimated"].median())
  features["payment_value"]           = df["payment_value"].fillna(0).astype(float)
  features["product_count"]           = df["product_count"].fillna(1)
  features["log_payment_value"]       = np.log1p(features["payment_value"])
  features["is_expensive"]            = (features["payment_value"] > features["payment_value"].median()).astype(int)

  target = df["is_late"].astype(int)
  logger.info(f"Late rate: {target.mean():.1%}")

Step 3: train ImbPipeline (SMOTE + Scaler + RF) — same as churn_model.py

Step 4: save_pipeline(pipeline, X, "delay_pipeline")

Step 5: write_delay_predictions() — write to ml.delay_predictions
  same pattern as write_churn_predictions

Step 6: main() — runs all steps

Self-check:
  ml.delay_predictions should have ~96,000 rows
  Late rate: ~8% (7,837 / 96,588 from Day 44 data quality check)
"""
from __future__ import annotations

import sys
import json
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import joblib
from imblearn.pipeline import Pipeline as ImbPipeline
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report, f1_score

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from logger import get_pipeline_logger
from db import get_ecommerce_engine, dispose_ecommerce_engine

logger = get_pipeline_logger("delay_model")

MODEL_DIR = Path(__file__).parent / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

def load_features() -> tuple[pd.DataFrame, pd.Series]:
    """Load and engineer features from analytics.order_metrics."""
    engine = get_ecommerce_engine()
    df = pd.read_sql("""
        SELECT
            delivery_days_estimated,
            payment_value,
            product_count,
            review_score,
            is_late
        FROM analytics.order_metrics
        WHERE is_late IS NOT NULL
    """, engine)
#    logger.info(f"Loaded {len(df):,} rows from analytics.order_metrics")
    dispose_ecommerce_engine()

    # Feature engineering
    features = pd.DataFrame()
    features["delivery_days_estimated"] = df["delivery_days_estimated"].fillna(df["delivery_days_estimated"].median())
    features["payment_value"]           = df["payment_value"].fillna(0).astype(float)
    features["product_count"]           = df["product_count"].fillna(1)
    features["log_payment_value"]       = np.log1p(features["payment_value"])
    features["is_expensive"]            = (features["payment_value"] > features["payment_value"].median()).astype(int)

    target = df["is_late"].astype(int)

    logger.info(f"Loaded {len(features):,} customers")
    logger.info(f"Late rate: {target.mean():.1%} "
                f"({target.sum():,} late, {(~target.astype(bool)).sum():,} on time)")

    return features, target

def train_model(X: pd.DataFrame, y: pd.Series) -> ImbPipeline:
    """Train a Random Forest model with SMOTE and StandardScaler."""
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    logger.info(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
    logger.info(f"Train late rate: {y_train.mean():.1%}")

    minority_count = y_train.value_counts().min()
    k_neighbors = min(5, minority_count - 1)

    pipeline = ImbPipeline([
        ("smote", SMOTE(random_state=42, k_neighbors=k_neighbors)),
        ("scaler", StandardScaler()),
        ("rf", RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42))
    ])

    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    logger.info(f"\n{classification_report(y_test, y_pred, target_names=['On Time','Late'])}")  

    # Optional: cross-validation scores
    logger.info("Cross-validation scores:")
    cv_scores = cross_val_score(
        ImbPipeline([
            ("smote", SMOTE(random_state=42, k_neighbors=k_neighbors)),
            ("scaler", StandardScaler()),
            ("rf", RandomForestClassifier(n_estimators=100, random_state=42))
        ]), X_train, y_train, cv=5, scoring="f1"
    )
    logger.info(f"CV F1 scores: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

    return pipeline

def save_pipeline(pipeline: ImbPipeline, X: pd.DataFrame, name: str) -> None:
    """Save the trained pipeline and feature columns."""
    path = MODEL_DIR / f"{name}_latest.pkl"
    joblib.dump(pipeline, path)
    meta = {
        "model_name": name,
        "trained_at": datetime.now().isoformat(),
        "n_features": len(X.columns),
        "features": list(X.columns),
        "n_samples": len(X)
        # Add other metadata as needed
    }
    with open(MODEL_DIR / f"{name}_metadata.json", "w") as f:
        json.dump(meta, f, indent=2)
    logger.info(f"Saved pipeline  → {path.name}")

    return path

def write_delay_predictions(pipeline: ImbPipeline, X: pd.DataFrame) -> None:
    """Write predictions to ml.delay_predictions."""
#    logger.info("Generating predictions for all data...")
    engine = get_ecommerce_engine()
    predictions = pipeline.predict(X)
    probabilities = pipeline.predict_proba(X)[:, 1]  # Probability of being late
    df_predictions = pd.DataFrame({
        "order_id": X.index,
        "prediction_late": predictions,
        "late_probability": probabilities.round(4),
        "prediction_date": datetime.now().strftime("%Y-%m-%d")
    })
    df_predictions.to_sql("delay_predictions", engine, schema="ml", if_exists="replace", index=False)
    logger.info(f"Wrote {len(df_predictions):,} predictions → ml.delay_predictions")
    dispose_ecommerce_engine()

def main() -> None:
    logger.info("=" * 52)
    logger.info("Delay Model — Day 45 Capstone")
    logger.info("=" * 52)

    X, y = load_features()
    pipeline = train_model(X, y)
    save_pipeline(pipeline, X, "delay_pipeline")
    write_delay_predictions(pipeline, X)
    logger.info("✅ Delay prediction pipeline completed successfully.")

if __name__ == "__main__":
    main()