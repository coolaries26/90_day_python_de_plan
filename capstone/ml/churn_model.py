#!/usr/bin/env python3
"""
churn_model.py — Day 45 | E-Commerce Churn Prediction
=======================================================
Predicts which customers are likely to never buy again.
Target: is_churned (1 = single purchase, 0 = repeat customer)
Source: analytics.customer_ltv

Run: python capstone/ml/churn_model.py
"""

from __future__ import annotations

from pyexpat import features
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

logger = get_pipeline_logger("churn_model")

MODEL_DIR = Path(__file__).parent / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)


def load_features() -> tuple[pd.DataFrame, pd.Series]:
    """Load and engineer features from analytics.customer_ltv."""
    engine = get_ecommerce_engine()
    df = pd.read_sql("""
        SELECT
            total_orders,
            total_spent,
            avg_order_value,
            days_since_last_order,
            avg_review_score,
            delivered_orders,
            cancelled_orders,
            is_churned
        FROM analytics.customer_ltv
        WHERE total_spent IS NOT NULL
    """, engine)
    dispose_ecommerce_engine()

    # Feature engineering
    features = pd.DataFrame()
#    features["total_orders"]          = df["total_orders"].fillna(0)
# "Did they review their order?" — repeat buyers tend to engage more
#    features["has_review"]            = (df["avg_review_score"].notna()).astype(int)
    features["total_spent"]           = df["total_spent"].fillna(0).astype(float)
    features["avg_order_value"]       = df["avg_order_value"].fillna(0).astype(float)
    features["days_since_last_order"] = df["days_since_last_order"].fillna(999)
    features["avg_review_score"]      = df["avg_review_score"].fillna(3.0).astype(float)
    features["cancel_rate"]           = (
        df["cancelled_orders"] / df["total_orders"].replace(0, 1)
    ).fillna(0).round(4)
    features["log_total_spent"]       = np.log1p(features["total_spent"])

    target = df["is_churned"].astype(int)

    churn_rate = target.mean()
    logger.info(f"Loaded {len(features):,} customers")
    logger.info(f"Churn rate: {churn_rate:.1%} "
                f"({target.sum():,} churned, {(~target.astype(bool)).sum():,} retained)")

    return features, target


def train_churn_pipeline(X: pd.DataFrame, y: pd.Series) -> ImbPipeline:
    """Train SMOTE → Scaler → RandomForest pipeline."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    logger.info(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
    logger.info(f"Train churn rate: {y_train.mean():.1%}")

    minority_count = y_train.value_counts().min()
    k_neighbors    = min(5, minority_count - 1)

    pipeline = ImbPipeline([
        ("smote",  SMOTE(random_state=42, k_neighbors=k_neighbors)),
        ("scaler", StandardScaler()),
        ("model",  RandomForestClassifier(
            n_estimators=100, random_state=42, n_jobs=-1
        )),
    ])

    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    logger.info(f"\n{classification_report(y_test, y_pred, target_names=['Retained','Churned'])}")

    # Cross-validation
    logger.info("Cross-validation scores:")
    cv_scores = cross_val_score(
        ImbPipeline([
            ("smote",  SMOTE(random_state=42, k_neighbors=k_neighbors)),
            ("scaler", StandardScaler()),
            ("model",  RandomForestClassifier(n_estimators=100, random_state=42)),
        ]),
        X, y, cv=5, scoring="f1_weighted"
    )
    logger.info(f"CV F1: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

    return pipeline


def save_pipeline(pipeline: ImbPipeline, X: pd.DataFrame, model_name: str) -> Path:
    """Save pipeline + metadata."""
    path    = MODEL_DIR / f"{model_name}_latest.pkl"
    joblib.dump(pipeline, path)

    meta = {
        "model_name":   model_name,
        "trained_at":   datetime.now().isoformat(),
        "features":     list(X.columns),
        "n_features":   len(X.columns),
        "n_samples":    len(X),
    }
    with open(MODEL_DIR / f"{model_name}_metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    logger.info(f"Saved → {path.name}")

    return path


# ── Write predictions to DB — WRITE THIS YOURSELF ─────────────────────────
def write_churn_predictions(pipeline: ImbPipeline, X: pd.DataFrame) -> None:
    """
    YOUR TASK: Generate predictions and write to ml.churn_predictions.

    HINTS:
    predictions   = pipeline.predict(X)
    probabilities = pipeline.predict_proba(X)[:, 1]  # prob of being churned

    df = pd.DataFrame({
        "predicted_churn":    predictions,
        "churn_probability":  probabilities.round(4),
        "prediction_date":    datetime.now().strftime("%Y-%m-%d"),
    })

    engine = get_ecommerce_engine()
    df.to_sql("churn_predictions", engine, schema="ml",
              if_exists="replace", index=True,
              index_label="customer_idx", method="multi")
    dispose_ecommerce_engine()
    logger.info(f"Predictions written: {len(df):,} rows → ml.churn_predictions")
    """
    # YOUR CODE HERE
    predictions   = pipeline.predict(X)
    probabilities = pipeline.predict_proba(X)[:, 1]
    df = pd.DataFrame({
        "predicted_churn":    predictions,
        "churn_probability":  probabilities.round(4),
        "prediction_date":    datetime.now().strftime("%Y-%m-%d"),
    })
    engine = get_ecommerce_engine()
    df.to_sql("churn_predictions", engine, schema="ml",
              if_exists="replace", index=True,
              index_label="customer_idx", method="multi")
    dispose_ecommerce_engine()
    logger.info(f"Predictions written: {len(df):,} rows → ml.churn_predictions")


def main() -> None:
    logger.info("=" * 52)
    logger.info("Churn Model — Day 45 Capstone")
    logger.info("=" * 52)

    X, y = load_features()
    pipeline = train_churn_pipeline(X, y)
    save_pipeline(pipeline, X, "churn_pipeline")
    write_churn_predictions(pipeline, X)
    logger.info("✅ Churn model complete")


if __name__ == "__main__":
    main()