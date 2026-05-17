#!/usr/bin/env python3
"""
train_model.py — Day 36 | Logistic Regression Churn Model
===========================================================
Trains a customer churn classifier on DVD Rental data.
Evaluates with accuracy, classification report, confusion matrix.
Saves model + scaler with joblib.

Run: python train_model.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04")) # for logger
sys.path.insert(0, str(Path(__file__).resolve().parent))    # for feature_engineering (same directory)

from logger import get_pipeline_logger
from feature_engineering import main as get_features

logger = get_pipeline_logger("train_model")

MODEL_DIR = Path(__file__).parent / "models"    # directory for saving trained model and scaler
MODEL_DIR.mkdir(exist_ok=True)


# ── Q1: Train + Evaluate — fully provided ────────────────────────────────
def train_and_evaluate(X: pd.DataFrame, y: pd.Series) -> tuple:
    """
    Train LogisticRegression on churn features.
    Returns (model, scaler, X_test, y_test, y_pred)
    """
    logger.info("\n── Train/Test Split ─────────────────────────")

    # Train/test split — stratify preserves class ratio in both sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y,         # preserve churn ratio
    )
    logger.info(f"Train: {len(X_train)} samples | Test: {len(X_test)} samples")
    logger.info(f"Train churn rate: {(y_train == 0).mean():.1%}")
    logger.info(f"Test  churn rate: {(y_test  == 0).mean():.1%}")

    # Scale features — LogisticRegression sensitive to scale
    scaler  = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)  # fit + transform train
    X_test_scaled  = scaler.transform(X_test)       # transform only (no refit)

    # Train model
    logger.info("\n── Training LogisticRegression ──────────────")
    model = LogisticRegression(
        max_iter=1000,
        random_state=42,
        class_weight="balanced",   # handles imbalanced classes
    )
    model.fit(X_train_scaled, y_train)

    # Predict
    y_pred = model.predict(X_test_scaled)

    # Evaluate
    accuracy = accuracy_score(y_test, y_pred)
    logger.info(f"\n── Evaluation ───────────────────────────────")
    logger.info(f"Accuracy: {accuracy:.4f} ({accuracy:.1%})")

    report = classification_report(y_test, y_pred,
                                   target_names=["Churned", "Active"])
    logger.info(f"\nClassification Report:\n{report}")

    cm = confusion_matrix(y_test, y_pred)
    logger.info(f"\nConfusion Matrix:\n{cm}")
    logger.info(f"\n  True Negatives (correct churn):  {cm[0,0]}")
    logger.info(f"\n  False Positives (wrong active):  {cm[0,1]}")
    logger.info(f"\n  False Negatives (missed churn):  {cm[1,0]}")
    logger.info(f"\n  True Positives (correct active): {cm[1,1]}")

    # Cross-validation — more reliable than single split
    logger.info("\n── Cross-Validation (5-fold) ────────────────")
    X_scaled_all = scaler.fit_transform(X)   # scale full dataset for CV
    cv_scores = cross_val_score(
        LogisticRegression(max_iter=1000, random_state=42, class_weight="balanced"),
        X_scaled_all, y, cv=5, scoring="accuracy"
    )
    logger.info(f"CV Accuracy: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
    logger.info(f"CV Scores:   {cv_scores.round(4)}")

    # Feature importance (coefficients)
    logger.info("\n── Feature Importance (Coefficients) ────────")
    coefs = pd.Series(
        model.coef_[0], index=X.columns
    ).sort_values(key=abs, ascending=False)
    for feature, coef in coefs.items():
        direction = "→ active" if coef > 0 else "→ churn"
        logger.info(f"  {feature:<30} {coef:+.4f}  {direction}")

    return model, scaler, X_test, y_test, y_pred


# ── Q2: Save model — WRITE THIS YOURSELF ─────────────────────────────────
def save_model(model, scaler, feature_names: list[str]) -> None:
    """
    Q2 — YOUR TASK:
    Save trained model, scaler, and feature names using joblib.

    HINTS:
    import joblib

    Step 1: Save model
        model_path = MODEL_DIR / "churn_model.pkl"
        joblib.dump(model, model_path)
        logger.info(f"Model saved → {model_path.name}")

    Step 2: Save scaler (MUST save alongside model — same preprocessing needed at inference)
        scaler_path = MODEL_DIR / "churn_scaler.pkl"
        joblib.dump(scaler, scaler_path)
        logger.info(f"Scaler saved → {scaler_path.name}")

    Step 3: Save metadata as JSON
        import json
        meta = {
            "model_type":     type(model).__name__,
            "feature_names":  feature_names,
            "n_features":     len(feature_names),
            "trained_at":     datetime.now().isoformat(),
            "model_file":     "churn_model.pkl",
            "scaler_file":    "churn_scaler.pkl",
        }
        meta_path = MODEL_DIR / "model_metadata.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        logger.info(f"Metadata saved → {meta_path.name}")

    Self-check:
        ls sprint-06/day-36/models/
        # churn_model.pkl
        # churn_scaler.pkl
        # model_metadata.json
    """
    # YOUR CODE HERE
    import joblib
    import json
    from datetime import datetime

    # Step 1: Save model
    model_path = MODEL_DIR / "churn_model.pkl"
    joblib.dump(model, model_path)
    logger.info(f"Model saved → {model_path.name}")

    # Step 2: Save scaler
    scaler_path = MODEL_DIR / "churn_scaler.pkl"
    joblib.dump(scaler, scaler_path)
    logger.info(f"Scaler saved → {scaler_path.name}")

    # Step 3: Save metadata
    meta = {
        "model_type":     type(model).__name__,
        "feature_names":  feature_names,
        "n_features":     len(feature_names),
        "trained_at":     datetime.now().isoformat(),
        "model_file":     "churn_model.pkl",
        "scaler_file":    "churn_scaler.pkl",
    }
    meta_path = MODEL_DIR / "model_metadata.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    logger.info(f"Metadata saved → {meta_path.name}")

def main() -> None:
    logger.info("=" * 52)
    logger.info("Model Training — Day 36")
    logger.info("=" * 52)

    # Get features
    X, y = get_features()

    # Train and evaluate
    model, scaler, X_test, y_test, y_pred = train_and_evaluate(X, y)

    # Save
    save_model(model, scaler, list(X.columns))

    logger.info("\n✅ Training complete. Model saved in models/")


if __name__ == "__main__":
    main()