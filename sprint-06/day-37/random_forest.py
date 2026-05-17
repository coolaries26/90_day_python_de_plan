#!/usr/bin/env python3
"""
random_forest.py — Day 37 | RandomForest + GridSearchCV
=========================================================
Trains RandomForestClassifier with hyperparameter tuning.
Compares with LogisticRegression from Day 36.

Run: python random_forest.py
"""

from __future__ import annotations

import sys
import json
import time
from pathlib import Path
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import (
    train_test_split, GridSearchCV, cross_val_score
)
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, f1_score, precision_score,
    recall_score, roc_auc_score, classification_report
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day-36"))

from logger import get_pipeline_logger
from feature_engineering import main as get_features

logger = get_pipeline_logger("random_forest")

MODEL_DIR  = Path(__file__).parent / "models"
OUTPUT_DIR = Path(__file__).parent / "output"
MODEL_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


# ── Q1: RandomForest + GridSearchCV — fully provided ─────────────────────
def train_random_forest(X_train, y_train, X_test, y_test) -> tuple:
    """
    Train RandomForestClassifier with GridSearchCV.
    Returns (best_model, best_params, train_time_s)
    """
    logger.info("\n── RandomForest + GridSearchCV ───────────────")

    # Smaller grid for speed — expands on Day 38
    param_grid = {
        "n_estimators":      [50, 100],
        "max_depth":         [3, 5, None],
        "min_samples_split": [2, 5],
        "min_samples_leaf":  [1, 2],
    }
    logger.info(f"Grid size: {2*3*2*2} combinations × 5 folds = {2*3*2*2*5} fits")

    gs = GridSearchCV(
        RandomForestClassifier(
            random_state=42,
            class_weight="balanced",
        ),
        param_grid,
        cv=5,
        scoring="f1",      # F1 better than accuracy for imbalanced
        n_jobs=-1,
        verbose=0,
    )

    start = time.perf_counter()
    gs.fit(X_train, y_train)
    elapsed = time.perf_counter() - start

    logger.info(f"GridSearch complete in {elapsed:.1f}s")
    logger.info(f"Best params: {gs.best_params_}")
    logger.info(f"Best CV F1:  {gs.best_score_:.4f}")

    best_model = gs.best_estimator_
    y_pred = best_model.predict(X_test)

    logger.info(f"\nTest Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    logger.info(f"Test F1:       {f1_score(y_test, y_pred, average='weighted'):.4f}")
    logger.info(f"\n{classification_report(y_test, y_pred, target_names=['Churned','Active'])}")

    # Feature importance
    importances = pd.Series(
        best_model.feature_importances_,
        index=X_train.columns
        if hasattr(X_train, "columns") else range(X_train.shape[1])
    ).sort_values(ascending=False)

    logger.info("\n── Feature Importance (RandomForest) ────────")
    for feat, imp in importances.items():
        bar = "█" * int(imp * 50)
        logger.info(f"  {feat:<30} {imp:.4f} {bar}")

    # Save model
    model_path = MODEL_DIR / "churn_rf_model.pkl"
    joblib.dump(best_model, model_path)
    logger.info(f"\nRF model saved → {model_path.name}")

    return best_model, gs.best_params_, elapsed


# ── Q2: Model comparison table — WRITE THIS YOURSELF ─────────────────────
def compare_models(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    """
    Q2 — YOUR TASK:
    Compare LogisticRegression (Day 36) and RandomForest (today).
    Returns a DataFrame with metrics for both models.

    HINTS:
    Step 1: Train/test split (same as Day 36 — same random_state=42)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

    Step 2: Load Day 36 LogisticRegression model
        lr_model  = joblib.load(Path(__file__).parent.parent / "day-36" / "models" / "churn_model.pkl")
        lr_scaler = joblib.load(Path(__file__).parent.parent / "day-36" / "models" / "churn_scaler.pkl")
        X_test_scaled = lr_scaler.transform(X_test)
        lr_pred = lr_model.predict(X_test_scaled)

    Step 3: Train RandomForest (reuse train_random_forest result or retrain)
        rf_model, _, _ = train_random_forest(X_train, y_train, X_test, y_test)
        rf_pred = rf_model.predict(X_test)

    Step 4: Build comparison DataFrame
        results = []
        for name, pred in [("LogisticRegression", lr_pred), ("RandomForest", rf_pred)]:
            results.append({
                "Model":     name,
                "Accuracy":  round(accuracy_score(y_test, pred), 4),
                "F1":        round(f1_score(y_test, pred, average="weighted"), 4),
                "Precision": round(precision_score(y_test, pred, average="weighted", zero_division=0), 4),
                "Recall":    round(recall_score(y_test, pred, average="weighted"), 4),
            })
        df = pd.DataFrame(results)

    Step 5: Save to output/model_comparison.csv and print
        df.to_csv(OUTPUT_DIR / "model_comparison.csv", index=False)
        logger.info(f"\n── Model Comparison ──────────────────────")
        logger.info(f"\n{df.to_string(index=False)}")

    Step 6: Return df

    Self-check: DataFrame has 2 rows (one per model) and 5 columns
    """
    # YOUR CODE HERE
    import joblib
    from sklearn.metrics import (accuracy_score, f1_score, precision_score, recall_score)
    from sklearn.model_selection import train_test_split
    from pathlib import Path
    logger.info("\n── Model Comparison ──────────────────────")
    # Step 1: Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    # Step 2: Load LogisticRegression model
    lr_model  = joblib.load(Path(__file__).parent.parent / "day-36" / "models" / "churn_model.pkl")
    lr_scaler = joblib.load(Path(__file__).parent.parent / "day-36" / "models" / "churn_scaler.pkl")
    X_test_scaled = lr_scaler.transform(X_test)
    lr_pred = lr_model.predict(X_test_scaled)
    # Step 3: Train RandomForest
    rf_model, _, _ = train_random_forest(X_train, y_train, X_test, y_test)
    rf_pred = rf_model.predict(X_test)
    # Step 4: Build comparison DataFrame
    results = []
    for name, pred in [("LogisticRegression", lr_pred), ("RandomForest", rf_pred)]:
        results.append({
            "Model":     name,
            "Accuracy":  round(accuracy_score(y_test, pred), 4),
            "F1":        round(f1_score(y_test, pred, average="weighted"), 4),
            "Precision": round(precision_score(y_test, pred, average="weighted", zero_division=0), 4),
            "Recall":    round(recall_score(y_test, pred, average="weighted"), 4),
        })
    df = pd.DataFrame(results) 
    # Step 5: Save and print
    df.to_csv(OUTPUT_DIR / "model_comparison.csv", index=False)
    logger.info(f"\n── Model Comparison ──────────────────────")
    logger.info(f"\n{df.to_string(index=False)}")   
    # Step 6: Return DataFrame
    return df



def main() -> None:
    logger.info("=" * 52)
    logger.info("RandomForest + Model Comparison — Day 37")
    logger.info("=" * 52)

    X, y = get_features()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    rf_model, best_params, train_time = train_random_forest(
        X_train, y_train, X_test, y_test
    )

    comparison = compare_models(X, y)

    logger.info("\n✅ Done. Check output/model_comparison.csv")


if __name__ == "__main__":
    main()