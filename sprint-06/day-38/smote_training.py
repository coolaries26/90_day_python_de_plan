#!/usr/bin/env python3
"""
smote_training.py — Day 38 | SMOTE Oversampling
=================================================
Applies SMOTE to balance the churn dataset.
Retrains models and compares with/without SMOTE.

Run: python smote_training.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import joblib
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, f1_score, precision_score,
    recall_score, classification_report
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day-36"))

from logger import get_pipeline_logger
from feature_engineering import main as get_features

logger = get_pipeline_logger("smote_training")

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ── Apply SMOTE — fully provided ──────────────────────────────────────────
def apply_smote(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    sampling_strategy: float = 1.0,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Apply SMOTE to training data.
    sampling_strategy=1.0 means minority = majority (fully balanced)
    sampling_strategy=0.5 means minority = 50% of majority

    Returns numpy arrays (SMOTE requires/returns numpy).
    """
    logger.info(f"\n── SMOTE Oversampling ────────────────────────")
    logger.info(f"Before SMOTE: {dict(y_train.value_counts().sort_index())}")

    smote = SMOTE(
        sampling_strategy=sampling_strategy,
        random_state=42,
        k_neighbors=min(5, y_train.value_counts().min() - 1),
        # k_neighbors must be < n_minority_samples
    )
    X_resampled, y_resampled = smote.fit_resample(X_train, y_train)

    unique, counts = np.unique(y_resampled, return_counts=True)
    logger.info(f"After SMOTE:  {dict(zip(unique.tolist(), counts.tolist()))}")
    logger.info(f"New training size: {len(X_resampled)} samples")

    return X_resampled, y_resampled


# ── Train one model — helper ──────────────────────────────────────────────
def train_evaluate(
    model,
    X_train, y_train,
    X_test, y_test,
    scale: bool = False,
    label: str = "",
) -> dict:
    """Train model and return metrics dict."""
    if scale:
        scaler = StandardScaler()
        X_tr = scaler.fit_transform(X_train)
        X_te = scaler.transform(X_test)
    else:
        X_tr, X_te = X_train, X_test

    model.fit(X_tr, y_train)
    y_pred = model.predict(X_te)

    metrics = {
        "label":     label,
        "accuracy":  round(accuracy_score(y_test, y_pred), 4),
        "f1":        round(f1_score(y_test, y_pred, average="weighted"), 4),
        "precision": round(precision_score(y_test, y_pred, average="weighted",
                                           zero_division=0), 4),
        "recall":    round(recall_score(y_test, y_pred, average="weighted"), 4),
        "churn_recall": round(recall_score(y_test, y_pred, pos_label=0), 4),
    }
    return metrics


# ── Q2: Before/After comparison — WRITE THIS YOURSELF ────────────────────
def run_comparison(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    """
    Q2 — YOUR TASK:
    Train 4 model variants and compare:
      1. LogisticRegression WITHOUT SMOTE
      2. LogisticRegression WITH SMOTE
      3. RandomForest WITHOUT SMOTE
      4. RandomForest WITH SMOTE

    HINTS:
    Step 1: Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

    Step 2: Apply SMOTE to training data only
        X_smote, y_smote = apply_smote(X_train, y_train)

    Step 3: Train all 4 variants using train_evaluate():
        results = []

        # LR without SMOTE (scale=True for LogisticRegression)
        results.append(train_evaluate(
            LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42),
            X_train, y_train, X_test, y_test, scale=True,
            label="LR (no SMOTE)"
        ))

        # LR with SMOTE
        results.append(train_evaluate(
            LogisticRegression(max_iter=1000, random_state=42),
            X_smote, y_smote, X_test, y_test, scale=True,
            label="LR + SMOTE"
        ))

        # RF without SMOTE (scale=False — RF doesn't need scaling)
        results.append(train_evaluate(
            RandomForestClassifier(n_estimators=100, class_weight="balanced", random_state=42),
            X_train, y_train, X_test, y_test, scale=False,
            label="RF (no SMOTE)"
        ))

        # RF with SMOTE
        results.append(train_evaluate(
            RandomForestClassifier(n_estimators=100, random_state=42),
            X_smote, y_smote, X_test, y_test, scale=False,
            label="RF + SMOTE"
        ))

    Step 4: Build and print DataFrame
        df = pd.DataFrame(results)
        logger.info(f"\n── SMOTE Comparison ──────────────────────────")
        logger.info(f"\n{df.to_string(index=False)}")

    Step 5: Save to output/smote_comparison.csv
        df.to_csv(OUTPUT_DIR / "smote_comparison.csv", index=False)

    Step 6: Return df

    KEY METRIC to watch: churn_recall
      - Without SMOTE: churn_recall likely very low (misses churned customers)
      - With SMOTE:    churn_recall should improve significantly
    """
    # YOUR CODE HERE
# Step 1: Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Step 2: Apply SMOTE to training data only
    X_smote, y_smote = apply_smote(X_train, y_train)

    # Step 3: Train all 4 variants using train_evaluate():
    results = []

    # LR without SMOTE (scale=True for LogisticRegression)
    results.append(train_evaluate(
        LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42),
        X_train, y_train, X_test, y_test, scale=True,
        label="LR (no SMOTE)"
    ))

    # LR with SMOTE
    results.append(train_evaluate(
        LogisticRegression(max_iter=1000, random_state=42),
        X_smote, y_smote, X_test, y_test, scale=True,
        label="LR + SMOTE"
    ))

    # RF without SMOTE (scale=False — RF doesn't need scaling)
    results.append(train_evaluate(
        RandomForestClassifier(n_estimators=100, class_weight="balanced", random_state=42),
        X_train, y_train, X_test, y_test, scale=False,
        label="RF (no SMOTE)"
    ))

    # RF with SMOTE
    results.append(train_evaluate(
        RandomForestClassifier(n_estimators=100, random_state=42),
        X_smote, y_smote, X_test, y_test, scale=False,
        label="RF + SMOTE"
    ))

    # Step 4: Build and print DataFrame
    df = pd.DataFrame(results)
    logger.info(f"\n── SMOTE Comparison ──────────────────────────")
    logger.info(f"\n{df.to_string(index=False)}")

    # Step 5: Save to output/smote_comparison.csv
    df.to_csv(OUTPUT_DIR / "smote_comparison.csv", index=False)

    # Step 6: Return df
    return df


def main() -> None:
    logger.info("=" * 52)
    logger.info("SMOTE Training — Day 38")
    logger.info("=" * 52)

    X, y = get_features()
    comparison = run_comparison(X, y)

    logger.info("\n✅ SMOTE comparison complete.")
    logger.info(f"Best churn_recall: {comparison['churn_recall'].max():.4f}")


if __name__ == "__main__":
    main()
