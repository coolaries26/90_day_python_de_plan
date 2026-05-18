#!/usr/bin/env python3
"""
ml_pipeline.py — Day 38 | sklearn Pipeline Object
===================================================
Wraps preprocessing + SMOTE + model into a reusable pipeline.
The saved pipeline file contains EVERYTHING needed for inference.

Run: python ml_pipeline.py
"""

from __future__ import annotations

import sys
import json
from datetime import datetime
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from imblearn.pipeline import Pipeline as ImbPipeline   # supports SMOTE in pipeline
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day-36"))

from logger import get_pipeline_logger
from feature_engineering import main as get_features

logger = get_pipeline_logger("ml_pipeline")

PIPELINE_DIR = Path(__file__).parent / "pipelines"
PIPELINE_DIR.mkdir(exist_ok=True)


def build_and_train_pipeline(X: pd.DataFrame, y: pd.Series) -> ImbPipeline:
    """
    Build an imbalanced-learn Pipeline:
      step 1: SMOTE (only applied during fit, not predict)
      step 2: StandardScaler
      step 3: RandomForestClassifier

    ImbPipeline (from imblearn) supports SMOTE as a step.
    sklearn Pipeline does not (SMOTE changes y — sklearn Pipelines don't allow this).
    """
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    pipeline = ImbPipeline([
        ("smote",  SMOTE(
            random_state=42,
            k_neighbors=min(5, y_train.value_counts().min() - 1),
        )),
        ("scaler", StandardScaler()),
        ("model",  RandomForestClassifier(
            n_estimators=100,
            random_state=42,
            class_weight=None,   # SMOTE handles balance — no need for class_weight
        )),
    ])

    logger.info("Training full ML pipeline (SMOTE → Scaler → RF)...")
    pipeline.fit(X_train, y_train)

    # Evaluate
    y_pred = pipeline.predict(X_test)
    logger.info(f"\n{classification_report(y_test, y_pred, target_names=['Churned','Active'])}")

    # Cross-validation on full dataset
    # Note: cross_val_score applies pipeline.fit on each fold
    cv_scores = cross_val_score(
        ImbPipeline([
            ("smote",  SMOTE(random_state=42,
                             k_neighbors=min(5, y.value_counts().min() - 1))),
            ("scaler", StandardScaler()),
            ("model",  RandomForestClassifier(n_estimators=100, random_state=42)),
        ]),
        X, y, cv=5, scoring="f1_weighted"
    )
    logger.info(f"CV F1: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

    return pipeline


def save_pipeline(pipeline: ImbPipeline, X: pd.DataFrame) -> Path:
    """Save complete pipeline + metadata."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pipeline_path = PIPELINE_DIR / f"churn_pipeline_{timestamp}.pkl"
    latest_path   = PIPELINE_DIR / "churn_pipeline_latest.pkl"

    joblib.dump(pipeline, pipeline_path)
    joblib.dump(pipeline, latest_path)   # always-current symlink

    metadata = {
        "created_at":    datetime.now().isoformat(),
        "pipeline_file": str(latest_path.name),
        "versioned_file":str(pipeline_path.name),
        "steps":         [name for name, _ in pipeline.steps],
        "feature_names": list(X.columns),
        "n_features":    len(X.columns),
    }
    meta_path = PIPELINE_DIR / "pipeline_metadata.json"
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Pipeline saved → {pipeline_path.name}")
    logger.info(f"Latest link   → {latest_path.name}")
    logger.info(f"Metadata      → {meta_path.name}")
    return latest_path


def load_and_predict(pipeline_path: Path, X: pd.DataFrame) -> np.ndarray:
    """Load pipeline and predict — demonstrates one-step inference."""
    pipeline = joblib.load(pipeline_path)
    predictions   = pipeline.predict(X)
    probabilities = pipeline.predict_proba(X)[:, 1]
    return predictions, probabilities


def main() -> None:
    logger.info("=" * 52)
    logger.info("ML Pipeline — Day 38")
    logger.info("=" * 52)

    X, y = get_features()

    # Build + train
    pipeline = build_and_train_pipeline(X, y)

    # Save
    pipeline_path = save_pipeline(pipeline, X)

    # Verify load + predict
    logger.info("\n── Verify: Load + Predict ────────────────────")
    preds, probs = load_and_predict(pipeline_path, X)
    logger.info(f"Predictions shape: {preds.shape}")
    logger.info(f"Predicted active:  {preds.sum()}")
    logger.info(f"Predicted churned: {(preds == 0).sum()}")
    logger.info(f"Avg churn prob:    {(1 - probs).mean():.4f}")

    logger.info("\n✅ Pipeline complete. Check pipelines/ folder.")


if __name__ == "__main__":
    main()