#!/usr/bin/env python3
"""
feature_engineering.py — Day 36 | Feature Engineering for Churn Prediction
============================================================================
Builds a feature matrix from analytics_customer_airflow for ML training.

Features created:
  total_rentals          → how active was the customer
  total_spend            → monetary value
  avg_spend_per_rental   → spending pattern
  days_since_last_payment→ recency (key churn signal)
  segment_encoded        → Bronze=0, Silver=1, Gold=2, Platinum=3
  spend_per_day          → total_spend / days_as_customer (proxy)
  is_high_value          → total_spend > median (binary feature)

Target:
  is_active → 1 = active customer, 0 = churned
"""

from __future__ import annotations

import sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02")) # for db_utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04")) # for logger

from db_utils import get_engine, dispose_engine # for database connection management
from logger import get_pipeline_logger # for consistent logging across pipeline steps

logger = get_pipeline_logger("feature_engineering")

OUTPUT_DIR = Path(__file__).parent / "output" # directory for saving engineered features and logs
OUTPUT_DIR.mkdir(exist_ok=True)


def load_raw_data() -> pd.DataFrame:
    """Load customer analytics table from PostgreSQL."""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            customer_id,
            is_active,
            segment,
            total_rentals,
            total_spend,
            days_since_last_payment,
            first_payment,
            last_payment
        FROM analytics_customer_summary
        WHERE total_spend IS NOT NULL
    """, engine)
    dispose_engine()
    logger.info(f"Loaded {len(df)} customer records")
    logger.info(f"Churn rate: {(~df['is_active']).mean():.1%}")
    return df


def engineer_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    Transform raw customer data into ML-ready feature matrix.
    Returns (X_features, y_target).
    """
    features = pd.DataFrame()

    # 1. Direct numeric features
    features["total_rentals"]           = df["total_rentals"].fillna(0)
    features["total_spend"]             = df["total_spend"].fillna(0).astype(float)
    features["days_since_last_payment"] = df["days_since_last_payment"].fillna(999)

    # 2. Derived features
    features["avg_spend_per_rental"] = np.where(
        features["total_rentals"] > 0,
        features["total_spend"] / features["total_rentals"],
        0,
    )

    # 3. Log transform — reduces skew on monetary features
    # log1p(x) = log(1+x) handles zeros safely
    features["log_total_spend"]   = np.log1p(features["total_spend"])
    features["log_total_rentals"] = np.log1p(features["total_rentals"])

    # 4. Segment encoding — ordinal (Bronze < Silver < Gold < Platinum)
    segment_map = {"Bronze": 0, "Silver": 1, "Gold": 2, "Platinum": 3}
    features["segment_encoded"] = (
        df["segment"].map(segment_map).fillna(0).astype(int)
    )

    # 5. Binary features
    spend_median = features["total_spend"].median()
    features["is_high_value"] = (features["total_spend"] > spend_median).astype(int)
    features["is_recent"]     = (features["days_since_last_payment"] < 30).astype(int)

    # Target variable
    target = df["is_active"].astype(int)

    logger.info(f"Feature matrix shape: {features.shape}")
    logger.info(f"Features: {list(features.columns)}")
    logger.info(f"Target distribution: active={target.sum()}, churned={(~target.astype(bool)).sum()}")

    return features, target


def explore_features(X: pd.DataFrame, y: pd.Series) -> None:
    """NumPy-based feature exploration."""
    logger.info("\n── Feature Statistics (NumPy) ───────────────")

    X_np = X.to_numpy()
    feature_names = X.columns.tolist()

    for i, name in enumerate(feature_names):
        col = X_np[:, i]
        logger.info(
            f"{name:<30} mean={col.mean():.3f}  "
            f"std={col.std():.3f}  "
            f"min={col.min():.1f}  max={col.max():.1f}"
        )

    # Correlation with target
    logger.info("\n── Feature-Target Correlations ──────────────")
    y_np = y.to_numpy()
    for i, name in enumerate(feature_names):
        corr = np.corrcoef(X_np[:, i], y_np)[0, 1]
        direction = "↑" if corr > 0 else "↓"
        logger.info(f"{name:<30} corr={corr:+.4f} {direction}")


def save_features(X: pd.DataFrame, y: pd.Series) -> None:
    """Save feature matrix to CSV for inspection."""
    combined = X.copy()
    combined["target_is_active"] = y.values
    path = OUTPUT_DIR / "features.csv"
    combined.to_csv(path, index=False)
    logger.info(f"Features saved → {path.name} ({len(combined)} rows)")


def main() -> tuple[pd.DataFrame, pd.Series]:
    logger.info("=" * 52)
    logger.info("Feature Engineering — Day 36")
    logger.info("=" * 52)

    df   = load_raw_data()
    X, y = engineer_features(df)
    explore_features(X, y)
    save_features(X, y)

    return X, y


if __name__ == "__main__":
    main()