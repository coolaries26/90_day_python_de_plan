# 📅 DAY 45 — Sprint 07 | Capstone ML
## Customer Churn + Delivery Delay Prediction on E-Commerce Data

---

## 🔁 RETROSPECTIVE — Day 44

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| 5 analytics tables | ✅ Pass | 195,994 total rows |
| Late delivery insight | ✅ Pass | 4.29 vs 2.57 — key finding |
| is_churned=1 for all | ⚠️ Fix | Dataset ends 2018 — use single-purchase definition |
| monthly_revenue=22 rows | ✅ Expected | Incomplete boundary months |

### Fix Churn Definition FIRST
```bash
cd C:\90_day_python_de_plan

# Open capstone/etl/analytics_etl.py
# Find CUSTOMER_LTV_SQL CASE statement for is_churned
# Replace:
#   WHEN days_since_last_order > 365 THEN 1
#   WHEN total_orders = 1 AND days_since_last_order > 180 THEN 1
# With:
#   WHEN total_orders = 1 THEN 1
#   ELSE 0

# Re-run ETL to fix the table
python capstone/etl/analytics_etl.py

# Verify churn split is now meaningful
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
df = pd.read_sql('''
    SELECT is_churned, COUNT(*) AS customers,
           ROUND(AVG(total_spent)::numeric, 2) AS avg_spend
    FROM analytics.customer_ltv
    GROUP BY is_churned ORDER BY is_churned
''', engine)
print(df.to_string(index=False))
dispose_ecommerce_engine()
"
# Expected:
# is_churned=0: ~5,000-8,000 customers (bought more than once)
# is_churned=1: ~88,000-91,000 customers (single purchase)

git checkout -b sprint-07/day-45-ml
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-10: Capstone & Job Readiness                              |
| Story           | ST-45: ML — Churn + Delivery Delay Prediction                |
| Task ID         | TASK-045                                                     |
| Sprint          | Sprint 07 (Days 43–48)                                       |
| Story Points    | 3                                                            |
| Priority        | CRITICAL                                                     |
| Labels          | ml, churn, delay, sklearn, capstone, day-45                  |
| Acceptance Criteria | Churn model trained on customer_ltv; delay model trained on order_metrics; both saved as pipelines; predictions written to ml schema |

---

## 🎯 OBJECTIVES

1. Fix churn definition (single-purchase = churned)
2. Train churn model on `analytics.customer_ltv`
3. Train delivery delay model on `analytics.order_metrics`
4. Save both as ImbPipelines
5. Write predictions to `ml.churn_predictions` and `ml.delay_predictions`
6. Push clean `[DAY-045][S07]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 15 min | Fix churn + branch |
| B | 35 min | `churn_model.py` |
| C | 35 min | `delay_model.py` |
| D | 15 min | Write predictions to DB |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — churn_model.py (Block B)
**[Feature engineering + model fully provided. Prediction write — write yourself]**

Create `capstone/ml/churn_model.py`:

```python
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
    features["total_orders"]          = df["total_orders"].fillna(0)
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
    raise NotImplementedError("Implement write_churn_predictions")


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
```

---

### EXERCISE 2 — delay_model.py (Block C)
**[Write yourself — follows same pattern as churn_model.py]**

Create `capstone/ml/delay_model.py`:

```python
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
# YOUR CODE HERE
```

---

### EXERCISE 3 — Verify Both Models

```bash
# Verify models saved
ls -lh capstone/ml/models/
# churn_pipeline_latest.pkl
# churn_pipeline_metadata.json
# delay_pipeline_latest.pkl
# delay_pipeline_metadata.json

# Verify predictions in DB
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()

for table, target_col in [('churn_predictions','predicted_churn'), ('delay_predictions','predicted_late')]:
    try:
        df = pd.read_sql(f'SELECT COUNT(*) AS cnt, AVG({target_col}::float) AS rate FROM ml.{table}', engine)
        print(f'ml.{table}: {df.cnt[0]:,} rows, {target_col} rate={df.rate[0]:.1%}')
    except Exception as e:
        print(f'ml.{table}: {e}')
dispose_ecommerce_engine()
"
```

---

### EXERCISE 4 — Git Push

```bash
python scripts/daily_commit.py --day 45 --sprint 7 ^
    --message "Capstone ML: churn pipeline, delay pipeline, predictions to ml schema" ^
    --merge
```

---

## ✅ DAY 45 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | Churn definition fixed: single-purchase = churned | [ ] |
| 2 | `churn_model.py` trains — CV F1 logged | [ ] |
| 3 | **`write_churn_predictions()` written — ml.churn_predictions populated** | [ ] |
| 4 | **`delay_model.py` written — ImbPipeline trained on order_metrics** | [ ] |
| 5 | **`write_delay_predictions()` written — ml.delay_predictions populated** | [ ] |
| 6 | 4 model files in `capstone/ml/models/` | [ ] |
| 7 | `ml.churn_predictions` has ~96k rows | [ ] |
| 8 | `ml.delay_predictions` has ~96k rows | [ ] |
| 9 | One clean `[DAY-045][S07]` commit | [ ] |

---

## 🔍 EXPECTED OUTPUT

```
Churn model:
  Churn rate: ~93% (single-purchase customers)
  CV F1: ~0.85-0.95 (RF handles this well)
  ml.churn_predictions: ~96,218 rows

Delay model:
  Late rate: ~8% (7,837 / 96,588)
  CV F1: ~0.80-0.90
  ml.delay_predictions: ~96,588 rows
```

---

## 🔜 PREVIEW: DAY 46

**Topic:** Airflow orchestration for the capstone pipeline  
**What you'll do:** Create `dag_ecommerce_etl.py` that runs the
full pipeline: load_raw_data → analytics_etl → churn_model → delay_model.
Dataset triggers chain the stages. The entire capstone pipeline runs
from a single Airflow DAG trigger.

---

*Day 45 | Sprint 07 | EP-10 | TASK-045*
