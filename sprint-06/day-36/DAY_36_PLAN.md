# 📅 DAY 36 — Sprint 06 | ML Foundations
## NumPy + Feature Engineering + First scikit-learn Model

---

## 🔁 SPRINT 05 CLOSE — Confirmed

```
sprint-05-complete tag: ✅
Score: 98/100
All 5 sprint tags: ✅
```

### Two Fixes Before Starting
```bash
cd C:\90_day_python_de_plan

# Fix 1: tag_name f-string in daily_commit.py
# Find: log.info("✅ Merged and pushed main + tag '{tag_name}'")
# Replace: log.info(f"✅ Merged and pushed main + tag '{tag_name}'")

# Fix 2: JIRA client instantiation
# Open scripts/daily_commit.py, find the JIRA call block
# Verify: jira_client = JiraClient()  ← instance (with parentheses)
# NOT:    jira_client = JiraClient    ← class reference (missing parentheses)

# Install ML libraries
.venv\Scripts\activate
pip install scikit-learn==1.3.2 joblib==1.3.2

# Create Day 36 branch
git checkout develop
git pull origin develop
git checkout -b sprint-06/day-36-ml-foundations
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-09: ML Foundations & Scikit-learn                         |
| Story           | ST-36: NumPy + Feature Engineering + First ML Model          |
| Task ID         | TASK-036                                                     |
| Sprint          | Sprint 06 (Days 36–42)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | numpy, sklearn, feature-engineering, classification, day-36  |
| Acceptance Criteria | Feature matrix built from customer analytics; LogisticRegression trained; accuracy > 60%; model saved with joblib |

---

## 📚 BACKGROUND

### Why ML After Data Engineering?

```
Data Engineer path:
  Raw data → ETL pipelines → analytics tables → dashboards

Adding ML:
  Analytics tables → feature engineering → ML model → predictions
                                                          ↓
                                               fed back into analytics tables
                                               shown on Streamlit dashboard
```

You already have the data pipeline. ML is the next layer on top.

### Customer Churn — The Problem

```
"Churn" = a customer who has stopped renting (gone inactive).
Predicting churn lets a business:
  - Target retention campaigns at likely-to-churn customers
  - Identify which customers need attention NOW
  - Measure lifetime value vs acquisition cost

In dvdrental:
  active = 1 → still a customer
  active = 0 → churned
  (simplified — real churn is time-based, but this is a valid starting point)
```

### ML Workflow

```
1. Feature Engineering  → transform raw data into numeric features
2. Train/Test Split     → hold out 20% for evaluation
3. Preprocessing        → scale features (StandardScaler)
4. Train Model          → LogisticRegression
5. Evaluate             → accuracy, classification report, confusion matrix
6. Save Model           → joblib.dump()
7. Predict              → joblib.load() + model.predict()
```

### NumPy — What You Need Today

```python
import numpy as np

# Array operations (vectorised — no loops)
arr = np.array([1, 2, 3, 4, 5])
arr * 2          # [2, 4, 6, 8, 10]
arr.mean()       # 3.0
arr.std()        # 1.414...
np.log1p(arr)    # log(1+x) for each element

# From DataFrame to numpy
X = df[feature_cols].to_numpy()   # shape: (n_samples, n_features)
y = df["target"].to_numpy()       # shape: (n_samples,)
```

---

## 🎯 OBJECTIVES

1. Build `feature_engineering.py` — create feature matrix from customer analytics
2. Explore features with NumPy (stats, distributions, correlations)
3. Train `LogisticRegression` model on churn prediction
4. Evaluate with accuracy, classification report, confusion matrix
5. Save model and scaler with `joblib`
6. Write `predict.py` — load model and predict for new customers
7. Push clean `[DAY-036][S06]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                            |
|-------|----------|----------------------------------------------------|
| A     | 15 min   | Install + branch setup + fixes                      |
| B     | 35 min   | `feature_engineering.py` — feature matrix          |
| C     | 35 min   | `train_model.py` — train + evaluate + save         |
| D     | 15 min   | `predict.py` — load + predict                      |
| E     | 20 min   | Git push                                            |

---

## 📝 EXERCISES

---

### EXERCISE 1 — feature_engineering.py (Block B)
**[Fully provided — study each feature and why it's useful]**

Create `sprint-06/day-36/feature_engineering.py`:

```python
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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))

from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("feature_engineering")

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_raw_data() -> pd.DataFrame:
    """Load customer analytics table from PostgreSQL."""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            customer_id,
            is_active,
            value_segment,
            total_rentals,
            total_spend,
            days_since_last_payment,
            first_payment,
            last_payment
        FROM analytics_customer_airflow
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
        df["value_segment"].map(segment_map).fillna(0).astype(int)
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
```

**✅ Checkpoint:**
```bash
python sprint-06/day-36/feature_engineering.py
# Should print feature stats and correlations
# features.csv should appear in output/
```

---

### EXERCISE 2 — train_model.py (Block C)
**[Train + evaluate fully provided. Model persistence — write yourself]**

Create `sprint-06/day-36/train_model.py`:

```python
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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from logger import get_pipeline_logger
from feature_engineering import main as get_features

logger = get_pipeline_logger("train_model")

MODEL_DIR = Path(__file__).parent / "models"
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
    logger.info(f"  True Negatives (correct churn):  {cm[0,0]}")
    logger.info(f"  False Positives (wrong active):  {cm[0,1]}")
    logger.info(f"  False Negatives (missed churn):  {cm[1,0]}")
    logger.info(f"  True Positives (correct active): {cm[1,1]}")

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
    raise NotImplementedError("Implement save_model")


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
```

---

### EXERCISE 3 — predict.py (Block D)
**[Write yourself — hints given]**

Create `sprint-06/day-36/predict.py`:

```python
"""
predict.py — Day 36 | Load Model + Predict Churn
=================================================
YOUR TASK: Load saved model and predict churn for all customers.

HINTS:
import joblib, json
from pathlib import Path

MODEL_DIR = Path(__file__).parent / "models"

Step 1: Load model metadata
    with open(MODEL_DIR / "model_metadata.json") as f:
        meta = json.load(f)

Step 2: Load model + scaler
    model  = joblib.load(MODEL_DIR / meta["model_file"])
    scaler = joblib.load(MODEL_DIR / meta["scaler_file"])

Step 3: Load features (reuse feature_engineering.py)
    from feature_engineering import main as get_features
    X, y = get_features()

Step 4: Scale and predict
    X_scaled = scaler.transform(X)
    predictions = model.predict(X_scaled)
    probabilities = model.predict_proba(X_scaled)[:, 1]  # prob of being active

Step 5: Build results DataFrame
    results = pd.DataFrame({
        "predicted_active":   predictions,
        "churn_probability":  (1 - probabilities).round(4),  # prob of churning
        "actual_active":      y.values,
        "correct":            (predictions == y.values).astype(int),
    })

Step 6: Print summary
    print(f"Predicted active:  {predictions.sum()}")
    print(f"Predicted churned: {(predictions == 0).sum()}")
    print(f"Overall accuracy:  {results['correct'].mean():.1%}")

Step 7: Save to output/predictions.csv
    results.to_csv(Path(__file__).parent / "output" / "predictions.csv", index=False)
    print("Saved → predictions.csv")
"""
# YOUR CODE HERE
```

---

### EXERCISE 4 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 36 --sprint 6 ^
    --message "ML foundations: feature engineering, LogisticRegression churn model, joblib save, predict.py" ^
    --merge
```

---

## ✅ DAY 36 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | scikit-learn + joblib installed                                           | [ ]   |
| 2 | `feature_engineering.py` runs — features.csv created                    | [ ]   |
| 3 | Feature correlations printed — days_since_last_payment likely top signal | [ ]   |
| 4 | `train_model.py` trains — accuracy > 60%                                 | [ ]   |
| 5 | Cross-validation scores printed (5-fold)                                 | [ ]   |
| 6 | Feature coefficients show which features drive churn                     | [ ]   |
| 7 | **`save_model()` written — 3 files in models/**                          | [ ]   |
| 8 | **`predict.py` written — predictions.csv created**                       | [ ]   |
| 9 | `ls sprint-06/day-36/models/` shows 3 files                              | [ ]   |
|10 | One clean `[DAY-036][S06]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 EXPECTED OUTPUT FROM train_model.py

```
Accuracy: ~0.75-0.85  (depends on features + class balance)

Classification Report:
              precision  recall  f1-score  support
    Churned      0.xx     0.xx    0.xx      xx
     Active      0.xx     0.xx    0.xx      xx

Top feature by coefficient magnitude:
  days_since_last_payment  → strong churn signal (negative = churned)
  total_rentals            → positive = active
  log_total_spend          → positive = active
```

---

## ⚠️ CLASS IMBALANCE NOTE

```
dvdrental has ~97% active customers (584/599) and ~3% churned (15/599).
This is severe imbalance — a model that always predicts "active" gets 97% accuracy
but is USELESS (it never identifies churned customers).

Two mitigations in the code:
1. class_weight="balanced" → penalises misclassifying the minority class more
2. stratify=y in train_test_split → preserves the ratio in both splits

In Day 38 you'll learn about SMOTE (oversampling) to handle this better.
```

---

## 🔜 PREVIEW: DAY 37

**Topic:** RandomForest + feature importance + hyperparameter tuning  
**What you'll do:** Replace LogisticRegression with RandomForestClassifier,
use GridSearchCV to tune hyperparameters, compare models, add the predictions
to the Streamlit dashboard as a "Churn Risk" page.

---

*Day 36 | Sprint 06 | EP-09 | TASK-036*