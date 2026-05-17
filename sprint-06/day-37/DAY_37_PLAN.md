# 📅 DAY 37 — Sprint 06 | RandomForest + GridSearchCV + Churn Dashboard
## Compare Models, Tune Hyperparameters, Add Churn Risk to Streamlit

---

## 🔁 RETROSPECTIVE — Day 36

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| feature_engineering.py | ✅ Pass | 43KB features.csv |
| LogisticRegression trained | ✅ Pass | 3 model files saved |
| accuracy 61% | ✅ Expected | 3 churned in test — unreliable metric |
| save_model() | ✅ Pass | joblib correctly used |
| predict.py | ✅ Pass | predictions.csv generated |
| tag_name hardcoded fix | ✅ Apply | Dynamic sprint from branch name |

### Pre-Start
```bash
cd C:\90_day_python_de_plan

# Apply tag fix in daily_commit.py first
# Then commit:
git add scripts/daily_commit.py
git commit -m "[DAY-036][FIX] Dynamic sprint tag in merge_to_main"
git push

git checkout develop
git pull origin develop
git checkout -b sprint-06/day-37-random-forest
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-09: ML Foundations & Scikit-learn                         |
| Story           | ST-37: RandomForest + GridSearchCV + Churn Dashboard         |
| Task ID         | TASK-037                                                     |
| Sprint          | Sprint 06 (Days 36–42)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | randomforest, gridsearch, comparison, streamlit, day-37      |
| Acceptance Criteria | RandomForest trained; GridSearchCV finds best params; model comparison table; churn risk page in Streamlit |

---

## 📚 BACKGROUND

### Why RandomForest Over LogisticRegression?

```
LogisticRegression:
  ✅ Fast, interpretable, works well on linear relationships
  ❌ Struggles with non-linear patterns
  ❌ Sensitive to feature scale (needs StandardScaler)

RandomForest:
  ✅ Handles non-linear relationships naturally
  ✅ Built-in feature importance (no coefficients needed)
  ✅ Less sensitive to scale (no StandardScaler needed)
  ✅ Robust to outliers
  ❌ Slower to train, less interpretable
  ❌ Can overfit on small datasets
```

### GridSearchCV — Hyperparameter Tuning

```python
from sklearn.model_selection import GridSearchCV

# Define parameter grid to search
param_grid = {
    "n_estimators":  [50, 100, 200],
    "max_depth":     [3, 5, 10, None],
    "min_samples_split": [2, 5, 10],
}

# GridSearchCV tries ALL combinations with cross-validation
# 3 × 4 × 3 = 36 combinations × 5 folds = 180 model fits
gs = GridSearchCV(
    RandomForestClassifier(random_state=42, class_weight="balanced"),
    param_grid,
    cv=5,
    scoring="f1",    # optimise for F1, not accuracy (better for imbalanced)
    n_jobs=-1,       # use all CPU cores
    verbose=1,
)
gs.fit(X_train, y_train)

best_model  = gs.best_estimator_
best_params = gs.best_params_
best_score  = gs.best_score_
```

### Model Comparison — What to Compare

```
Metric          Why it matters
──────────────  ────────────────────────────────────────────
Accuracy        Overall correctness (misleading for imbalanced)
Precision       Of predicted churned, how many actually churned?
Recall          Of all churned, how many did we catch?
F1 Score        Balance of precision and recall — best for imbalance
ROC-AUC         Discrimination ability across all thresholds
Training time   Practical deployment consideration
```

---

## 🎯 OBJECTIVES

1. Train `RandomForestClassifier` on same features as Day 36
2. Use `GridSearchCV` to find optimal hyperparameters
3. Compare LogisticRegression vs RandomForest on all metrics
4. Plot feature importance from RandomForest
5. Add "Churn Risk" page to Streamlit dashboard
6. Push clean `[DAY-037][S06]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                           |
|-------|----------|----------------------------------------------------|
| A     | 10 min   | Branch setup + fix                                 |
| B     | 40 min   | `random_forest.py` — RF + GridSearch + comparison  |
| C     | 35 min   | `pages/churn_risk.py` — Streamlit churn page       |
| D     | 15 min   | Run + verify + git push                            |
| E     | 20 min   | Commit                                             |

---

## 📝 EXERCISES

---

### EXERCISE 1 — random_forest.py (Block B)
**[Q1 RF + GridSearch fully provided. Q2 comparison table — write yourself]**

Create `sprint-06/day-37/random_forest.py`:

```python
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
    raise NotImplementedError("Implement compare_models")


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
```

---

### EXERCISE 2 — pages/churn_risk.py (Block C)
**[Write yourself — follows same pattern as rentals.py from Day 35]**

Create `sprint-05/day-31/pages/churn_risk.py`:

```python
"""
churn_risk.py — Churn Risk Streamlit Page
==========================================
YOUR TASK: Build churn risk analytics page.

Requirements:
1. Page title: "⚠️ Churn Risk"

2. Load predictions from Day 36 predictions.csv:
   predictions_path = Path(__file__).resolve().parent.parent.parent
       / "sprint-06" / "day-36" / "output" / "predictions.csv"
   pred_df = pd.read_csv(predictions_path)

3. Load customer data from db.py:
   customers = load_customers()
   # Merge predictions with customer names if possible
   # customers has customer_id, pred_df doesn't — use index alignment
   # or just show predictions as-is

4. 3 KPI metrics:
   - Predicted Active:  pred_df["predicted_active"].sum()
   - Predicted Churned: (pred_df["predicted_active"] == 0).sum()
   - Model Accuracy:    f"{pred_df['correct'].mean():.1%}"

5. Churn risk histogram:
   px.histogram(pred_df, x="churn_probability", nbins=20,
                title="Churn Probability Distribution",
                color_discrete_sequence=["tomato"])

6. High risk customers table (churn_probability > 0.5):
   high_risk = pred_df[pred_df["churn_probability"] > 0.5]
   st.warning(f"⚠️ {len(high_risk)} customers at high churn risk")
   st.dataframe(high_risk, use_container_width=True)

7. Model comparison table (load from model_comparison.csv if exists):
   comp_path = Path(...) / "sprint-06" / "day-37" / "output" / "model_comparison.csv"
   if comp_path.exists():
       comp_df = pd.read_csv(comp_path)
       st.subheader("Model Comparison")
       st.dataframe(comp_df, use_container_width=True)

8. Add to app.py sidebar:
   elif page == "⚠️ Churn Risk":
       import pages.churn_risk as churn
       churn.render()
"""
# YOUR CODE HERE
```

---

### EXERCISE 3 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 37 --sprint 6 ^
    --message "RandomForest GridSearchCV, model comparison, churn risk Streamlit page" ^
    --merge
```

---

## ✅ DAY 37 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | tag_name fix committed in daily_commit.py                                | [ ]   |
| 2 | `random_forest.py` runs — best params logged                             | [ ]   |
| 3 | GridSearch completes in <60 seconds                                      | [ ]   |
| 4 | Feature importance printed with bar chart                                | [ ]   |
| 5 | RF model saved as `churn_rf_model.pkl`                                   | [ ]   |
| 6 | **`compare_models()` written — 2-row comparison DataFrame**              | [ ]   |
| 7 | `model_comparison.csv` saved with Accuracy/F1/Precision/Recall           | [ ]   |
| 8 | **`pages/churn_risk.py` written — 3 KPIs + histogram + high risk table** | [ ]   |
| 9 | Churn Risk page accessible from Streamlit sidebar                        | [ ]   |
|10 | One clean `[DAY-037][S06]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 SELF-CHECK

```bash
# model_comparison.csv should show:
python -c "
import pandas as pd
df = pd.read_csv('sprint-06/day-37/output/model_comparison.csv')
print(df.to_string(index=False))
"
# Expected:
#          Model  Accuracy     F1  Precision  Recall
# LogisticRegression  0.61xx  0.7xxx  0.96xx  0.61xx
#       RandomForest  0.9xxx  0.9xxx  0.9xxx  0.9xxx
```

---

## 🔜 PREVIEW: DAY 38

**Topic:** SMOTE oversampling + model persistence pipeline  
**What you'll do:** Use `imbalanced-learn` SMOTE to create synthetic churn samples,
retrain both models on balanced data, compare results, write an automated
retraining pipeline that runs on a schedule via Airflow.

---

*Day 37 | Sprint 06 | EP-09 | TASK-037*
