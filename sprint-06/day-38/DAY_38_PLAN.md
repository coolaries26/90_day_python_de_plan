# 📅 DAY 38 — Sprint 06 | SMOTE + Retraining Pipeline
## Synthetic Oversampling, Balanced Training, Automated ML Pipeline

---

## 🔁 RETROSPECTIVE — Day 37

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| RandomForest GridSearchCV | ✅ Pass | Completed in time |
| Feature importance logged | ✅ Pass | |
| compare_models() | ✅ Pass | LR=0.617 vs RF=0.950 |
| churn_risk.py Streamlit page | ✅ Pass | |
| tag_name fix | ✅ Pass | Dynamic from branch name |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-06/day-38-smote-pipeline
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-09: ML Foundations & Scikit-learn                         |
| Story           | ST-38: SMOTE + Automated ML Retraining Pipeline              |
| Task ID         | TASK-038                                                     |
| Sprint          | Sprint 06 (Days 36–42)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | smote, imbalanced-learn, pipeline, sklearn, day-38           |
| Acceptance Criteria | SMOTE applied; both models retrained on balanced data; before/after comparison; sklearn Pipeline object used |

---

## 📚 BACKGROUND

### Why SMOTE?

```
Problem (Day 36/37):
  Training data: 584 active, 15 churned (97.5% / 2.5%)
  Model sees 39x more active examples → biased toward predicting active
  Even class_weight="balanced" is a partial fix

SMOTE (Synthetic Minority Oversampling TEchnique):
  Creates SYNTHETIC churned customer examples by interpolating between
  real churned customers in feature space.
  
  Before SMOTE: 584 active, 15 churned
  After SMOTE:  584 active, 584 churned (or any ratio you choose)
  
  Result: model trains on balanced data → better minority class detection
```

### sklearn Pipeline — Production ML Pattern

```python
from sklearn.pipeline import Pipeline

# OLD (fragile — easy to forget a step):
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
model.fit(X_train_scaled, y_train)
X_test_scaled = scaler.transform(X_test)    # easy to forget this
y_pred = model.predict(X_test_scaled)

# NEW (Pipeline — steps always applied in order):
pipeline = Pipeline([
    ("scaler", StandardScaler()),
    ("model",  RandomForestClassifier()),
])
pipeline.fit(X_train, y_train)    # scaler.fit_transform + model.fit
y_pred = pipeline.predict(X_test) # scaler.transform + model.predict

# Saved as one object — scaler always travels with model:
joblib.dump(pipeline, "model_pipeline.pkl")
loaded = joblib.load("model_pipeline.pkl")
loaded.predict(X_new)  # scaling applied automatically
```

---

## 🎯 OBJECTIVES

1. Install `imbalanced-learn`
2. Apply SMOTE to training data
3. Retrain both models on SMOTE-balanced data
4. Compare before/after SMOTE metrics
5. Wrap model + preprocessing in `sklearn.pipeline.Pipeline`
6. Save pipeline (scaler + model in one object)
7. Write `ml_pipeline.py` — reusable retraining script
8. Push clean `[DAY-038][S06]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 15 min   | Install imbalanced-learn + branch                  |
| B     | 35 min   | `smote_training.py` — SMOTE + retrain              |
| C     | 35 min   | `ml_pipeline.py` — reusable Pipeline object        |
| D     | 15 min   | Save + verify                                      |
| E     | 20 min   | Git push                                           |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Install (Block A)

```bash
.venv\Scripts\activate
pip install imbalanced-learn==0.11.0

# Verify
python -c "import imblearn; print(imblearn.__version__)"
```

---

### EXERCISE 2 — smote_training.py (Block B)
**[SMOTE application fully provided. Before/After comparison — write yourself]**

Create `sprint-06/day-38/smote_training.py`:

```python
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
    raise NotImplementedError("Implement run_comparison")


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
```

---

### EXERCISE 3 — ml_pipeline.py: sklearn Pipeline Object (Block C)
**[Full steps — Pipeline is the production ML pattern]**

Create `sprint-06/day-38/ml_pipeline.py`:

```python
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
```

---

### EXERCISE 4 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 38 --sprint 6 ^
    --message "SMOTE oversampling, before/after comparison, sklearn ImbPipeline saved" ^
    --merge
```

---

## ✅ DAY 38 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `imbalanced-learn` installed                                             | [ ]   |
| 2 | `smote_training.py` runs — SMOTE before/after logged                    | [ ]   |
| 3 | **`run_comparison()` written — 4-row comparison DataFrame**              | [ ]   |
| 4 | `smote_comparison.csv` shows churn_recall improvement with SMOTE        | [ ]   |
| 5 | `ml_pipeline.py` runs — ImbPipeline trained                             | [ ]   |
| 6 | `churn_pipeline_latest.pkl` saved in pipelines/                         | [ ]   |
| 7 | Load + predict verified — 599 predictions total                          | [ ]   |
| 8 | One clean `[DAY-038][S06]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 SELF-CHECK

```bash
python -c "
import pandas as pd
df = pd.read_csv('sprint-06/day-38/output/smote_comparison.csv')
print(df[['label','accuracy','f1','churn_recall']].to_string(index=False))
"

# Key insight to look for:
# churn_recall WITH SMOTE should be higher than WITHOUT SMOTE
# Even if overall accuracy drops slightly — that's acceptable
# The goal is to catch more churned customers, not maximise overall accuracy
```

---

## 🔜 PREVIEW: DAY 39

**Topic:** Airflow ML retraining DAG  
**What you'll do:** Create `dag_ml_retrain.py` — an Airflow DAG that runs
`ml_pipeline.py` automatically after ETL completes, saves the new model,
and triggers the Streamlit cache to refresh predictions.

---

*Day 38 | Sprint 06 | EP-09 | TASK-038*
