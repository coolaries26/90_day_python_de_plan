# 📅 DAY 40 — Sprint 06 | Model Evaluation + Drift Detection
## Baseline Comparison, Accuracy Drift Alert, Streamlit Drift Widget

---

## 🔁 RETROSPECTIVE — Day 39

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| extract_features XCom | ✅ Pass | |
| train_pipeline versioned pkl | ✅ Pass | |
| write_predictions_to_db | ✅ Pass | 599 rows correct |
| log_model_metrics | ✅ Pass | audit log entry written |
| churn_prob=0 for top customers | ✅ Expected | High spend = low churn risk |
| All 4 tasks green | ✅ Pass | |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-06/day-40-drift-detection
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-09: ML Foundations & Scikit-learn                         |
| Story           | ST-40: Model Evaluation + Drift Detection                    |
| Task ID         | TASK-040                                                     |
| Sprint          | Sprint 06 (Days 36–42)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | model-eval, drift, monitoring, streamlit, day-40             |
| Acceptance Criteria | Drift detector compares current vs baseline accuracy; WARNING logged when drift > threshold; Streamlit shows drift alert |

---

## 📚 BACKGROUND

### What Is Model Drift?

```
Data drift:   Input feature distributions change over time
              e.g. customers suddenly spending much less than before

Concept drift: The relationship between features and target changes
               e.g. high spend used to mean active, now even high spenders churn

Accuracy drift: Model accuracy on new data drops vs baseline
                This is what we detect today — simplest to implement

Detection approach (today):
  1. Establish baseline accuracy when model is first trained
  2. Each retrain: compute accuracy on held-out test set
  3. Compare current accuracy vs baseline
  4. If delta > threshold (5%) → log WARNING → show alert in dashboard
```

### Drift Detection Pipeline

```
dag_ml_retrain runs
    ↓
Task 5 (new today): evaluate_and_detect_drift
    ↓
  Load current model + baseline metrics from model_metadata.json
  Compute accuracy on current test set
  Compare: current_accuracy vs baseline_accuracy
    ↓
  No drift: log INFO
  Drift detected: log WARNING → write to ml_drift_log table
    ↓
  Streamlit churn_risk.py reads ml_drift_log → shows alert banner
```

---

## 🎯 OBJECTIVES

1. Create `drift_detector.py` — reusable drift detection module
2. Add Task 5 (`evaluate_and_detect_drift`) to `dag_ml_retrain.py`
3. Create `ml_drift_log` PostgreSQL table via Alembic
4. Update `pages/churn_risk.py` — show drift alert if detected
5. Simulate drift by artificially lowering accuracy
6. Push clean `[DAY-040][S06]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 15 min   | Branch + drift_detector.py module                  |
| B     | 35 min   | Add Task 5 to dag_ml_retrain.py                    |
| C     | 30 min   | Alembic migration + Streamlit drift alert          |
| D     | 20 min   | Simulate drift + verify alert fires               |
| E     | 20 min   | Git push                                           |

---

## 📝 EXERCISES

---

### EXERCISE 1 — drift_detector.py (Block A)
**[Fully provided — study the logic]**

Create `sprint-06/day-40/drift_detector.py`:

```python
#!/usr/bin/env python3
"""
drift_detector.py — Day 40 | Model Drift Detection
====================================================
Compares current model accuracy against baseline.
Logs WARNING when drift exceeds threshold.

Usage (standalone):
    python drift_detector.py

Usage (in Airflow task):
    from drift_detector import detect_drift
    result = detect_drift(current_accuracy=0.85, baseline_accuracy=0.95)
"""

from __future__ import annotations

import sys
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
from logger import get_pipeline_logger

logger = get_pipeline_logger("drift_detector")

DRIFT_THRESHOLD  = 0.05   # 5% accuracy drop = drift detected
WARN_THRESHOLD   = 0.02   # 2% drop = warning (not yet drift)


@dataclass
class DriftResult:
    """Result of a drift detection check."""
    baseline_accuracy: float
    current_accuracy:  float
    delta:             float
    drift_detected:    bool
    warning_only:      bool
    status:            str        # "ok", "warning", "drift"
    message:           str
    checked_at:        str

    @property
    def as_dict(self) -> dict:
        return {
            "baseline_accuracy": self.baseline_accuracy,
            "current_accuracy":  self.current_accuracy,
            "delta":             round(self.delta, 4),
            "drift_detected":    self.drift_detected,
            "warning_only":      self.warning_only,
            "status":            self.status,
            "message":           self.message,
            "checked_at":        self.checked_at,
        }


def detect_drift(
    current_accuracy: float,
    baseline_accuracy: float,
    drift_threshold: float = DRIFT_THRESHOLD,
    warn_threshold:  float = WARN_THRESHOLD,
) -> DriftResult:
    """
    Compare current model accuracy against baseline.
    Returns DriftResult with status and message.
    """
    delta = baseline_accuracy - current_accuracy

    if delta >= drift_threshold:
        status   = "drift"
        detected = True
        warning  = False
        message  = (
            f"🚨 DRIFT DETECTED: accuracy dropped {delta:.1%} "
            f"(baseline={baseline_accuracy:.1%} → current={current_accuracy:.1%}). "
            f"Consider retraining with fresh data."
        )
        logger.warning(message)

    elif delta >= warn_threshold:
        status   = "warning"
        detected = False
        warning  = True
        message  = (
            f"⚠️ Accuracy declining: {delta:.1%} drop "
            f"(baseline={baseline_accuracy:.1%} → current={current_accuracy:.1%}). "
            f"Monitor closely."
        )
        logger.warning(message)

    else:
        status   = "ok"
        detected = False
        warning  = False
        message  = (
            f"✅ No drift: accuracy delta={delta:.1%} "
            f"(baseline={baseline_accuracy:.1%}, current={current_accuracy:.1%})"
        )
        logger.info(message)

    return DriftResult(
        baseline_accuracy=baseline_accuracy,
        current_accuracy=current_accuracy,
        delta=delta,
        drift_detected=detected,
        warning_only=warning,
        status=status,
        message=message,
        checked_at=datetime.now().isoformat(),
    )


def load_baseline_accuracy(metadata_path: Path) -> float:
    """
    Load baseline accuracy from model metadata JSON.
    Returns a default if not found (first run).
    """
    if not metadata_path.exists():
        logger.info("No baseline metadata found — using default 0.90")
        return 0.90

    with open(metadata_path) as f:
        meta = json.load(f)
    return float(meta.get("baseline_accuracy", 0.90))


def save_baseline_accuracy(metadata_path: Path, accuracy: float) -> None:
    """Update baseline accuracy in metadata JSON."""
    meta = {}
    if metadata_path.exists():
        with open(metadata_path) as f:
            meta = json.load(f)
    meta["baseline_accuracy"] = round(accuracy, 4)
    meta["baseline_set_at"]   = datetime.now().isoformat()
    with open(metadata_path, "w") as f:
        json.dump(meta, f, indent=2)
    logger.info(f"Baseline accuracy saved: {accuracy:.4f}")


if __name__ == "__main__":
    # Demo
    print("\nDrift Detection Demo")
    print("=" * 40)
    for current in [0.95, 0.92, 0.89, 0.84]:
        result = detect_drift(current, baseline_accuracy=0.95)
        print(f"Current={current:.2f} → {result.status.upper()}: {result.message[:60]}")
```

---

### EXERCISE 2 — Add Task 5 to dag_ml_retrain.py (Block B)
**[Write yourself — builds on Tasks 3 + 4 pattern]**

Open `airflow/dags/dag_ml_retrain.py` and add:

```python
# Add import at top of file:
# sys.path already includes sprint-06/day-40 if you add it, OR
# put drift_detector.py in the same folder as the DAG

# Task 5 function — WRITE THIS YOURSELF:
def evaluate_and_detect_drift(**context) -> None:
    """
    Evaluate current model accuracy and check for drift.

    HINTS:
    Step 1: Pull model path from XCom
        model_path = context["ti"].xcom_pull(
            task_ids="train_pipeline", key="model_path"
        )

    Step 2: Load pipeline + features
        import joblib
        pipeline = joblib.load(model_path)
        from feature_engineering import main as get_features
        X, y = get_features()

    Step 3: Evaluate on 20% test set (same split as training)
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score
        _, X_test, _, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        y_pred = pipeline.predict(X_test)
        current_accuracy = accuracy_score(y_test, y_pred)

    Step 4: Load baseline + detect drift
        sys.path.insert(0, str(PROJECT_ROOT / "sprint-06" / "day-40"))
        from drift_detector import detect_drift, load_baseline_accuracy, save_baseline_accuracy

        meta_path = PIPELINE_DIR / "model_metadata.json"
        baseline  = load_baseline_accuracy(meta_path)
        result    = detect_drift(current_accuracy, baseline)

    Step 5: If this is the first run, save current as baseline
        if baseline == 0.90:   # default value = first run
            save_baseline_accuracy(meta_path, current_accuracy)

    Step 6: Write to ml_drift_log if drift or warning
        if result.drift_detected or result.warning_only:
            from sqlalchemy.orm import Session
            from db_utils import get_engine, dispose_engine
            engine = get_engine()
            # INSERT into ml_drift_log (create table if needed)
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO ml_drift_log
                        (checked_at, baseline_accuracy, current_accuracy,
                         delta, status, message)
                    VALUES (:checked_at, :baseline, :current, :delta, :status, :message)
                """), {
                    "checked_at": result.checked_at,
                    "baseline":   result.baseline_accuracy,
                    "current":    result.current_accuracy,
                    "delta":      result.delta,
                    "status":     result.status,
                    "message":    result.message,
                })
                conn.commit()
            dispose_engine()

    Step 7: Push result to XCom
        context["ti"].xcom_push(key="drift_result", value=result.as_dict)
        print(f"Drift check: {result.status} — {result.message}")
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement evaluate_and_detect_drift")


# Add to DAG — after task_log:
# task_drift = PythonOperator(
#     task_id="evaluate_and_detect_drift",
#     python_callable=evaluate_and_detect_drift,
#     trigger_rule="all_done",   # run even if previous tasks had issues
# )
# task_log >> task_drift
```

---

### EXERCISE 3 — Create ml_drift_log Table (Block C)
**[Full steps — create via psql, no Alembic needed for a simple table]**

```bash
# Windows PowerShell
psql -h 127.0.0.1 -U appuser -d dvdrental -W << 'EOF'
CREATE TABLE IF NOT EXISTS ml_drift_log (
    id              SERIAL PRIMARY KEY,
    checked_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    baseline_accuracy NUMERIC(6,4) NOT NULL,
    current_accuracy  NUMERIC(6,4) NOT NULL,
    delta           NUMERIC(6,4)  NOT NULL,
    status          VARCHAR(20)   NOT NULL,  -- ok / warning / drift
    message         TEXT
);

COMMENT ON TABLE ml_drift_log IS 'ML model drift detection log';
EOF

# Verify
psql -h 127.0.0.1 -U appuser -d dvdrental -W -c "\d ml_drift_log"
```

---

### EXERCISE 4 — Streamlit Drift Alert (Block C)
**[Write yourself — add to pages/churn_risk.py]**

Open `sprint-05/day-31/pages/churn_risk.py` and add at the top of `render()`:

```python
# HINTS:
# Load drift log from DB:
#   from db import _engine
#   import pandas as pd
#   try:
#       drift_df = pd.read_sql(
#           "SELECT * FROM ml_drift_log ORDER BY checked_at DESC LIMIT 5",
#           _engine()
#       )
#   except Exception:
#       drift_df = pd.DataFrame()

# Show alert banner if drift detected:
#   if not drift_df.empty:
#       latest = drift_df.iloc[0]
#       if latest["status"] == "drift":
#           st.error(f"🚨 MODEL DRIFT DETECTED: {latest['message']}")
#       elif latest["status"] == "warning":
#           st.warning(f"⚠️ Accuracy declining: {latest['message']}")
#       else:
#           st.success("✅ Model performing normally")
#   else:
#       st.info("No drift checks recorded yet")
```

---

### EXERCISE 5 — Simulate Drift + Verify Alert (Block D)

```python
# Manual drift simulation — run this once to test the alert:
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
sys.path.insert(0, 'sprint-06/day-40')
from db_utils import get_engine, dispose_engine
from sqlalchemy import text
from datetime import datetime

engine = get_engine()
with engine.connect() as conn:
    conn.execute(text('''
        INSERT INTO ml_drift_log
            (checked_at, baseline_accuracy, current_accuracy, delta, status, message)
        VALUES
            (:ts, 0.95, 0.87, 0.08, 'drift',
             'SIMULATED DRIFT: accuracy dropped 8% for testing')
    '''), {'ts': datetime.now().isoformat()})
    conn.commit()
dispose_engine()
print('Simulated drift inserted')
"

# Open Streamlit and verify alert banner appears:
# http://localhost:8501 → Churn Risk page
```

---

### EXERCISE 6 — Git Push

```bash
python scripts/daily_commit.py --day 40 --sprint 6 ^
    --message "Drift detection: drift_detector.py, Task 5 in ML DAG, ml_drift_log table, Streamlit alert" ^
    --merge
```

---

## ✅ DAY 40 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `drift_detector.py` standalone demo runs                                 | [ ]   |
| 2 | `detect_drift()` returns correct status for ok/warning/drift inputs      | [ ]   |
| 3 | **`evaluate_and_detect_drift` Task 5 written and added to DAG**          | [ ]   |
| 4 | `ml_drift_log` table created in PostgreSQL                               | [ ]   |
| 5 | Task 5 runs on DAG trigger — drift result pushed to XCom                 | [ ]   |
| 6 | **Streamlit drift alert added to churn_risk.py**                         | [ ]   |
| 7 | Simulated drift record visible as error banner in Streamlit              | [ ]   |
| 8 | One clean `[DAY-040][S06]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 SELF-CHECK

```bash
# Test drift_detector standalone:
python sprint-06/day-40/drift_detector.py
# Expected:
# Current=0.95 → OK: ✅ No drift: accuracy delta=0.0%
# Current=0.92 → WARNING: ⚠️ Accuracy declining...
# Current=0.89 → WARNING: ⚠️ Accuracy declining...
# Current=0.84 → DRIFT: 🚨 DRIFT DETECTED: accuracy dropped 11.0%

# Verify ml_drift_log:
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_query, close_pool
rows = execute_query('SELECT status, message FROM ml_drift_log ORDER BY id DESC LIMIT 3', as_dict=True)
for r in rows: print(r['status'], '|', r['message'][:60])
close_pool()
"
```

---

## 🔜 PREVIEW: DAY 41

**Topic:** Clustering — KMeans customer segmentation  
**What you'll do:** Apply KMeans clustering to create data-driven customer segments
(not rule-based like Bronze/Silver/Gold), compare with existing segments,
write cluster assignments to PostgreSQL, show on Streamlit.

---

*Day 40 | Sprint 06 | EP-09 | TASK-040*
