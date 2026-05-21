# 📅 DAY 42 — Sprint 06 Test + Sprint Close
## ML Assessment + sprint-06-complete Tag + Capstone Preview

---

## 🔁 RETROSPECTIVE — Day 41

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Elbow curve | ✅ Pass | 73KB — correct |
| Cluster scatter | ✅ Pass | 122KB — 599 points PCA 2D |
| k=3 optimal | ✅ Pass | Data-driven result |
| profile_clusters() | ✅ Pass | cluster_profiles.csv 3 rows |
| DB write 599 rows | ✅ Pass | |
| Streamlit clusters page | ✅ Pass | |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-06/day-42-sprint-test
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-09: ML Foundations & Scikit-learn                         |
| Story           | ST-42: Sprint 06 Test + Assessment                           |
| Task ID         | TASK-042                                                     |
| Sprint          | Sprint 06 (Days 36–42)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | sprint-test, ml, assessment, day-42                          |
| Acceptance Criteria | All 4 tasks pass; sprint-06-complete tag created; Day 43 capstone unlocked |

---

## 📚 SPRINT 06 TEST — RULES

```
1. No looking at previous day plans during test tasks
2. 90-minute time box total
3. Document blockers and move on after time box
4. Honest self-scoring
```

---

## 🎯 SPRINT 06 TEST — 4 TASKS

**Time box: 90 minutes total**

---

### TASK T1 — New Feature + Retrain (25 min)

**Brief:** Add one new feature to the churn model and retrain.

**Requirements:**
1. Open `sprint-06/day-36/feature_engineering.py`
2. Add a new feature `rental_frequency` defined as:
   ```python
   # total_rentals divided by days_since_last_payment + 1
   # (avoids division by zero)
   features["rental_frequency"] = (
       features["total_rentals"] / (features["days_since_last_payment"] + 1)
   ).round(4)
   ```
3. Retrain the ImbPipeline from Day 38:
   ```bash
   python sprint-06/day-38/ml_pipeline.py
   ```
4. Compare new F1 score vs original:
   - Original: CV F1 from Day 38 log
   - New: CV F1 with `rental_frequency` added

**Pass criteria:**
```bash
# Model trains without error
python sprint-06/day-38/ml_pipeline.py 2>&1 | grep "CV F1"
# Shows a CV F1 score
# Feature count increases from N to N+1
```

---

### TASK T2 — Drift Simulation + Alert Verification (15 min)

**Brief:** Verify the full drift detection chain works end-to-end.

**Requirements:**
1. Insert a new drift record with `status='drift'` and `delta=0.09`
2. Run the Streamlit app
3. Navigate to Churn Risk page
4. Screenshot or describe the alert banner shown

**Pass criteria:**
```bash
# Insert drift
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import get_engine, dispose_engine
from sqlalchemy import text
from datetime import datetime
engine = get_engine()
with engine.connect() as conn:
    conn.execute(text('''
        INSERT INTO ml_drift_log
            (baseline_accuracy, current_accuracy, delta, status, message)
        VALUES (0.95, 0.86, 0.09, 'drift',
                'Sprint 06 test: simulated 9% accuracy drop')
    '''))
    conn.commit()
dispose_engine()
print('Test drift inserted')
"
# Then describe what Streamlit shows
```

---

### TASK T3 — Cluster Analysis Query (20 min)

**Brief:** Without looking at previous queries, write a Python one-liner
that joins `analytics_customer_clusters` with `analytics_customer_airflow`
and shows the average spend and rental count per cluster — sorted by avg spend descending.

```bash
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_query, close_pool

# YOUR QUERY HERE
# Expected columns: cluster, customer_count, avg_spend, avg_rentals
# ORDER BY avg_spend DESC

rows = execute_query('''
    -- write your SQL here
''', as_dict=True)
for r in rows: print(r)
close_pool()
"
```

**Pass criteria:**
```
3 rows returned (one per cluster)
avg_spend values differ between clusters (shows meaningful segmentation)
ORDER BY avg_spend DESC correctly applied
```

---

### TASK T4 — ML Pipeline End-to-End (30 min)

**Brief:** Trigger the full ML pipeline via Airflow and verify
every table gets updated.

**Requirements:**
1. Trigger `customer_etl_daily` (which triggers `dag_ml_retrain` via dataset)
2. After both complete, verify:
   - `analytics_customer_airflow` — fresh ETL data
   - `ml_predictions` — fresh predictions (prediction_date = today)
   - `etl_audit_log` — new entry for dag_ml_retrain
   - `ml_drift_log` — new drift check entry (from Task 5)

**Pass criteria:**
```bash
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_query, execute_scalar, close_pool
from datetime import date

today = date.today().isoformat()
print('ETL rows:',        execute_scalar('SELECT COUNT(*) FROM analytics_customer_airflow'))
print('Predictions today:', execute_scalar(
    f\"SELECT COUNT(*) FROM ml_predictions WHERE prediction_date = '{today}'\"))
print('Audit entries:',   execute_scalar(
    \"SELECT COUNT(*) FROM etl_audit_log WHERE pipeline_name = 'dag_ml_retrain'\"))
print('Drift checks:',    execute_scalar('SELECT COUNT(*) FROM ml_drift_log'))
close_pool()
"
```

---

## 📊 SPRINT 06 SCORING RUBRIC

| Task | Max | Your Score | Notes |
|------|-----|------------|-------|
| T1: rental_frequency added, model retrains | 25 | | |
| T2: Drift alert visible in Streamlit | 20 | | |
| T3: Cluster join query — 3 rows, ordered | 20 | | |
| T4: Full pipeline — all 4 tables updated | 20 | | |
| Code quality: no print() in pipeline code | 5 | | |
| Git: one clean [DAY-042][S06] commit | 10 | | |
| **TOTAL** | **100** | | |

**Thresholds:**
```
≥85  → Sprint 07 (Capstone) starts Day 43
70–84 → Sprint 07 starts with one remediation task
<70  → Two remediation days
```

---

## 📤 SPRINT CLOSE

```bash
cd C:\90_day_python_de_plan

# Commit Day 42
python scripts/daily_commit.py --day 42 --sprint 6 ^
    --message "Sprint 06 test: rental_frequency feature, drift alert, cluster query, full ML pipeline" ^
    --merge

# Close Sprint 06
python scripts/daily_commit.py --day 42 --sprint 6 ^
    --message "Sprint 06 complete" ^
    --to-main

# Verify all sprint tags
git tag
# sprint-01-complete through sprint-06-complete
```

---

## ✅ DAY 42 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `rental_frequency` feature added and model retrains                      | [ ]   |
| 2 | Drift alert shows in Streamlit Churn Risk page                           | [ ]   |
| 3 | Cluster join query returns 3 rows ordered by avg_spend DESC              | [ ]   |
| 4 | All 4 tables updated after full Airflow pipeline run                     | [ ]   |
| 5 | `sprint-06-complete` tag created and pushed                              | [ ]   |
| 6 | All 6 sprint tags visible in `git tag`                                   | [ ]   |

---

## 🔍 SELF-CHECK

```bash
# Cluster profiles from Day 41 — paste this:
python -c "
import pandas as pd
df = pd.read_csv('sprint-06/day-41/output/cluster_profiles.csv')
print(df.to_string(index=False))
"

# T4 verification — full pipeline numbers:
# analytics_customer_airflow: 599
# ml_predictions today:       599
# audit entries for ML DAG:   ≥1
# drift_log rows:             ≥1 (including simulated from T2)
```

---

## 🔜 PREVIEW: SPRINT 07 — Day 43

**Topic:** Capstone Project  
**What you've built across 42 days:**
```
✅ ETL pipelines (psycopg2, SQLAlchemy, pandas)
✅ Airflow orchestration (10+ DAGs, datasets, pools, callbacks)
✅ Visualization (matplotlib, seaborn, plotly, Streamlit dashboard)
✅ ML pipeline (feature engineering, LogisticRegression, RandomForest, 
               SMOTE, KMeans, drift detection, auto-retrain)
```

**Sprint 07 (Days 43–48): Capstone**
```
Day 43  Capstone design + architecture diagram
Day 44  Build: end-to-end pipeline from raw → prediction
Day 45  Build: Streamlit dashboard integration
Day 46  Build: Airflow orchestration + scheduling
Day 47  Code review, documentation, README
Day 48  Final demo + self-assessment
```

The capstone is a **new dataset** — not dvdrental.  
You'll choose from: NYC Taxi, Airbnb Listings, or E-commerce Orders.  
All skills from Sprints 01–06 applied to fresh data.

---

*Day 42 | Sprint 06 | EP-09 | TASK-042*
