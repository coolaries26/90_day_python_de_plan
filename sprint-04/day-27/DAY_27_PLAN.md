# 📅 DAY 27 — Sprint 04 | Data-Aware Scheduling + Dataset Triggers
## Event-Driven DAGs, Airflow Datasets, TriggerDagRunOperator

---

## 🔁 RETROSPECTIVE — Day 26

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| db_connection_pool 3 slots | ✅ Pass | |
| etl_pool 2 slots | ✅ Pass | |
| priority_demo DAG | ✅ Pass | Both runs succeeded |
| priority weights on ETL DAGs | ✅ Pass | 10/5/1 hierarchy |
| max_active_runs=1 | ✅ Pass | Overlap prevention |
| Untracked files missed | ⚠️ Fix | dag_priority_demo.py not in commit |

### Fix Before Starting
```bash
# Windows Git Bash
cd C:\90_day_python_de_plan
git checkout sprint-04/day-26-pools-priorities
git add airflow/dags/dag_priority_demo.py sprint-04/day-26/
git commit -m "[DAY-026][FIX] Add dag_priority_demo.py and day-26 folder"
git push
git checkout develop
git merge sprint-04/day-26-pools-priorities --no-edit
git push origin develop

# Then create Day 27 branch
git checkout -b sprint-04/day-27-dataset-triggers
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-06: Workflow Orchestration                                |
| Story           | ST-27: Data-Aware Scheduling + Dataset Triggers              |
| Task ID         | TASK-027                                                     |
| Sprint          | Sprint 04 (Days 21–28)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | airflow, datasets, event-driven, trigger, day-27             |
| Acceptance Criteria | audit_report triggers automatically when both ETL DAGs complete; TriggerDagRunOperator used; dataset dependency visible in UI |

---

## 📚 BACKGROUND

### The Problem with Schedule-Based Triggers

```
Current setup:
  customer_etl_daily  → @daily (midnight)
  film_etl_daily      → @daily (midnight)
  pipeline_audit_report → @daily (midnight)

Problem: all three run at midnight simultaneously
  → Audit report might run BEFORE ETL pipelines finish
  → Report shows stale data
  → You need audit to run AFTER both ETL DAGs complete

Old fix: set audit to run at 1am (fragile — what if ETL takes >1 hour?)
Modern fix: Airflow Datasets (event-driven)
```

### Airflow Datasets (2.4+)

```python
from airflow import Dataset

# Define datasets — logical identifiers for data
customer_dataset = Dataset("postgresql://dvdrental/analytics_customer_airflow")
film_dataset     = Dataset("postgresql://dvdrental/analytics_film_airflow")

# Producer DAG — declares it UPDATES a dataset
with DAG(...) as dag:
    task = PythonOperator(
        task_id="run_etl",
        outlets=[customer_dataset],   # ← signals: this task updated the dataset
        ...
    )

# Consumer DAG — triggers when datasets are updated
with DAG(
    dag_id="audit_report",
    schedule=[customer_dataset, film_dataset],  # ← run when BOTH are updated
    ...
) as dag:
    ...
```

**Visual in UI:** Datasets view shows producer → consumer relationships.
Audit DAG shows "Waiting for: customer_dataset, film_dataset".
When both are updated → audit DAG triggers automatically.

### TriggerDagRunOperator — Explicit DAG Triggering

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Trigger another DAG from within a DAG
trigger = TriggerDagRunOperator(
    task_id="trigger_audit",
    trigger_dag_id="pipeline_audit_report",
    wait_for_completion=True,     # wait for triggered DAG to finish
    conf={"triggered_by": "customer_etl"},  # pass config to triggered DAG
)
```

---

## 🎯 OBJECTIVES

1. Define `customer_dataset` and `film_dataset` as Airflow Datasets
2. Add `outlets=[dataset]` to customer and film ETL DAGs
3. Update `pipeline_audit_report` to use dataset schedule
4. Add `TriggerDagRunOperator` as alternative trigger method
5. Trigger ETL DAGs and verify audit report fires automatically
6. Push clean `[DAY-027][S04]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                           |
|-------|----------|----------------------------------------------------|
| A     | 15 min   | Fix Day 26 + branch setup                          |
| B     | 35 min   | Add Dataset outlets to ETL DAGs                    |
| C     | 35 min   | Update audit DAG + TriggerDagRunOperator           |
| D     | 15 min   | Trigger + verify automatic scheduling              |
| E     | 20 min   | Git push + merge                                   |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Define Datasets (Block B)
**[Full steps — add to each ETL DAG]**

**Update `dag_customer_etl.py`:**
```python
# Add import at top:
from airflow import Dataset

# Define dataset
CUSTOMER_DATASET = Dataset(
    "postgresql://dvdrental/analytics_customer_airflow"
)

# Add outlets to the ETL task:
task_etl = PythonOperator(
    task_id="run_customer_etl",
    python_callable=run_customer_etl,
    outlets=[CUSTOMER_DATASET],    # ← signals dataset was updated
    pool="etl_pool",
    priority_weight=10,
    weight_rule="absolute",
    sla=timedelta(minutes=5),
)
```

**Update `dag_film_etl.py`:**
```python
from airflow import Dataset

FILM_DATASET = Dataset(
    "postgresql://dvdrental/analytics_film_airflow"
)

# Add to the load task inside transform_load TaskGroup:
task_load = PythonOperator(
    task_id="transform_and_load_films",
    python_callable=transform_and_load_films,
    outlets=[FILM_DATASET],        # ← signals dataset was updated
    pool="etl_pool",
    priority_weight=5,
    weight_rule="absolute",
)
```

**Verify datasets registered:**
```bash
airflow datasets list 2>/dev/null || \
airflow dags list  # datasets appear after DAG runs with outlets
```

---

### EXERCISE 2 — Update audit_report DAG with Dataset Schedule (Block C)
**[Full steps for dataset schedule. TriggerDagRunOperator — write yourself]**

**Update `dag_audit_taskflow.py`:**
```python
# Add import:
from airflow import Dataset

# Define the datasets this DAG consumes:
CUSTOMER_DATASET = Dataset(
    "postgresql://dvdrental/analytics_customer_airflow"
)
FILM_DATASET = Dataset(
    "postgresql://dvdrental/analytics_film_airflow"
)

# Change schedule from @daily to dataset-based:
@dag(
    dag_id="audit_report_taskflow",
    description="Audit report — triggers when ETL datasets updated",
    schedule=[CUSTOMER_DATASET, FILM_DATASET],  # ← event-driven
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["audit", "taskflow", "dataset", "sprint-04"],
    default_args={
        "owner": "python-de-journey",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "priority_weight": 1,
        "weight_rule": "absolute",
    },
)
def audit_pipeline():
    summary = read_audit_log()
    counts  = check_counts()
    write_report(summary, counts)

audit_dag = audit_pipeline()
```

---

### EXERCISE 3 — TriggerDagRunOperator (Block C — write yourself)

**Add to `dag_customer_etl.py` as the final task:**

```python
# HINTS:
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
#
# task_trigger_audit = TriggerDagRunOperator(
#     task_id="trigger_audit_report",
#     trigger_dag_id="pipeline_audit_report",
#     wait_for_completion=False,    # don't block customer ETL waiting for audit
#     reset_dag_run=True,           # reset if already running
#     conf={
#         "triggered_by": "customer_etl_daily",
#         "trigger_time": "{{ ts }}",   # Jinja template for execution timestamp
#     },
# )
#
# Add to dependency chain AFTER validate task:
# ... >> validate >> task_trigger_audit
#
# Note: This gives you TWO ways audit can run:
#   1. Dataset trigger (automatic when both ETL datasets updated)
#   2. TriggerDagRunOperator (explicitly triggered by customer ETL)
# For testing, use TriggerDagRunOperator since datasets need both to update

# YOUR CODE HERE
```

---

### EXERCISE 4 — Trigger + Verify Auto-Schedule (Block D)

```bash
# Trigger customer ETL (signals CUSTOMER_DATASET)
airflow dags trigger customer_etl_daily
sleep 30

# Trigger film ETL (signals FILM_DATASET)
# Once both are updated → audit_report_taskflow triggers automatically
airflow dags trigger film_etl_daily
sleep 60

# Check if audit report triggered automatically
airflow dags list-runs -d audit_report_taskflow --output table | head -5

# Check dataset update history
airflow dags list-runs -d customer_etl_daily --output table | tail -3
airflow dags list-runs -d film_etl_daily --output table | tail -3

# Verify TriggerDagRunOperator fired
airflow dags list-runs -d pipeline_audit_report --output table | tail -3
```

**In Airflow UI:**
```
Navigate to: Browse → Datasets
You should see:
  postgresql://dvdrental/analytics_customer_airflow
    └── Updated by: customer_etl_daily
    └── Consumed by: audit_report_taskflow

  postgresql://dvdrental/analytics_film_airflow
    └── Updated by: film_etl_daily
    └── Consumed by: audit_report_taskflow
```

---

### EXERCISE 5 — Git Push

```bash
cd C:\90_day_python_de_plan

python scripts/daily_commit.py --day 27 --sprint 4 ^
    --message "Airflow: Dataset outlets on ETL DAGs, audit_report dataset schedule, TriggerDagRunOperator" ^
    --merge
```

---

## ✅ DAY 27 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | Day 26 fix commit pushed (dag_priority_demo.py)                          | [ ]   |
| 2 | `CUSTOMER_DATASET` defined + added as outlet to customer ETL task        | [ ]   |
| 3 | `FILM_DATASET` defined + added as outlet to film ETL load task           | [ ]   |
| 4 | `audit_report_taskflow` schedule changed to `[CUSTOMER_DATASET, FILM_DATASET]` | [ ]   |
| 5 | **`TriggerDagRunOperator` added to customer ETL by you**                 | [ ]   |
| 6 | Both ETL DAGs triggered manually                                         | [ ]   |
| 7 | Audit DAG triggered automatically (dataset event or explicit trigger)    | [ ]   |
| 8 | Datasets visible in Airflow UI under Browse → Datasets                   | [ ]   |
| 9 | One clean `[DAY-027][S04]` commit via `daily_commit.py --merge`          | [ ]   |

---

## ⚠️ WATCH OUT FOR

**Dataset triggers require BOTH datasets to be updated in the same "epoch":**
Airflow tracks dataset updates per scheduling interval. If you trigger
`customer_etl_daily` at 10:00 and `film_etl_daily` at 10:30, the audit DAG
will trigger after the film ETL completes. But if you trigger customer at 10:00
on Day 1 and film at 10:00 on Day 2, they may not be considered the same epoch.
For testing: trigger both within a few minutes of each other.

**`TriggerDagRunOperator` with `wait_for_completion=False`:**
This is non-blocking — customer ETL marks itself successful immediately
after triggering audit. Good for production. If you set `wait_for_completion=True`,
customer ETL waits for audit to finish before marking success — only use this
when audit is truly a dependency of customer ETL completion.

**Dataset URI format:**
Airflow Datasets use URIs as identifiers — the format doesn't need to be a real URL.
`postgresql://dvdrental/analytics_customer_airflow` is just a string label.
Consistency matters more than format — use the same URI in producer and consumer.

---

## 🔜 PREVIEW: DAY 28 — Sprint 04 Test

**Sprint 04 final test** — 5 tasks covering everything from Days 21–27.
Close sprint with `sprint-04-complete` tag.
Sprint 05 begins Day 29: Data Visualization + Streamlit dashboards.

---

*Day 27 | Sprint 04 | EP-06 | TASK-027*