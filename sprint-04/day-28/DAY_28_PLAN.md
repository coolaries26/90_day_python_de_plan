# 📅 DAY 28 — Sprint 04 Test + Sprint Close
## Airflow Assessment + sprint-04-complete Tag

---

## 🔁 RETROSPECTIVE — Day 27

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Dataset outlets on ETL DAGs | ✅ Pass | customer + film both producing |
| audit_report dataset schedule | ✅ Pass | dataset_triggered__ prefix confirmed |
| Auto-trigger timing | ✅ Pass | Fired within seconds of film ETL |
| TriggerDagRunOperator | ✅ Pass | Added to customer ETL |
| daily_commit.py untracked | ✅ Confirmed | git add -A working correctly |

### Pre-Start
```bash
# WSL2 — start services if not running
source ~/.bashrc
airflow webserver --port 8081 &
airflow scheduler &

# Windows Git Bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-04/day-28-sprint-test
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-06: Workflow Orchestration                                |
| Story           | ST-28: Sprint 04 Test + Assessment                           |
| Task ID         | TASK-028                                                     |
| Sprint          | Sprint 04 (Days 21–28)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | sprint-test, assessment, airflow, day-28                     |
| Acceptance Criteria | All 5 tasks pass; sprint-04-complete tag created; Sprint 05 unlocked |

---

## 📚 SPRINT 04 TEST — RULES

```
1. No looking at previous day plans during test tasks
2. 90-minute time box total
3. Document blockers and move on if stuck after time box
4. Honest self-scoring
```

---

## 🎯 SPRINT 04 TEST — 5 TASKS

**Time box: 90 minutes total**

---

### TASK T1 — Write a New DAG from Scratch (25 min)

**Brief:** Without referencing previous DAGs, write `dag_rental_summary.py`
that summarises the `rental` table daily.

**Requirements:**
- DAG ID: `rental_summary_daily`
- Schedule: `@daily`, `catchup=False`, `max_active_runs=1`
- Uses `db_connection_pool` pool
- Uses `on_failure_callback` from `airflow_callbacks.py`
- Has exactly 3 tasks using TaskFlow API (`@task` + `@dag`):

```
Task 1: extract_rental_stats
  - Query rental table: total_rentals, open_rentals (return_date IS NULL),
    returned_rentals, unique_customers
  - Return dict with these 4 keys

Task 2: validate_rental_stats(stats: dict)
  - Receives stats from Task 1 automatically (TaskFlow)
  - Assert total_rentals >= 15000
  - Assert unique_customers >= 500
  - Return "validation_passed"

Task 3: write_rental_report(stats: dict, validation: str)
  - Receives stats + validation result
  - Write to /mnt/c/90_day_python_de_plan/airflow/output/rental_report.md
  - Content: timestamp, all 4 stats, validation status
  - Return output file path
```

**Pass criteria:**
```bash
airflow dags list | grep rental_summary_daily
airflow dags trigger rental_summary_daily
# All 3 tasks green
cat airflow/output/rental_report.md
# Shows all 4 stats with correct values
```

---

### TASK T2 — Add Dataset to Rental DAG (10 min)

**Brief:** Make `rental_summary_daily` a dataset producer.

**Requirements:**
- Define `RENTAL_DATASET = Dataset("postgresql://dvdrental/rental_summary")`
- Add `outlets=[RENTAL_DATASET]` to the `extract_rental_stats` task
- Update `audit_report_taskflow` to also consume `RENTAL_DATASET`:
  ```python
  schedule=[CUSTOMER_DATASET, FILM_DATASET, RENTAL_DATASET]
  ```

**Pass criteria:**
```
Trigger rental_summary_daily + customer_etl_daily + film_etl_daily
→ audit_report_taskflow triggers automatically (all 3 datasets updated)
```

---

### TASK T3 — Pool + Priority (10 min)

**Brief:** Configure `rental_summary_daily` correctly.

**Requirements:**
- `extract_rental_stats` uses `db_connection_pool`, `priority_weight=7`
- `rental_summary_daily` sits between customer (10) and film (5) in priority
- Add `max_active_runs=1` to DAG

**Pass criteria:**
```bash
airflow pools list
# db_connection_pool still shows 3 slots
# rental task visible in pool slot usage when running
```

---

### TASK T4 — Backfill (10 min)

**Brief:** Run `rental_summary_daily` for the past 3 days.

```bash
airflow dags backfill \
    --start-date 2026-05-05 \
    --end-date 2026-05-07 \
    --reset-dagruns \
    rental_summary_daily
```

**Pass criteria:**
```bash
airflow dags list-runs -d rental_summary_daily --output table | head -5
# Shows 3 successful backfill runs + 1 manual run
```

---

### TASK T5 — etl_audit_log Verification (10 min)

**Brief:** Without looking at previous queries, write a Python one-liner
that shows the last 5 pipeline runs grouped by status.

```bash
python -c "
# Your query here
# Expected output format:
# status     | count
# -----------+------
# success    | XX
# failed     | 3
"
```

**Pass criteria:**
- Query runs without error
- Shows both `success` and `failed` counts
- `failed` count = 3 (from Day 24 failure_test)

---

## 📊 SPRINT 04 SCORING RUBRIC

| Task | Max | Your Score | Notes |
|------|-----|------------|-------|
| T1: rental_summary_daily — 3 tasks green | 25 | | |
| T2: Dataset outlet + audit auto-trigger | 15 | | |
| T3: Pool + priority correctly set | 10 | | |
| T4: Backfill 3 dates successful | 15 | | |
| T5: etl_audit_log grouped query | 10 | | |
| Code quality: ip route, no resolv.conf | 10 | | |
| Git: one clean [DAY-028][S04] commit | 15 | | |
| **TOTAL** | **100** | | |

**Thresholds:**
```
≥85  → Sprint 05 starts Day 29 — full speed ahead
70–84 → Sprint 05 starts with one remediation task
<70  → Two remediation days before Sprint 05
```

---

## 📤 SPRINT CLOSE

```bash
cd C:\90_day_python_de_plan

# 1. Commit Day 28 work
python scripts/daily_commit.py --day 28 --sprint 4 ^
    --message "Sprint 04 test: rental_summary DAG, dataset trigger, backfill, all tasks green" ^
    --merge

# 2. Close Sprint 04
python scripts/daily_commit.py --day 28 --sprint 4 ^
    --message "Sprint 04 complete" ^
    --to-main

# 3. Verify tags
git tag
# Should show:
# sprint-01-complete
# sprint-02-complete
# sprint-03-complete
# sprint-04-complete
```

---

## ✅ DAY 28 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `dag_rental_summary.py` created with TaskFlow API                        | [ ]   |
| 2 | All 3 TaskFlow tasks green on trigger                                    | [ ]   |
| 3 | `rental_report.md` exists with correct stats                             | [ ]   |
| 4 | `RENTAL_DATASET` outlet added                                            | [ ]   |
| 5 | `audit_report_taskflow` consumes all 3 datasets                          | [ ]   |
| 6 | `priority_weight=7` on extract_rental_stats                              | [ ]   |
| 7 | Backfill 3 dates successful                                              | [ ]   |
| 8 | etl_audit_log grouped query returns correct counts                       | [ ]   |
| 9 | `sprint-04-complete` tag created and pushed                              | [ ]   |
|10 | All 4 sprint tags visible in `git tag`                                   | [ ]   |

---

## 🔍 SELF-CHECK

```bash
# Rental report should contain:
cat airflow/output/rental_report.md

# Expected values from dvdrental:
# total_rentals:    16,044
# open_rentals:     183   (return_date IS NULL)
# returned_rentals: 15,861
# unique_customers: 599

# etl_audit_log query:
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_query, close_pool
rows = execute_query('''
    SELECT status, COUNT(*) as count
    FROM etl_audit_log
    GROUP BY status ORDER BY count DESC
''', as_dict=True)
for r in rows: print(r)
close_pool()
"
```

---

## 🔜 PREVIEW: SPRINT 05 — Day 29

**Topic:** Data Visualization with Matplotlib + Seaborn  
**What you'll do:** Build charts from your analytics tables, export as PNG,
embed in Airflow reports. First step toward the Streamlit dashboard on Day 33.

**Stack added in Sprint 05:**
```
matplotlib, seaborn, plotly, streamlit
All connect to your PostgreSQL analytics tables
```

---

*Day 28 | Sprint 04 | EP-06 | TASK-028*