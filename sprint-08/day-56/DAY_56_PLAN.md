# 📅 DAY 56 — Sprint 08 Test + Sprint Close
## Advanced SQL + dbt Assessment + sprint-08-complete Tag

---

## 🔁 RETROSPECTIVE — Day 55

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| dbt installed in WSL2 | ✅ Pass | |
| dag_dbt_pipeline 4 tasks | ✅ Pass | 37s end-to-end |
| Dataset trigger chain | ✅ Pass | ETL → dbt pipeline |
| db.py updated to dbt marts | ✅ Pass | |
| delivery_days vs delivery_days_actual | ✅ Fixed | Good catch — documented |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-08/day-56-sprint-test
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-11: Advanced SQL & dbt |
| Story           | ST-56: Sprint 08 Test + Assessment |
| Task ID         | TASK-056 |
| Sprint          | Sprint 08 (Days 49–56) |
| Story Points    | 3 |
| Priority        | CRITICAL |
| Labels          | sprint-test, sql, dbt, assessment, day-56 |
| Acceptance Criteria | All 4 tasks pass; sprint-08-complete tag created |

---

## 📚 SPRINT 08 TEST — RULES

```
1. No looking at previous day plans during tasks
2. 90-minute time box total
3. Honest self-scoring
```

---

## 🎯 SPRINT 08 TEST — 4 TASKS

**Time box: 90 minutes total**

---

### TASK T1 — Window Function Query (25 min)

**Brief:** Write a single SQL query (no Python wrapper needed) that answers:

> "For each Brazilian state, find: total sellers, total revenue, average review score,
> and rank the state by total revenue. Also show what % of national revenue each state represents."

**Requirements:**
- Source: `analytics.seller_performance`
- One row per state
- Columns: `seller_state`, `seller_count`, `state_revenue`, `avg_review`,
  `revenue_rank` (RANK by state_revenue DESC),
  `revenue_share_pct` (state / national total × 100)
- Use window functions — no subqueries for the rank or share

**Run and save:**
```bash
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
df = pd.read_sql('''
    -- YOUR SQL HERE
''', engine)
print(df.to_string(index=False))
df.to_csv('sprint-08/day-56/t1_state_revenue_rank.csv', index=False)
dispose_ecommerce_engine()
"
```

**Pass criteria:**
```
27 rows (one per state)
SP has revenue_rank = 1
All revenue_share_pct values sum to ~100
```

---

### TASK T2 — dbt Model from Scratch (25 min)

**Brief:** Without referencing existing models, write a new dbt model
`mart_seller_summary.sql` in `models/marts/`.

**Requirements:**
- Source: use `{{ ref('stg_orders') }}` and `{{ source('raw', 'order_items') }}`
  and `{{ source('raw', 'sellers') }}`
- One row per `seller_id`
- Columns: `seller_id`, `seller_state`, `total_orders`, `total_revenue`,
  `avg_delivery_days`, `late_order_count`, `on_time_rate`
- Config: `materialized='table'`
- Add to `marts/schema.yml`: description + `unique` test on `seller_id`

**Run and verify:**
```bash
cd sprint-08/day-51/ecommerce_dbt
dbt run --select mart_seller_summary --profiles-dir "C:\90_day_python_de_plan\.dbt"
dbt test --select mart_seller_summary --profiles-dir "C:\90_day_python_de_plan\.dbt"
```

**Pass criteria:**
```
dbt run: 1/1 OK
dbt test: unique test passes
Row count: 3,095 (one per seller)
```

---

### TASK T3 — Index + EXPLAIN (15 min)

**Brief:** Without referencing Day 54, add an index that speeds up this query:

```sql
SELECT seller_id, total_revenue, avg_review_score
FROM analytics.seller_performance
WHERE on_time_delivery_rate >= 0.9
  AND total_revenue > 5000
ORDER BY total_revenue DESC
```

**Requirements:**
1. Measure time BEFORE index
2. Create the index (choose appropriate columns)
3. Measure time AFTER index
4. Print speedup

```bash
python sprint-08/day-56/t3_index_test.py
```

**Pass criteria:**
```
Index created without error
Before/after timing printed
Speedup calculated (any value — measuring is what matters)
```

---

### TASK T4 — dbt + Airflow Trigger (25 min)

**Brief:** Verify the full integrated pipeline runs correctly.

1. Trigger `dag_ecommerce_etl` manually
2. Wait for `dag_dbt_pipeline` to trigger automatically via dataset
3. Verify `dbt_dev_marts.mart_seller_summary` exists after the run
   (it was created in T2 — dbt run in Airflow should pick it up)

```bash
# WSL2
airflow dags trigger dag_ecommerce_etl
sleep 120
airflow dags list-runs -d dag_dbt_pipeline --output table | tail -3
airflow dags list-runs -d dag_ecommerce_etl --output table | tail -3
# Verify new mart table exists
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
cnt = pd.read_sql('SELECT COUNT(*) FROM dbt_dev_marts.mart_seller_summary', engine)
print(f'mart_seller_summary: {cnt.iloc[0,0]:,} rows')
dispose_ecommerce_engine()
"
```

**Pass criteria:**
```
dag_dbt_pipeline: success (triggered automatically)
mart_seller_summary: 3,095 rows
```

---

## 📊 SPRINT 08 SCORING RUBRIC

| Task | Max | Your Score | Notes |
|------|-----|------------|-------|
| T1: State revenue rank SQL — 27 rows, window functions | 25 | | |
| T2: mart_seller_summary — 3,095 rows, tests pass | 25 | | |
| T3: Index created + timing measured | 20 | | |
| T4: Full pipeline triggered, mart populated | 15 | | |
| Code quality: no hardcoded IPs, uses ref() correctly | 5 | | |
| Git: clean [DAY-056][S08] commit + sprint tag | 10 | | |
| **TOTAL** | **100** | | |

**Thresholds:**
```
≥85 → Sprint 09 starts Day 57
70-84 → Sprint 09 with one remediation
<70  → Two remediation days
```

---

## 📤 SPRINT CLOSE

```bash
cd C:\90_day_python_de_plan

# Commit Day 56
python scripts/daily_commit.py --day 56 --sprint 8 ^
    --message "Sprint 08 test: state revenue window, mart_seller_summary, index test, full pipeline" ^
    --merge

# Close Sprint 08
python scripts/daily_commit.py --day 56 --sprint 8 ^
    --message "Sprint 08 complete" ^
    --to-main

# Verify tags
git tag
# sprint-01-complete through sprint-08-complete
```

---

## ✅ DAY 56 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | T1: 27-row state revenue CSV saved | [ ] |
| 2 | T2: mart_seller_summary — 3,095 rows, tests pass | [ ] |
| 3 | T3: index timing script runs | [ ] |
| 4 | T4: full pipeline triggers automatically | [ ] |
| 5 | sprint-08-complete tag created | [ ] |
| 6 | All 8 sprint tags visible in `git tag` | [ ] |

---

## 🔍 SPRINT 08 — WHAT YOU MASTERED

```
Window functions:  ROW_NUMBER, RANK, DENSE_RANK, NTILE
                   LAG, LEAD, SUM OVER, AVG OVER
                   Partition by, Order by, Frame clauses

CTEs:              Simple, chained, recursive
Self-joins:        First/last comparison, state averages
EXPLAIN ANALYZE:   Seq Scan vs Index Scan, cost vs actual time

dbt:               Project setup, staging + mart models
                   Generic + singular tests
                   Incremental materialisation
                   Snapshots (SCD Type 2)
                   Documentation + lineage graph
                   Airflow orchestration via BashOperator

Indexes:           Single column, composite, function
                   When indexes help (high cardinality, small result)
                   When they don't (small tables, low cardinality)

Partitioning:      PARTITION BY RANGE, monthly partitions
                   Partition pruning, when partitioning helps
```

---

## 🔜 PREVIEW: SPRINT 09 — Day 57

**Topic:** Cloud fundamentals — AWS S3 + boto3  
**What you'll do:** Upload your analytics CSVs to S3, configure IAM,
read from S3 into pandas, build an Airflow DAG that archives
daily analytics outputs to S3. Cloud storage as a data lake layer.

**Alternative if no AWS account:**
Sprint 09 can also cover Great Expectations (data quality framework)
or advanced Python packaging — your choice when Sprint 08 closes.

---

*Day 56 | Sprint 08 | EP-11 | TASK-056*
