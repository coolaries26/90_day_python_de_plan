# 📅 DAY 20 — Sprint 03 Test + Sprint Close
## Self-Assessment Rubric + Code Review + Sprint-03-Complete Tag

---

## 🔁 RETROSPECTIVE — Days 14–19 (Gap Fill Sprint)

### Final Gap Fill Assessment
| Day | Topic | Score | Key Achievement |
|-----|-------|-------|-----------------|
| 14 | OOP depth | 6/6 | ETLConfig dataclass, Protocol, __repr__ lifecycle |
| 15 | mypy strict | 6/6 | 3 files passing mypy --strict |
| 16 | SQLAlchemy ORM + Alembic | 6/6 | etl_audit_log created via migration |
| 17 | Pandas window functions | 6/6 | 96% P4 total matched ground truth |
| 18 | pytest fixtures | 6/6 | 7/7, loguru bridge, parametrize |
| 19 | Coverage | 6/6 | 96% coverage, original tests fixed |

**Gap fill complete. All originally skipped skills now verified.**

### Branch Setup
```bash
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-03/day-20-sprint-test
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Python Core for DBAs                                  |
| Story           | ST-20: Sprint 03 Test + Assessment                           |
| Task ID         | TASK-020                                                     |
| Sprint          | Sprint 03 (Days 15–21)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | sprint-test, assessment, code-review, day-20                 |
| Acceptance Criteria | All 5 assessment tasks pass; sprint-03-complete tag created; Day 21 plan confirmed |

---

## 📚 SPRINT 03 TEST — RULES

```
1. No looking at previous day plans during the test tasks
2. Each task has a time box — stick to it
3. If stuck after the time box → document the blocker and move on
4. Honest self-scoring — the gaps you hide now become Sprint 09 failures
```

---

## 🎯 SPRINT 03 TEST — 5 TASKS

**Time box: 90 minutes total (18 min per task average)**

---

### TASK T1 — OOP: Build a FilmETLPipeline (20 min)

**Brief:** Without looking at `oop_etl.py`, create a new pipeline class
`FilmETLPipeline` that inherits from `BaseETLPipeline` and processes the `film` table.

**Requirements:**
- Inherits `BaseETLPipeline` from `sprint-03/day-14/oop_etl.py`
- `ETLConfig(source_table="film", target_table="analytics_film_sprint_test")`
- `extract()` → loads all films with category via JOIN
- `transform(df)` → adds `value_tier` column:
  - `rental_rate <= 0.99` → "Budget"
  - `rental_rate <= 2.99` → "Standard"
  - `rental_rate > 2.99`  → "Premium"
- `load(df)` → writes to `analytics_film_sprint_test` table
- `__repr__` inherited — must show `status='success'` after run

Create `sprint-03/day-20/film_etl_pipeline.py` and run it.

**Pass criteria:**
```
Pipeline SUCCESS | FilmETLPipeline(..., status='success') | rows=1000
Value tier distribution logged
analytics_film_sprint_test exists in PostgreSQL
```

---

### TASK T2 — Type Hints: Annotate FilmETLPipeline (10 min)

**Brief:** Run mypy on your `film_etl_pipeline.py`:

```bash
mypy sprint-03/day-20/film_etl_pipeline.py --ignore-missing-imports
```

Fix all errors until:
```
Success: no issues found in 1 source file
```

**Pass criteria:** `mypy` clean with zero errors.

---

### TASK T3 — pytest: Write 3 Tests for FilmETLPipeline (20 min)

**Brief:** Without looking at `test_etl_fixed.py`, write tests in
`sprint-03/day-20/test_film_pipeline.py`:

1. `test_film_extract_returns_dataframe` — extract returns DataFrame with film data
2. `test_film_transform_adds_value_tier` — transform adds correct tier for each rate range
3. `test_film_pipeline_success` — full run succeeds, load called once

**Requirements:**
- Use fixtures from `conftest.py` where applicable
- Mock all DB calls — no real PostgreSQL in unit tests
- All 3 passing

**Pass criteria:**
```bash
pytest sprint-03/day-20/test_film_pipeline.py -v
# 3 passed
```

---

### TASK T4 — Pandas: Revenue per Value Tier (15 min)

**Brief:** Using the `analytics_film_sprint_test` table you created in T1,
write a Pandas analysis in `sprint-03/day-20/tier_analysis.py`:

1. Load the table into a DataFrame
2. Group by `value_tier`, calculate:
   - `film_count`
   - `avg_rental_rate` (rounded to 4dp)
   - `avg_replacement_cost` (rounded to 2dp)
3. Add `pct_of_total` column — each tier's film_count as % of 1000
4. Sort by `avg_rental_rate` descending
5. Print result and write to `sprint-03/day-20/output/tier_analysis.csv`

**Pass criteria:**
```
3 rows (Budget, Standard, Premium)
pct_of_total sums to 100.0
CSV file exists
```

---

### TASK T5 — Alembic + ORM: Add Column via Migration (25 min)

**Brief:** Add a `value_tier` column to `etl_audit_log` via an Alembic migration.

**Steps:**
1. Add `value_tier: Mapped[str | None] = mapped_column(String(20), nullable=True)`
   to `AuditLog` model in `sprint-03/day-16/models.py`
2. Generate migration: `alembic revision --autogenerate -m "add value_tier to etl_audit_log"`
3. Review the generated migration file — confirm it adds the column
4. Run: `alembic upgrade head`
5. Verify in PostgreSQL:
```bash
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_query, close_pool
rows = execute_query(\"\"\"
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'etl_audit_log'
    ORDER BY ordinal_position
\"\"\")
for r in rows: print(r)
close_pool()
"
# Should show 10 columns including value_tier
```
6. Verify rollback works: `alembic downgrade -1`
   Then upgrade again: `alembic upgrade head`

**Pass criteria:**
```
etl_audit_log has 10 columns including value_tier
alembic downgrade -1 runs cleanly
alembic upgrade head restores the column
```

---

## 📊 SPRINT 03 SCORING RUBRIC

Score yourself honestly after completing all 5 tasks:

| Task | Max | Your Score | Notes |
|------|-----|------------|-------|
| T1: FilmETLPipeline runs, 1000 rows, status='success' | 20 | | |
| T2: mypy clean on film_etl_pipeline.py | 10 | | |
| T3: 3/3 tests passing | 20 | | |
| T4: Tier analysis, 3 rows, pct sums to 100 | 15 | | |
| T5: Migration up/down/up works cleanly | 20 | | |
| Code quality: no print(), f-strings in logger | 5 | | |
| Git: one clean [DAY-020][S03] commit | 10 | | |
| **TOTAL** | **100** | | |

**Thresholds:**
```
≥85  → Sprint 04 (Airflow) starts Day 21 — full speed ahead
70–84 → Sprint 04 starts but with one remediation task on Day 21
<70  → Two remediation days before Airflow
```

---

## 📤 SPRINT CLOSE — Run at End of Day 20

```bash
cd C:\Users\Lenovo\python-de-journey

# 1. Commit day 20 work
python scripts/daily_commit.py --day 20 --sprint 3 ^
    --message "Sprint 03 test: T1-T5 complete, FilmETLPipeline, mypy clean, 3/3 tests, tier analysis, Alembic migration" ^
    --merge

# 2. Close Sprint 03 — merge to main + tag
python scripts/daily_commit.py --day 20 --sprint 3 ^
    --message "Sprint 03 complete" ^
    --to-main

# 3. Verify
git log --oneline -5
git tag
# Should show: sprint-01-complete, sprint-02-complete, sprint-03-complete
```

---

## ✅ DAY 20 COMPLETION CHECKLIST

| # | Task                                                                  | Done? |
|---|-----------------------------------------------------------------------|-------|
| 1 | T1: `FilmETLPipeline` runs — 1000 rows, status='success'             | [ ]   |
| 2 | T1: `analytics_film_sprint_test` exists in PostgreSQL                 | [ ]   |
| 3 | T2: `mypy film_etl_pipeline.py` — zero errors                        | [ ]   |
| 4 | T3: `pytest test_film_pipeline.py` — 3/3 passing                     | [ ]   |
| 5 | T4: Tier analysis — 3 rows, pct_of_total sums to 100                 | [ ]   |
| 6 | T4: `tier_analysis.csv` exists                                       | [ ]   |
| 7 | T5: `value_tier` column added to `etl_audit_log` via Alembic         | [ ]   |
| 8 | T5: `alembic downgrade -1` + `alembic upgrade head` both work        | [ ]   |
| 9 | Self-score recorded in rubric above                                   | [ ]   |
|10 | `sprint-03-complete` tag created and pushed                           | [ ]   |
|11 | `git tag` shows all 3 sprint tags                                     | [ ]   |

---

## 🔜 PREVIEW: DAY 21 — Sprint 04 Begins

**Topic:** Apache Airflow setup + first DAG  
**What you'll do:** Install Airflow locally, configure PostgreSQL as metadata DB,
write your first DAG that runs the `CustomerETLPipeline` on a schedule.
Every ETL pipeline you've built becomes an Airflow task from Day 21 onward.

**Pre-requisite:** Score ≥70 on today's sprint test.

---

*Day 20 | Sprint 03 | EP-02 | TASK-020*
