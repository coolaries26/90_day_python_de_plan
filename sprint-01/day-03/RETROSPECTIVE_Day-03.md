# 🔁 Day 03 Retrospective
## Sprint 01 | Pandas DataFrames from PostgreSQL + First Data Exploration

> Fill this out at the START of Day 04 (before beginning Day 04 work).
> Honest reflection drives faster improvement.

---

## ✅ What Went Well

- [x] Day 02 rounding fix committed and pushed
- [x] `sprint-01/day-03/` folder created
- [x] `pandas_intro.py` runs — 4 tables loaded
- [x] `.info()` output reviewed for all 4 tables
- [x] Null columns identified and noted
- [x] `output/profile_report.md` generated
- [x] T1 customer summary runs — segments visible
- [x] T2 film value score runs — score range 0–100
- [x] T3 monthly cohort written — 4 rows, mom_growth_pct
- [x] `analytics_customer_summary` table exists in PostgreSQL
- [x] `analytics_film_value_score` table exists in PostgreSQL
- [x] `analytics_monthly_cohort` table exists in PostgreSQL
- [x] `output/dq_findings.md` with real numbers from run
- [x] Git pushed to `sprint-01/day-03-pandas-intro`

_Write notes here:_
Successfully loaded and profiled 4 DVD Rental tables using Pandas. Applied three transformations: customer enrichment, film value scoring, and monthly revenue cohort. Identified data quality issues like nulls in return_date and dtype mismatches. Wrote transformed DataFrames back to PostgreSQL as analytics tables. Fixed several runtime errors during development, including Period object serialization and duplicate checks on unhashable columns.

---

## ❌ What Didn't Go Well / Blockers

| Issue | Root Cause | Resolution |
|-------|------------|------------|
| PostgreSQL can't adapt pandas Period objects | Period objects not serializable by psycopg2 | Converted Period to string before writing to DB |
| TypeError in duplicated() check | special_features column contains lists (unhashable) | Handled unhashable types by checking hashable columns only |
| KeyError: 'payment_id' in aggregation | Column doesn't exist in payments DataFrame | Used existing columns like 'amount' for count |
| SQLAlchemy text() wrapper needed | psycopg2 compatibility issue with raw SQL | Wrapped SQL queries in text() for proper parameter handling |
| Permission denied for CREATE TABLE | appuser lacked CREATE on schema public | Granted CREATE permission on schema public to appuser |

---

## 📚 What I Learned Today

1. Pandas fundamentals: Loading DataFrames from SQL with `pd.read_sql()`, profiling with `.info()` and `.describe()`, handling nulls and duplicates.
2. Data transformations: Merging DataFrames, deriving new columns, grouping and aggregating, min-max normalization.
3. Writing to databases: Using `df.to_sql()` with different `if_exists` modes, handling serialization issues.
4. Data quality: Identifying dtype mismatches, null patterns, and precision risks in real datasets.
5. Error handling: Debugging pandas operations, SQLAlchemy compatibility, and permission issues in PostgreSQL.

---

## ⏱️ Time Tracking

| Block  | Estimated | Actual | Variance |
|--------|-----------|--------|----------|
| A (Branch setup + fix Day 02 rounding) | 10 min | 15 min | +5 min |
| B (`pandas_intro.py` — load + profile 4 tables) | 30 min | 35 min | +5 min |
| C (`data_explorer.py` — 3 transforms + write-back) | 40 min | 50 min | +10 min |
| D (Data quality finding + documentation) | 20 min | 20 min | 0 min |
| E (Log + git push) | 20 min | 15 min | -5 min |
| **Total** | **2 hr** | **2 hr 15 min** | **+15 min** |

---


_Paste git log --oneline -5 here:_

```
d4df5f8 [DAY-003] Pandas intro: load+profile 4 tables, 3 transforms, analytics tables written to DB
```

---
*Retrospective: Day 03 | Sprint 01*
