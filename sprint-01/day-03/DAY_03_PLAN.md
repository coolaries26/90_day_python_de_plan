# 📅 DAY 03 — Sprint 01 | Python Meets Data
## Pandas DataFrames from PostgreSQL + First Data Exploration

---

## 🔁 RETROSPECTIVE — Day 02 (Verify before starting)

### Required Fix from Day 02 Review
Before creating today's branch, merge the Day 02 fix:

```bash
cd C:\Users\Lenovo\python-de-journey
git checkout sprint-01/day-02-schema-queries

# Open queries.py and fix these two lines:
#   Q5: ROUND(AVG(f.rental_rate)::numeric, 4) AS avg_rental_rate
#   Q6: ROUND(EXTRACT(EPOCH FROM (r.return_date - due_date))/86400, 2) AS days_overdue

python sprint-01/day-02/queries.py   # re-verify output
git add sprint-01/day-02/queries.py
git commit -m "[DAY-002][FIX] ROUND avg_rental_rate(4dp) and days_overdue(2dp)"
git push

# Now create today's branch off develop
git checkout develop
git merge sprint-01/day-02-schema-queries   # bring fixes into develop
git push origin develop
git checkout -b sprint-01/day-03-pandas-intro
```

### Instructor Checklist
| Item | Status | Note |
|------|--------|------|
| Q1–Q3 output | ✅ Pass | Correct row counts and values |
| Q4 top customer | ✅ Pass | Eleanor Hunt $211.55 correct |
| Q5 top category | ✅ Pass | Sports first — correct |
| Q5 avg_rental_rate | ⚠️ Fix required | 16dp → ROUND to 4dp |
| Q6 overdue logic | ✅ Pass | Correct join + date calc |
| Q6 days_overdue | ⚠️ Fix required | 16dp → ROUND to 2dp |
| CSV output | ✅ Pass | 6 files generated |
| db_utils.py | ✅ Pass | Module structure correct |

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-01: Environment & Foundations                             |
| Story           | ST-03: Pandas DataFrames from PostgreSQL Data                |
| Task ID         | TASK-003                                                     |
| Sprint          | Sprint 01 (Days 1–7)                                         |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | pandas, dataframe, eda, postgresql, day-03                   |
| Acceptance Criteria | 4 DataFrames loaded from DB; .info()/.describe() used; 3 transformations applied; DataFrame written back to PostgreSQL; pushed to git |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                          |
|---------------|------------------------------------------------|
| Branch        | `sprint-01/day-03-pandas-intro`                |
| Base Branch   | `develop`                                      |
| Commit Prefix | `[DAY-003]`                                    |
| Folder        | `sprint-01/day-03/`                            |
| Files to Push | `pandas_intro.py`, `data_explorer.py`, `output/*.csv`, `output/*.md` |

---

## 📚 BACKGROUND

Yesterday you got data **out** of PostgreSQL via Python. Today you load it into **Pandas** —
the central data structure of all Python data engineering.

As a DBA you think in tables: rows, columns, joins, aggregations. Pandas DataFrames
are exactly that — but in memory, with 200+ built-in operations and seamless interop
with every other data tool in the Python ecosystem (SQLAlchemy, NumPy, Matplotlib,
scikit-learn, Airflow).

### The Bridge Pattern (used every day in this program)
```
PostgreSQL Table
      │
      ▼  pd.read_sql(query, engine)       ← DB → DataFrame
 DataFrame
      │
      ▼  transform / clean / enrich
 DataFrame
      │
      ▼  df.to_sql(table, engine)         ← DataFrame → DB
PostgreSQL Table (new or updated)
```

### Why Pandas over raw psycopg2 for analysis?
| psycopg2 rows          | Pandas DataFrame             |
|------------------------|------------------------------|
| `list[tuple]`          | Named columns, index         |
| Manual iteration       | Vectorised operations        |
| No built-in stats      | `.describe()` in one call    |
| Manual CSV writing     | `.to_csv()` one line         |
| No missing-value tools | `.isna()`, `.fillna()`       |

### Key Pandas concepts introduced today:
| Concept         | Method               | DBA equivalent        |
|-----------------|----------------------|-----------------------|
| Load from DB    | `pd.read_sql()`      | SELECT *              |
| Schema          | `df.info()`          | \d tablename          |
| Stats           | `df.describe()`      | SELECT MIN,MAX,AVG... |
| Filter rows     | `df[df.col > val]`   | WHERE clause          |
| Select columns  | `df[['col1','col2']]`| SELECT col1, col2     |
| Sort            | `df.sort_values()`   | ORDER BY              |
| Group + agg     | `df.groupby().agg()` | GROUP BY + aggregate  |
| Write to DB     | `df.to_sql()`        | INSERT INTO           |

---

## 🎯 OBJECTIVES

1. Load 4 DVD Rental tables into DataFrames using `pd.read_sql()`
2. Profile each DataFrame with `.info()` and `.describe()`
3. Apply 3 Pandas transformations (filter, groupby, merge)
4. Identify and handle one real data quality issue in the dataset
5. Write a transformed DataFrame back to PostgreSQL as a new table
6. Export analysis summary to CSV and Markdown

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                       |
|-------|----------|------------------------------------------------|
| A     | 10 min   | Branch setup + fix Day 02 rounding             |
| B     | 30 min   | `pandas_intro.py` — load + profile 4 tables    |
| C     | 40 min   | `data_explorer.py` — 3 transforms + write-back |
| D     | 20 min   | Data quality finding + documentation           |
| E     | 20 min   | Log + git push                                 |

---

## 📝 EXERCISES

---

### EXERCISE 1 — pandas_intro.py: Load & Profile (Block B)
**[Full steps — Pandas fundamentals every DE must know cold]**

**Objective:** Load 4 tables from PostgreSQL into DataFrames. Use `.info()` and
`.describe()` to understand the data before touching it. This mirrors what a DE does
on day one of any new data source.

Create `sprint-01/day-03/pandas_intro.py`:

```python
#!/usr/bin/env python3
"""
pandas_intro.py — Day 03 | Pandas Fundamentals
================================================
Load DVD Rental tables into DataFrames and profile them.
Demonstrates the core read → inspect → understand workflow.

Run: python pandas_intro.py
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "day-02"))
from db_utils import get_engine, dispose_engine

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ── 1. Load tables ─────────────────────────────────────────────────────────────
def load_tables() -> dict[str, pd.DataFrame]:
    """
    Load 4 core DVD Rental tables into DataFrames.
    pd.read_sql() handles connection + result → DataFrame in one call.
    The engine is passed in — pd.read_sql closes the connection after fetching.
    """
    engine = get_engine()
    tables = {
        "film":     "SELECT * FROM film",
        "customer": "SELECT * FROM customer",
        "rental":   "SELECT * FROM rental",
        "payment":  "SELECT * FROM payment",
    }
    dfs: dict[str, pd.DataFrame] = {}
    for name, sql in tables.items():
        dfs[name] = pd.read_sql(sql, engine)
        print(f"  ✅ {name:<12} loaded  →  {dfs[name].shape[0]:>6,} rows × {dfs[name].shape[1]} cols")
    return dfs


# ── 2. Profile each DataFrame ─────────────────────────────────────────────────
def profile_dataframe(name: str, df: pd.DataFrame) -> dict:
    """
    Run .info() and .describe() and capture key stats.
    Returns a dict summary for the report.
    """
    print(f"\n{'═'*54}")
    print(f"  TABLE: {name.upper()}")
    print(f"{'═'*54}")

    # .info() — dtypes, non-null counts, memory usage
    print("\n  [Schema — df.info()]")
    df.info(buf=sys.stdout, memory_usage="deep")

    # .describe() — for numeric columns
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if numeric_cols:
        print(f"\n  [Numeric Stats — df.describe()]")
        desc = df[numeric_cols].describe().round(4)
        print(desc.to_string())

    # Null check — critical for pipeline validation
    null_counts = df.isna().sum()
    null_cols = null_counts[null_counts > 0]
    print(f"\n  [Null Check]")
    if null_cols.empty:
        print("  No nulls found — clean table")
    else:
        for col, cnt in null_cols.items():
            pct = cnt / len(df) * 100
            print(f"  ⚠️  {col}: {cnt} nulls ({pct:.1f}%)")

    # Duplicate check
    dupes = df.duplicated().sum()
    print(f"\n  [Duplicate Rows]: {dupes}")

    return {
        "table":        name,
        "rows":         len(df),
        "columns":      len(df.columns),
        "null_columns": len(null_cols),
        "duplicates":   int(dupes),
        "memory_mb":    round(df.memory_usage(deep=True).sum() / 1_048_576, 3),
        "dtypes":       df.dtypes.value_counts().to_dict(),
    }


# ── 3. Write profile summary ──────────────────────────────────────────────────
def write_profile_report(summaries: list[dict]) -> None:
    report_path = OUTPUT_DIR / "profile_report.md"
    with open(report_path, "w") as f:
        f.write("# DataFrame Profile Report — Day 03\n\n")
        f.write("| Table | Rows | Columns | Null Cols | Duplicates | Memory (MB) |\n")
        f.write("|-------|------|---------|-----------|------------|-------------|\n")
        for s in summaries:
            f.write(
                f"| {s['table']} | {s['rows']:,} | {s['columns']} "
                f"| {s['null_columns']} | {s['duplicates']} | {s['memory_mb']} |\n"
            )
    print(f"\n  📄 Profile report → {report_path.name}")


def main():
    print("\n🐼 Pandas Intro — Load & Profile DVD Rental Tables")
    print("=" * 54)

    print("\n[1] Loading tables from PostgreSQL...")
    dfs = load_tables()

    print("\n[2] Profiling each DataFrame...")
    summaries = []
    for name, df in dfs.items():
        summary = profile_dataframe(name, df)
        summaries.append(summary)

    print("\n[3] Writing profile report...")
    write_profile_report(summaries)

    dispose_engine()
    print("\n✅ Done. Review output/profile_report.md\n")


if __name__ == "__main__":
    main()
```

**✅ Expected findings to note:**
- `rental.return_date` → has nulls (rentals not yet returned) — note this
- `customer.active` → dtype int64 but is really a boolean flag — note this
- `film` has several object (string) columns like `rating`, `special_features`
- `payment.amount` → float64 (monetary data stored as float — this is a real-world DQ issue)

---

### EXERCISE 2 — data_explorer.py: 3 Transforms + Write-back (Block C)
**[Q1–Q2 fully provided. Q3 write yourself — hints given]**

**Objective:** Apply three real transformations a data engineer performs on raw data before
loading it to an analytics layer.

Create `sprint-01/day-03/data_explorer.py`:

```python
#!/usr/bin/env python3
"""
data_explorer.py — Day 03 | Pandas Transformations
====================================================
Three transformations on DVD Rental data:
  T1: Customer enrichment (merge customer + rental + payment)
  T2: Film value score (derived column from existing fields)
  T3: [You write] Monthly cohort summary

Results written back to PostgreSQL as analytics tables.

Run: python data_explorer.py
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent / "day-02"))
from db_utils import get_engine, dispose_engine

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

engine = get_engine()


# ── T1: Customer enrichment ────────────────────────────────────────────────────
def t1_customer_summary() -> pd.DataFrame:
    """
    Transform 1: Merge customer + rental + payment into a customer summary.
    This is the foundation of a customer analytics table.

    Steps:
      1. Load three tables via SQL (push aggregation to DB — faster than Python)
      2. Merge on customer_id
      3. Derive new columns: customer_segment, days_since_last_rental
      4. Handle nulls in return_date (open rentals)
    """
    print("\n── T1: Customer Enrichment ──────────────────────────")

    # Load base tables
    customers = pd.read_sql("SELECT customer_id, first_name, last_name, email, active, create_date FROM customer", engine)
    spend = pd.read_sql("""
        SELECT
            customer_id,
            COUNT(rental_id)         AS total_rentals,
            ROUND(SUM(amount)::numeric, 2)  AS total_spend,
            MIN(payment_date)        AS first_payment,
            MAX(payment_date)        AS last_payment
        FROM payment
        GROUP BY customer_id
    """, engine)

    # Merge — equivalent to a LEFT JOIN
    df = customers.merge(spend, on="customer_id", how="left")
    print(f"  After merge: {df.shape}")

    # Derive: full name
    df["full_name"] = df["first_name"] + " " + df["last_name"]

    # Derive: customer segment based on total_spend
    # pd.cut() is like a CASE WHEN with value ranges
    df["segment"] = pd.cut(
        df["total_spend"].fillna(0),
        bins=[0, 50, 100, 150, float("inf")],
        labels=["Bronze", "Silver", "Gold", "Platinum"],
        right=True,
    )

    # Derive: days since last payment (relative to max date in dataset)
    max_date = df["last_payment"].max()
    df["days_since_last_payment"] = (max_date - df["last_payment"]).dt.days

    # Active flag: convert int → bool (DQ fix from profile step)
    df["is_active"] = df["active"].astype(bool)

    # Select final columns
    result = df[[
        "customer_id", "full_name", "email", "is_active",
        "segment", "total_rentals", "total_spend",
        "first_payment", "last_payment", "days_since_last_payment",
    ]].sort_values("total_spend", ascending=False)

    print(f"  Segments: {result['segment'].value_counts().to_dict()}")
    print(f"  Nulls in total_spend: {result['total_spend'].isna().sum()}")
    print(result.head(3).to_string(index=False))
    return result


# ── T2: Film value score ───────────────────────────────────────────────────────
def t2_film_value_score() -> pd.DataFrame:
    """
    Transform 2: Calculate a 'value score' for each film.
    Value = (rental_rate / rental_duration) * log(rental_count + 1)
    Higher score = high rate, short duration, frequently rented.
    This is a feature engineering pattern used in ML pipelines.

    Steps:
      1. Load film + rental counts from DB
      2. Apply numpy-based derived column
      3. Normalise score to 0–100 range (min-max scaling — preview of Sprint 12)
    """
    print("\n── T2: Film Value Score ─────────────────────────────")
    import numpy as np

    films = pd.read_sql("""
        SELECT
            f.film_id, f.title, f.rating, f.rental_rate,
            f.rental_duration, f.length, f.replacement_cost,
            c.name AS category,
            COUNT(r.rental_id) AS rental_count
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c       ON fc.category_id = c.category_id
        JOIN inventory i      ON f.film_id = i.film_id
        LEFT JOIN rental r    ON i.inventory_id = r.inventory_id
        GROUP BY f.film_id, f.title, f.rating, f.rental_rate,
                 f.rental_duration, f.length, f.replacement_cost, c.name
    """, engine)

    print(f"  Films loaded: {len(films)}")

    # Derived column: raw value score
    films["value_score_raw"] = (
        (films["rental_rate"] / films["rental_duration"]) *
        np.log1p(films["rental_count"])   # log1p = log(x+1) handles zero
    )

    # Min-max normalise to 0–100
    v_min = films["value_score_raw"].min()
    v_max = films["value_score_raw"].max()
    films["value_score"] = (
        (films["value_score_raw"] - v_min) / (v_max - v_min) * 100
    ).round(2)

    result = films[[
        "film_id", "title", "category", "rating",
        "rental_rate", "rental_duration", "rental_count",
        "value_score",
    ]].sort_values("value_score", ascending=False)

    print(f"  Top film: {result.iloc[0]['title']} (score: {result.iloc[0]['value_score']})")
    print(f"  Score range: {result['value_score'].min()} – {result['value_score'].max()}")
    return result


# ── T3: WRITE THIS YOURSELF (hints below) ─────────────────────────────────────
def t3_monthly_revenue_cohort() -> pd.DataFrame:
    """
    Transform 3: Monthly revenue summary with month-over-month growth %.

    HINTS:
      - Load from payment table: payment_date, amount
      - Use pd.to_datetime() to ensure payment_date is datetime dtype
      - Use df['payment_date'].dt.to_period('M') to get year-month period
      - Group by the period column, aggregate: sum(amount), count(payment_id)
      - Derive: mom_growth_pct = revenue.pct_change() * 100, round to 2dp
      - pct_change() returns NaN for first row — that's correct, leave it
      - Expected output columns:
            month, total_revenue, payment_count, avg_payment, mom_growth_pct
      - Expected rows: 4 (matches Q3 from Day 02)

    Self-check:
      - July 2005 should be the peak revenue month
      - mom_growth_pct for June → July should be a large positive number (~240%)
    """
    print("\n── T3: Monthly Revenue Cohort ───────────────────────")

    # YOUR CODE HERE
    # Step 1: load payment table
    # Step 2: ensure datetime dtype
    # Step 3: create month period column
    # Step 4: groupby + aggregate
    # Step 5: derive mom_growth_pct
    # Step 6: return clean DataFrame

    raise NotImplementedError("Implement T3 — see hints above")


# ── Write DataFrames back to PostgreSQL ───────────────────────────────────────
def write_to_db(df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> None:
    """
    Write a DataFrame to PostgreSQL.
    if_exists='replace' → drop and recreate table (safe for analytics tables)
    if_exists='append'  → add rows (for incremental loads)

    Note: to_sql() uses SQLAlchemy engine — connection managed by pool.
    """
    df.to_sql(
        name=table_name,
        con=engine,
        schema="public",
        if_exists=if_exists,
        index=False,
        method="multi",      # batch insert — faster than row-by-row
        chunksize=500,
    )
    # Verify write
    count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {table_name}", engine).iloc[0]["n"]
    print(f"  ✅ Written to DB: {table_name}  ({count:,} rows)")


def write_csv(df: pd.DataFrame, filename: str) -> None:
    path = OUTPUT_DIR / filename
    df.to_csv(path, index=False)
    print(f"  📄 CSV → {path.name}")


def main():
    print("\n🔄 DVD Rental — Pandas Transformations")
    print("=" * 52)

    # T1
    t1 = t1_customer_summary()
    write_to_db(t1, "analytics_customer_summary")
    write_csv(t1, "t1_customer_summary.csv")

    # T2
    t2 = t2_film_value_score()
    write_to_db(t2, "analytics_film_value_score")
    write_csv(t2, "t2_film_value_score.csv")

    # T3 — your implementation
    try:
        t3 = t3_monthly_revenue_cohort()
        write_to_db(t3, "analytics_monthly_cohort")
        write_csv(t3, "t3_monthly_cohort.csv")
    except NotImplementedError as e:
        print(f"  ⏳ {e}")

    # Verify analytics tables exist in DB
    print("\n── Analytics Tables in PostgreSQL ───────────────────")
    tables = pd.read_sql("""
        SELECT tablename,
               pg_size_pretty(pg_total_relation_size(quote_ident(tablename)::regclass)) AS size
        FROM pg_tables
        WHERE tablename LIKE 'analytics_%'
        ORDER BY tablename
    """, engine)
    print(tables.to_string(index=False))

    dispose_engine()
    print("\n✅ Done.\n")


if __name__ == "__main__":
    main()
```

---

### EXERCISE 3 — Data Quality Finding Documentation (Block D)
**[Write yourself — no hints needed, you are a DBA]**

The `.info()` and null checks from Exercise 1 will reveal real issues. Document them.

Create `sprint-01/day-03/output/dq_findings.md` manually:

```markdown
# Data Quality Findings — Day 03

| Table   | Column         | Issue Type      | Detail                        | Recommended Fix        |
|---------|----------------|-----------------|-------------------------------|------------------------|
| rental  | return_date    | Expected nulls  | X rows null = open rentals    | Filter in pipeline     |
| customer| active         | Wrong dtype     | int64 should be boolean       | Cast in T1 transform   |
| payment | amount         | Precision risk  | float64 for monetary data     | Use Decimal or integer |
| ...     | ...            | ...             | Fill in from your .info() run |                        |
```

**Fill in actual numbers from your `pandas_intro.py` output.**
A DBA writing DQ findings with exact row counts is far more valuable than vague notes.

---

### EXERCISE 4 — Git Push (Block E)

```bash
cd C:\Users\Lenovo\python-de-journey

git add sprint-01/day-03/
git commit -m "[DAY-003] Pandas intro: load+profile 4 tables, 3 transforms, analytics tables written to DB"
git push -u origin sprint-01/day-03-pandas-intro

python scripts/daily_log.py --day 3 --sprint 1 ^
  --message "Pandas: loaded 4 tables, profiled with info()/describe(), T1+T2+T3 transforms, wrote analytics tables to PostgreSQL" ^
  --status done
```

---

## ✅ DAY 03 COMPLETION CHECKLIST

| # | Task                                                        | Done? |
|---|-------------------------------------------------------------|-------|
| 1 | Day 02 rounding fix committed and pushed                    | [ ]   |
| 2 | `sprint-01/day-03/` folder created                         | [ ]   |
| 3 | `pandas_intro.py` runs — 4 tables loaded                    | [ ]   |
| 4 | `.info()` output reviewed for all 4 tables                 | [ ]   |
| 5 | Null columns identified and noted                           | [ ]   |
| 6 | `output/profile_report.md` generated                       | [ ]   |
| 7 | T1 customer summary runs — segments visible                 | [ ]   |
| 8 | T2 film value score runs — score range 0–100               | [ ]   |
| 9 | **T3 monthly cohort written by you — 4 rows, mom_growth_pct**| [ ]  |
|10 | `analytics_customer_summary` table exists in PostgreSQL     | [ ]   |
|11 | `analytics_film_value_score` table exists in PostgreSQL     | [ ]   |
|12 | `analytics_monthly_cohort` table exists in PostgreSQL       | [ ]   |
|13 | `output/dq_findings.md` with real numbers from your run     | [ ]   |
|14 | Git pushed to `sprint-01/day-03-pandas-intro`               | [ ]   |

---

## 🔍 SELF-CHECK — T3 Expected Output

When T3 is correct, your output should look like this:

```
month       total_revenue  payment_count  avg_payment  mom_growth_pct
2005-06     8349.85        2015           4.14         NaN
2005-07     28377.87       6713           4.23         239.87
2005-08     24070.14       5686           4.23         -15.18
2006-02     514.18         182            2.82         -97.86
```

If your numbers match within rounding — T3 is correct.

---

## ⚠️ WATCH OUT FOR

**`df.to_sql()` and appuser permissions:**
`to_sql()` creates a new table with `if_exists='replace'`. This requires `CREATE TABLE`
permission. Since `appuser` has `NOCREATEDB` and `REVOKE CREATE ON SCHEMA public`,
this will **fail** unless you grant it:

```sql
-- Run as postgres superuser in psql:
\c dvdrental
GRANT CREATE ON SCHEMA public TO appuser;
```

This is intentional — we'll discuss the security trade-off in Sprint 04. For now,
grant it so the pipeline can write analytics tables.

---

## 🔜 PREVIEW: DAY 04

**Topic:** Python logging module + structured pipeline logging  
**What you'll do:** Replace all `print()` statements in db_utils, pandas_intro, and
data_explorer with proper `logging` calls. Add log rotation, log levels, and write a
log file per pipeline run. This becomes the logging backbone for all Sprint 07 ETL pipelines.

---

*Day 03 | Sprint 01 | EP-01 | TASK-003*
