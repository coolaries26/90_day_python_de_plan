# 📅 DAY 02 — Sprint 01 | PostgreSQL + Python Foundations
## DVD Rental Schema Deep-Dive + First Python-Driven SQL Queries

---

## 🔁 RETROSPECTIVE — Day 01 (Complete BEFORE starting Day 02 work)

### Verification Checklist
Run this first — takes 2 minutes:
```bash
cd C:\Users\Lenovo\python-de-journey
.venv\Scripts\activate

# 1. Confirm .env is at project root (fix from Day 01 feedback)
python -c "from pathlib import Path; print(Path('.env').resolve(), Path('.env').exists())"

# 2. Quick DB ping
python -c "
import os; from dotenv import load_dotenv; load_dotenv()
import psycopg2
c = psycopg2.connect(host=os.getenv('DB_HOST'), dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'))
print('DB OK:', c.get_dsn_parameters()['dbname']); c.close()
"
```

### Instructor Feedback on Day 01 Output
| Item | Assessment | Note |
|------|-----------|------|
| Python 3.12.4 | ✅ Acceptable | Newer than planned; no issues |
| All 8 packages | ✅ Pass | |
| psycopg2 + pool | ✅ Pass | Connection + pool cleanup working |
| appuser NOT superuser | ✅ Pass | Security posture correct |
| SQLAlchemy 15 tables | ⚠️ Note | information_schema sees 15; psql sees 22 (includes views) — both correct |
| DVD Rental data | ✅ Pass | All 10 core tables populated |
| Git branch + remote | ✅ Pass | Correct branch naming convention |
| .env location | ⚠️ Fix | Move to project root before Day 02 |

**Overall: Strong Day 01. Environment is solid. Move .env then proceed.**

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-01: Environment & Foundations                             |
| Story           | ST-02: Explore DVD Rental Schema + Python SQL Fundamentals   |
| Task ID         | TASK-002                                                     |
| Sprint          | Sprint 01 (Days 1–7)                                         |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | postgresql, psycopg2, sql, schema, dvdrental, day-02         |
| Acceptance Criteria | ERD understood; 6 Python query scripts producing correct output; results written to CSV; pushed to git |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-01/day-02-schema-queries`          |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-002]`                                |
| Folder        | `sprint-01/day-02/`                        |
| Files to Push | `db_explorer.py`, `queries.py`, `db_utils.py`, `output/*.csv`, `day02_log.md` |

**Create your branch now:**
```bash
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-01/day-02-schema-queries
```

---

## 📚 BACKGROUND

The DVD Rental database (Pagila) models a video rental store chain. As a DBA you can
probably read the schema in your sleep — but today we approach it differently: **through
Python eyes**. Every query you'd normally write in pgAdmin or psql, you'll write as
a reusable Python function that handles connections, errors, and logging cleanly.

### Why this matters for a Data Engineer:
- Pipelines don't have a human clicking "Run" — Python code IS the query executor
- Connection hygiene (open → use → close) is your responsibility in code
- Query results must be converted to data structures (lists, dicts, DataFrames) for
  downstream processing
- Error handling around DB calls prevents silent data loss in pipelines

### DVD Rental ERD (Key Relationships)
```
language ──< film >──< film_actor >──< actor
                │
                └──< inventory >──< rental >──< customer
                         │               │
                       store          payment
                         │
                        staff
```

**Core tables you'll work with today:**
| Table       | Rows   | Description                      |
|-------------|--------|----------------------------------|
| film        | 1,000  | Film catalogue with rental rates |
| actor       | 200    | Cast members                     |
| customer    | 599    | Registered customers             |
| rental      | 16,044 | Individual rental transactions   |
| payment     | 14,596 | Payment records                  |
| inventory   | 4,581  | Physical copies per store        |
| film_actor  | 5,462  | Film ↔ Actor junction            |
| category    | 16     | Film genres                      |
| store       | 2      | Store locations                  |
| address     | 603    | Addresses for customers/staff    |

---

## 🎯 OBJECTIVES

1. Build a reusable `db_utils.py` module (connection factory — used for ALL 90 days)
2. Explore DVD Rental schema programmatically from Python
3. Write 6 progressively complex SQL queries as Python functions
4. Export results to CSV files
5. Handle DB errors and resource cleanup correctly
6. Log and push to git

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                   |
|-------|----------|--------------------------------------------|
| A     | 20 min   | Branch setup + db_utils.py module          |
| B     | 20 min   | db_explorer.py — schema introspection      |
| C     | 45 min   | queries.py — 6 analytical queries          |
| D     | 15 min   | CSV export + output review                 |
| E     | 20 min   | Log + git push                             |

---

## 📝 EXERCISES

---

### EXERCISE 1 — db_utils.py: Reusable Connection Module (Block A)
**[Full steps provided — this module is used every day for 90 days]**

**Background:** Every Python script that talks to the database needs a connection.
Writing `psycopg2.connect(...)` inline in every file is a maintenance nightmare —
if the password changes, you update 30 files. A single `db_utils.py` module gives
you one place to manage connections, pooling, and error handling.

Create `sprint-01/day-02/db_utils.py`:

```python
#!/usr/bin/env python3
"""
db_utils.py — Reusable Database Utilities
==========================================
Centralised connection factory for the entire 90-day program.
Import this module in every script that needs a DB connection.

Usage:
    from db_utils import get_connection, get_engine, execute_query

Design decisions:
    - Always use context managers (with) → guaranteed connection cleanup
    - Passwords loaded from .env → never hardcoded
    - SQLAlchemy engine singleton → avoids creating multiple pools
    - All public functions have type hints + docstrings
"""

import os
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator
from urllib.parse import quote_plus

import psycopg2
import psycopg2.extras          # for RealDictCursor
from psycopg2 import pool as pg_pool
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ── Load .env from project root ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    # Walk up from this file to find .env
    _here = Path(__file__).resolve().parent
    for _candidate in [_here, _here.parent, _here.parent.parent]:
        _env = _candidate / ".env"
        if _env.is_file():
            load_dotenv(dotenv_path=_env, override=False)
            break
except ImportError:
    pass  # dotenv optional; os.environ used directly

# ── Logger ────────────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)

# ── DB config from environment ────────────────────────────────────────────────
def _db_config() -> dict[str, Any]:
    """Read DB settings from environment. Raises if required vars missing."""
    required = {"DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"}
    missing = required - set(os.environ)
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}\n"
            f"Ensure .env is at project root and contains all DB_ vars."
        )
    return {
        "host":     os.environ["DB_HOST"],
        "port":     int(os.environ["DB_PORT"]),
        "dbname":   os.environ["DB_NAME"],
        "user":     os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }


# ── psycopg2 connection pool (module-level singleton) ─────────────────────────
_pool: pg_pool.ThreadedConnectionPool | None = None

def _get_pool() -> pg_pool.ThreadedConnectionPool:
    """Return the shared connection pool, creating it on first call."""
    global _pool
    if _pool is None or _pool.closed:
        cfg = _db_config()
        _pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=int(os.getenv("DB_POOL_SIZE", 5)),
            connect_timeout=10,
            **cfg,
        )
        log.debug("Connection pool created (maxconn=%s)", os.getenv("DB_POOL_SIZE", 5))
    return _pool


@contextmanager
def get_connection(
    autocommit: bool = False,
    cursor_factory=None,
) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager: yields a psycopg2 connection from the pool.
    Automatically commits on clean exit or rolls back on exception.
    Always returns connection to pool — no leaks.

    Args:
        autocommit:     If True, each statement commits immediately.
        cursor_factory: e.g. psycopg2.extras.RealDictCursor for dict rows.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ...")
    """
    pool = _get_pool()
    conn = pool.getconn()
    if cursor_factory:
        conn.cursor_factory = cursor_factory
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)          # ← critical: always return to pool


# ── SQLAlchemy engine (module-level singleton) ────────────────────────────────
_engine: Engine | None = None

def get_engine() -> Engine:
    """
    Return the shared SQLAlchemy engine (created once, reused).
    Uses pool_pre_ping to detect stale connections.
    """
    global _engine
    if _engine is None:
        cfg = _db_config()
        pwd = quote_plus(cfg["password"])   # encode @, !, etc.
        url = (
            f"postgresql+psycopg2://"
            f"{cfg['user']}:{pwd}"
            f"@{cfg['host']}:{cfg['port']}"
            f"/{cfg['dbname']}"
        )
        _engine = create_engine(
            url,
            pool_size=int(os.getenv("DB_POOL_SIZE", 5)),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 2)),
            pool_pre_ping=True,
            pool_recycle=int(os.getenv("DB_POOL_RECYCLE", 1800)),
            echo=False,
        )
        log.debug("SQLAlchemy engine created")
    return _engine


def dispose_engine() -> None:
    """Close all SQLAlchemy pool connections. Call at process exit."""
    global _engine
    if _engine:
        _engine.dispose()
        _engine = None
        log.debug("SQLAlchemy engine disposed")


def close_pool() -> None:
    """Close all psycopg2 pool connections. Call at process exit."""
    global _pool
    if _pool and not _pool.closed:
        _pool.closeall()
        _pool = None
        log.debug("Connection pool closed")


# ── Convenience helpers ───────────────────────────────────────────────────────
def execute_query(
    sql: str,
    params: tuple | dict | None = None,
    as_dict: bool = False,
) -> list[tuple] | list[dict]:
    """
    Execute a SELECT query and return all rows.

    Args:
        sql:      SQL string (use %s or %(name)s placeholders — NOT f-strings)
        params:   Query parameters (prevents SQL injection)
        as_dict:  If True, returns list of dicts instead of list of tuples

    Returns:
        List of rows as tuples (default) or dicts (as_dict=True)

    Example:
        rows = execute_query(
            "SELECT title, rental_rate FROM film WHERE rating = %s LIMIT 5",
            params=("PG",)
        )
    """
    factory = psycopg2.extras.RealDictCursor if as_dict else None
    with get_connection(cursor_factory=factory) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [dict(r) for r in rows] if as_dict else rows


def execute_scalar(sql: str, params: tuple | dict | None = None) -> Any:
    """Execute a query and return a single scalar value."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
    return result[0] if result else None
```

**✅ Checkpoint:** Import it with no errors:
```bash
cd sprint-01/day-02
python -c "import db_utils; print('db_utils OK')"
```

---

### EXERCISE 2 — db_explorer.py: Schema Introspection (Block B)
**[Full steps — schema exploration is foundational for all ETL work]**

**Background:** A data engineer's first job on any project is understanding the schema.
Rather than clicking through pgAdmin, we do this programmatically — the output becomes
documentation and a reference for future pipeline work.

Create `sprint-01/day-02/db_explorer.py`:

```python
#!/usr/bin/env python3
"""
db_explorer.py — DVD Rental Schema Introspector
================================================
Programmatically documents the dvdrental schema:
  - Table list with row counts
  - Column details (name, type, nullable, default)
  - Primary and foreign keys
  - Table sizes on disk

Run: python db_explorer.py
Output: sprint-01/day-02/output/schema_report.md
"""

import csv
import os
import sys
from pathlib import Path
from datetime import datetime

# Make db_utils importable from this directory
sys.path.insert(0, str(Path(__file__).parent))
from db_utils import execute_query, execute_scalar, close_pool, dispose_engine

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def get_tables() -> list[dict]:
    """Return all public tables with row counts and disk size."""
    sql = """
        SELECT
            t.tablename                                          AS table_name,
            pg_size_pretty(pg_total_relation_size(
                quote_ident(t.tablename)::regclass))             AS total_size,
            pg_size_pretty(pg_relation_size(
                quote_ident(t.tablename)::regclass))             AS table_size,
            (SELECT COUNT(*) FROM information_schema.columns c
             WHERE c.table_name = t.tablename
               AND c.table_schema = 'public')                    AS column_count,
            obj_description(
                quote_ident(t.tablename)::regclass, 'pg_class') AS description
        FROM pg_tables t
        WHERE t.schemaname = 'public'
        ORDER BY pg_total_relation_size(
            quote_ident(t.tablename)::regclass) DESC;
    """
    rows = execute_query(sql, as_dict=True)

    # Add exact row counts (pg_class.reltuples is an estimate)
    for row in rows:
        row["row_count"] = execute_scalar(
            f"SELECT COUNT(*) FROM {row['table_name']}"   # table name from pg_tables is safe
        )
    return rows


def get_columns(table_name: str) -> list[dict]:
    """Return column metadata for a given table."""
    sql = """
        SELECT
            c.ordinal_position                      AS pos,
            c.column_name,
            c.data_type,
            c.character_maximum_length              AS max_len,
            c.is_nullable,
            c.column_default,
            CASE WHEN pk.column_name IS NOT NULL
                 THEN 'YES' ELSE 'NO' END           AS is_primary_key
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT ku.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku
              ON tc.constraint_name = ku.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_name = %s
              AND tc.table_schema = 'public'
        ) pk ON c.column_name = pk.column_name
        WHERE c.table_name = %s
          AND c.table_schema = 'public'
        ORDER BY c.ordinal_position;
    """
    return execute_query(sql, params=(table_name, table_name), as_dict=True)


def get_foreign_keys() -> list[dict]:
    """Return all foreign key relationships across the schema."""
    sql = """
        SELECT
            tc.table_name        AS from_table,
            kcu.column_name      AS from_column,
            ccu.table_name       AS to_table,
            ccu.column_name      AS to_column,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema    = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema    = 'public'
        ORDER BY tc.table_name, kcu.column_name;
    """
    return execute_query(sql, as_dict=True)


def write_schema_report(tables: list[dict], fk_list: list[dict]) -> Path:
    """Write a markdown schema report."""
    report_path = OUTPUT_DIR / "schema_report.md"
    with open(report_path, "w") as f:
        f.write(f"# DVD Rental Schema Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Table summary
        f.write("## Tables\n\n")
        f.write("| Table | Rows | Columns | Size |\n")
        f.write("|-------|------|---------|------|\n")
        for t in tables:
            f.write(
                f"| {t['table_name']} | {t['row_count']:,} "
                f"| {t['column_count']} | {t['total_size']} |\n"
            )

        # Column details per table
        f.write("\n## Column Details\n\n")
        for t in tables:
            cols = get_columns(t["table_name"])
            f.write(f"### {t['table_name']}\n\n")
            f.write("| # | Column | Type | Nullable | PK |\n")
            f.write("|---|--------|------|----------|----|\n")
            for c in cols:
                f.write(
                    f"| {c['pos']} | {c['column_name']} | {c['data_type']} "
                    f"| {c['is_nullable']} | {c['is_primary_key']} |\n"
                )
            f.write("\n")

        # Foreign keys
        f.write("## Foreign Keys\n\n")
        f.write("| From Table | From Column | → | To Table | To Column |\n")
        f.write("|------------|-------------|---|----------|-----------|\n")
        for fk in fk_list:
            f.write(
                f"| {fk['from_table']} | {fk['from_column']} | → "
                f"| {fk['to_table']} | {fk['to_column']} |\n"
            )

    return report_path


def write_tables_csv(tables: list[dict]) -> Path:
    """Write table summary to CSV for verification."""
    csv_path = OUTPUT_DIR / "tables_summary.csv"
    if tables:
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=tables[0].keys())
            writer.writeheader()
            writer.writerows(tables)
    return csv_path


def main():
    print("\n📐 DVD Rental Schema Explorer")
    print("=" * 40)

    print("\n[1] Fetching table inventory...")
    tables = get_tables()
    print(f"    Found {len(tables)} tables")
    for t in tables:
        print(f"    {t['table_name']:<20} {t['row_count']:>7,} rows  {t['total_size']}")

    print("\n[2] Fetching foreign keys...")
    fk_list = get_foreign_keys()
    print(f"    Found {len(fk_list)} foreign key relationships")

    print("\n[3] Writing schema report...")
    report = write_schema_report(tables, fk_list)
    print(f"    → {report}")

    print("\n[4] Writing tables CSV...")
    csv_out = write_tables_csv(tables)
    print(f"    → {csv_out}")

    print("\n✅ Done. Review output/ folder.\n")

    # Clean up connections
    close_pool()
    dispose_engine()


if __name__ == "__main__":
    main()
```

**✅ Checkpoint:** `python db_explorer.py` prints all 15 tables with row counts and creates `output/schema_report.md`.

---

### EXERCISE 3 — queries.py: 6 Analytical Queries (Block C)
**[Full steps for Q1–Q3, hints only for Q4–Q6 — you write these yourself]**

**Background:** These are the kinds of queries a data engineer writes to validate data
pipelines. Each one tests a different SQL pattern you'll reuse in ETL work.

Create `sprint-01/day-02/queries.py`:

```python
#!/usr/bin/env python3
"""
queries.py — Day 02 Analytical Queries on DVD Rental DB
=========================================================
6 queries progressing from simple to multi-table joins.
Each function returns a list of dicts and writes a CSV.

Run: python queries.py
"""

import csv
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from db_utils import execute_query, close_pool, dispose_engine

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def write_csv(filename: str, rows: list[dict]) -> Path:
    """Write query results to a CSV file."""
    path = OUTPUT_DIR / filename
    if not rows:
        print(f"  ⚠️  No rows returned for {filename}")
        return path
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return path


# ── Q1: Simple SELECT with filter and ORDER BY ────────────────────────────────
def q1_top_rental_rate_films() -> list[dict]:
    """
    Q1: Top 10 most expensive films to rent, showing title,
        rating, rental_rate, and rental_duration.
    Concepts: SELECT, WHERE, ORDER BY, LIMIT
    """
    sql = """
        SELECT
            film_id,
            title,
            rating,
            rental_rate,
            rental_duration,
            length          AS duration_minutes
        FROM film
        WHERE rental_rate > 2.99
        ORDER BY rental_rate DESC, title ASC
        LIMIT 10;
    """
    return execute_query(sql, as_dict=True)


# ── Q2: Aggregation + GROUP BY ─────────────────────────────────────────────────
def q2_revenue_by_rating() -> list[dict]:
    """
    Q2: Total payment revenue grouped by film rating.
    Shows how different rating categories perform commercially.
    Concepts: JOIN, GROUP BY, SUM, ROUND, ORDER BY aggregate
    """
    sql = """
        SELECT
            f.rating,
            COUNT(DISTINCT f.film_id)           AS film_count,
            COUNT(r.rental_id)                  AS total_rentals,
            ROUND(SUM(p.amount)::numeric, 2)    AS total_revenue,
            ROUND(AVG(p.amount)::numeric, 4)    AS avg_payment
        FROM film f
        JOIN inventory i   ON f.film_id   = i.film_id
        JOIN rental r      ON i.inventory_id = r.inventory_id
        JOIN payment p     ON r.rental_id    = p.rental_id
        GROUP BY f.rating
        ORDER BY total_revenue DESC;
    """
    return execute_query(sql, as_dict=True)


# ── Q3: Multi-table JOIN with date functions ──────────────────────────────────
def q3_monthly_rental_trend() -> list[dict]:
    """
    Q3: Monthly rental counts and revenue for the full dataset.
    Reveals seasonality — useful for forecasting pipelines.
    Concepts: date_trunc, multi-join, time series aggregation
    """
    sql = """
        SELECT
            DATE_TRUNC('month', r.rental_date)::date   AS rental_month,
            COUNT(r.rental_id)                         AS rentals,
            COUNT(DISTINCT r.customer_id)              AS unique_customers,
            ROUND(SUM(p.amount)::numeric, 2)           AS monthly_revenue
        FROM rental r
        JOIN payment p ON r.rental_id = p.rental_id
        GROUP BY DATE_TRUNC('month', r.rental_date)
        ORDER BY rental_month;
    """
    return execute_query(sql, as_dict=True)


# ── Q4: HINT ONLY — write this yourself ──────────────────────────────────────
def q4_top_10_customers_by_spend() -> list[dict]:
    """
    Q4: Top 10 customers by total spend.
    Expected columns: customer_id, full_name, email, total_spend,
                      rental_count, avg_spend_per_rental

    HINTS:
      - JOIN: customer → rental → payment
      - Concatenate first_name + ' ' + last_name for full_name
      - Use SUM(amount) for total_spend
      - Use COUNT(rental_id) for rental_count
      - ROUND to 2 decimal places
      - ORDER BY total_spend DESC LIMIT 10
    """
    sql = """
        -- YOUR SQL HERE
        -- Replace this comment with the actual query
        SELECT 'Write Q4 SQL here' AS reminder;
    """
    return execute_query(sql, as_dict=True)


# ── Q5: HINT ONLY — write this yourself ──────────────────────────────────────
def q5_category_popularity() -> list[dict]:
    """
    Q5: Film category rental popularity + revenue.
    Expected columns: category_name, total_rentals, total_revenue,
                      avg_rental_rate, film_count

    HINTS:
      - Tables: category → film_category → film → inventory → rental → payment
      - GROUP BY category.name
      - ORDER BY total_rentals DESC
    """
    sql = """
        -- YOUR SQL HERE
        SELECT 'Write Q5 SQL here' AS reminder;
    """
    return execute_query(sql, as_dict=True)


# ── Q6: HINT ONLY — write this yourself ──────────────────────────────────────
def q6_overdue_rentals() -> list[dict]:
    """
    Q6: Rentals that were returned late (return_date > rental_date + rental_duration days).
    Expected columns: rental_id, customer_name, film_title, rental_date,
                      due_date, return_date, days_overdue

    HINTS:
      - A rental's due_date = rental_date + INTERVAL '(rental_duration) days'
      - Filter: return_date > due_date
      - EXTRACT(EPOCH FROM (return_date - due_date))/86400 gives days_overdue as float
      - Include only rentals where return_date IS NOT NULL
      - ORDER BY days_overdue DESC LIMIT 20
    """
    sql = """
        -- YOUR SQL HERE
        SELECT 'Write Q6 SQL here' AS reminder;
    """
    return execute_query(sql, as_dict=True)


# ── Runner ─────────────────────────────────────────────────────────────────────
def run_query(label: str, fn) -> list[dict]:
    """Execute a query function, print preview, write CSV."""
    print(f"\n{'─'*50}")
    print(f"  {label}")
    print(f"{'─'*50}")
    try:
        rows = fn()
        if rows and "reminder" in rows[0]:
            print(f"  ⏳ Not implemented yet — write the SQL in queries.py")
            return []
        print(f"  Rows returned: {len(rows)}")
        if rows:
            # Print first 3 rows as preview
            headers = list(rows[0].keys())
            print(f"  Columns: {', '.join(headers)}")
            for i, row in enumerate(rows[:3]):
                print(f"  [{i+1}] {dict(row)}")
        return rows
    except Exception as exc:
        print(f"  ❌ Error: {exc}")
        return []


def main():
    print("\n🔍 DVD Rental — Analytical Queries")
    print(f"   Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 52)

    queries = [
        ("Q1: Top 10 Films by Rental Rate",   q1_top_rental_rate_films,    "q1_top_rental_rate_films.csv"),
        ("Q2: Revenue by Film Rating",         q2_revenue_by_rating,         "q2_revenue_by_rating.csv"),
        ("Q3: Monthly Rental Trend",           q3_monthly_rental_trend,      "q3_monthly_rental_trend.csv"),
        ("Q4: Top 10 Customers by Spend",      q4_top_10_customers_by_spend, "q4_top_customers.csv"),
        ("Q5: Category Popularity + Revenue",  q5_category_popularity,       "q5_category_popularity.csv"),
        ("Q6: Overdue Rentals",                q6_overdue_rentals,           "q6_overdue_rentals.csv"),
    ]

    for label, fn, csv_name in queries:
        rows = run_query(label, fn)
        if rows:
            out = write_csv(csv_name, rows)
            print(f"  📄 CSV → {out.name}")

    print("\n" + "=" * 52)
    print("  All queries complete. Check output/ folder.")
    print("=" * 52 + "\n")

    close_pool()
    dispose_engine()


if __name__ == "__main__":
    main()
```

**Expected output for Q1 (verify correctness):**
```
Rows returned: 10
Columns: film_id, title, rating, rental_rate, rental_duration, duration_minutes
[1] {'film_id': ..., 'title': 'Academy Dinosaur', 'rating': 'PG', 'rental_rate': 4.99, ...}
```

**Expected output for Q2 (verify totals are sensible):**
```
Rows returned: 5
[1] {'rating': 'PG-13', 'film_count': ..., 'total_rentals': ..., 'total_revenue': Decimal('...'), ...}
```

**✅ Q4–Q6 self-check — your SQL is correct when:**
- Q4: First row total_spend > $150, customer has 30+ rentals
- Q5: 'Sports' or 'Animation' likely in top 3 by rentals
- Q6: days_overdue values are positive floats, all return_dates are NOT NULL

---

### EXERCISE 4 — Move .env to Project Root (Block D)
**[Fix from Day 01 feedback — do this now if not already done]**

```bash
# Windows
move C:\Users\Lenovo\python-de-journey\sprint-01\day-01\.env ^
     C:\Users\Lenovo\python-de-journey\.env

# Verify db_utils finds it
cd C:\Users\Lenovo\python-de-journey\sprint-01\day-02
python -c "import db_utils; print('db_utils loaded cleanly')"
```

---

### EXERCISE 5 — Git Push (Block E)

```bash
cd C:\Users\Lenovo\python-de-journey

# Stage day 02 files
git add sprint-01/day-02/

# Commit
git commit -m "[DAY-002] Schema explorer, db_utils module, 6 analytical queries + CSV output"

# Push branch
git push -u origin sprint-01/day-02-schema-queries

# Log progress
python scripts/daily_log.py --day 2 --sprint 1 --message "Schema introspection, db_utils module, Q1-Q3 complete, Q4-Q6 attempted" --status done
```

---

## ✅ DAY 02 COMPLETION CHECKLIST

| # | Task                                                  | Done? |
|---|-------------------------------------------------------|-------|
| 1 | .env moved to project root                            | [ ]   |
| 2 | `sprint-01/day-02/` folder created                    | [ ]   |
| 3 | `db_utils.py` created and imports cleanly             | [ ]   |
| 4 | `db_explorer.py` runs — 15 tables printed             | [ ]   |
| 5 | `output/schema_report.md` generated                   | [ ]   |
| 6 | Q1 runs and returns 10 rows                           | [ ]   |
| 7 | Q2 returns 5 rows (one per rating)                    | [ ]   |
| 8 | Q3 returns monthly trend data                         | [ ]   |
| 9 | Q4 written and tested (top customers)                 | [ ]   |
|10 | Q5 written and tested (category popularity)           | [ ]   |
|11 | Q6 written and tested (overdue rentals)               | [ ]   |
|12 | All 6 CSVs in `output/` folder                        | [ ]   |
|13 | `git push` to `sprint-01/day-02-schema-queries`       | [ ]   |

---

## 🔜 PREVIEW: DAY 03

**Topic:** Python libraries installation audit + first use of Pandas with DB data  
**What you'll do:** Load query results directly into Pandas DataFrames, explore `.info()`,
`.describe()`, `.value_counts()`, and write your first DataFrame back to a new PostgreSQL table.

---

## ⚠️ SELF-STUDY NOTE

If Q4–Q6 take more than 20 minutes combined, stop and push what you have.
Note which queries you struggled with — we'll revisit the patterns in Sprint 05.
The goal today is **correct connection hygiene** in `db_utils.py`,
not perfect SQL (you already know SQL as a DBA).

---

*Day 02 | Sprint 01 | EP-01 | TASK-002*
