# 📅 DAY 16 — Sprint 03 | SQLAlchemy ORM + Alembic
## Define ORM Models + Run Your First Schema Migration

---

## 🔁 RETROSPECTIVE — Day 15

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| mypy etl_protocols.py | ✅ Pass | No issues |
| mypy oop_etl.py | ✅ Pass | No issues |
| mypy typed_utils.py | ✅ Pass | No issues |
| mypy.ini at project root | ✅ Pass | Config in place |
| Sprint number in commit | ⚠️ Minor | S02 → should be S03 from today |
| WinError 6 | ℹ️ Known | Windows/GitPython cosmetic — ignore |

### Branch Setup
```bash
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-03/day-16-sqlalchemy-orm
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-03: DB Connectivity & ORM                                 |
| Story           | ST-16: SQLAlchemy ORM Models + Alembic Migrations            |
| Task ID         | TASK-016                                                     |
| Sprint          | Sprint 03 (Days 15–21)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | sqlalchemy, orm, alembic, migrations, dvdrental, day-16      |
| Acceptance Criteria | ORM models for 5 tables; Alembic initialised; migration creates audit_log table; ORM query replaces raw SQL in one pipeline; pushed to git |

---

## 📚 BACKGROUND

### SQLAlchemy Two Layers — Know Which You Are Using

```
Layer 1: SQLAlchemy Core          Layer 2: SQLAlchemy ORM
─────────────────────────────     ──────────────────────────────────
SQL expression language           Python classes map to tables
text("SELECT * FROM film")        class Film(Base): film_id = Column(...)
Explicit SQL, full control        Python objects, less SQL visible
Used in: db_utils.py today        Used in: application code, migrations
```

You have been using Core (via `execute_query` and `pd.read_sql`).
Today you learn the ORM layer — required for Alembic migrations and
for writing queries as Python objects instead of SQL strings.

### ORM vs Raw SQL — When to Use Each

| Situation | Use |
|-----------|-----|
| Analytics / ETL bulk reads | `pd.read_sql()` + raw SQL — faster |
| CRUD operations on single rows | ORM — safer, readable |
| Schema migrations | Alembic ORM models — only option |
| Complex multi-table aggregations | Raw SQL — ORM generates poor SQL for these |
| Insert/Update single records | ORM Session — handles transactions cleanly |

### Alembic — Schema Migration Tool

```
Alembic is to SQLAlchemy what git is to files.
It tracks every schema change as a versioned migration script.

Without Alembic:            With Alembic:
ALTER TABLE ... (manual)    alembic revision --autogenerate
No history                  Full history in versions/ folder
Can't roll back             alembic downgrade -1
Team conflicts              Each migration has a unique revision ID
```

### Today's Target Schema Addition

```sql
-- New table you will create via Alembic migration:
CREATE TABLE etl_audit_log (
    id              SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    source_table    VARCHAR(100) NOT NULL,
    target_table    VARCHAR(100) NOT NULL,
    rows_loaded     INTEGER      NOT NULL DEFAULT 0,
    status          VARCHAR(20)  NOT NULL,
    elapsed_s       NUMERIC(8,3),
    error_message   TEXT,
    run_at          TIMESTAMP    NOT NULL DEFAULT NOW()
);
```

---

## 🎯 OBJECTIVES

1. Install Alembic (already in requirements.txt — verify)
2. Define ORM models for 5 DVD Rental tables
3. Initialise Alembic in the project
4. Write a migration that creates `etl_audit_log`
5. Run `alembic upgrade head` — verify table exists in PostgreSQL
6. Write an ORM query that replaces raw SQL in one function
7. Write audit log entries from `ETLResult` after each pipeline run
8. Push clean — one commit, `--sprint 3`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                        |
|-------|----------|-------------------------------------------------|
| A     | 15 min   | Branch setup + Alembic install + init           |
| B     | 30 min   | `models.py` — 5 ORM models                     |
| C     | 30 min   | Alembic migration — etl_audit_log table         |
| D     | 25 min   | `orm_queries.py` — ORM queries + audit writer   |
| E     | 20 min   | Commit + merge                                  |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Alembic Setup (Block A)
**[Full steps — Alembic init is done once per project]**

```bash
.venv\Scripts\activate

# Verify alembic is installed (was in requirements.txt from Day 01)
alembic --version
# If missing: pip install alembic==1.13.0

# Initialise Alembic at project root — creates alembic/ folder + alembic.ini
cd C:\Users\Lenovo\python-de-journey
alembic init alembic
```

**Edit `alembic.ini` — set the database URL:**
```ini
# Find this line in alembic.ini:
sqlalchemy.url = driver://user:pass@localhost/dbname

# Replace with (URL-encode the @ and ! in password):
sqlalchemy.url = postgresql+psycopg2://appuser:AppUser%402024%21@127.0.0.1:5432/dvdrental
```

**Edit `alembic/env.py` — point to your models:**
```python
# Find this section near the top of env.py:
# target_metadata = None

# Replace with:
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sprint-03" / "day-16"))
from models import Base          # your ORM models (created in Exercise 2)
target_metadata = Base.metadata  # tells Alembic about your table definitions
```

**✅ Checkpoint:**
```bash
alembic current
# Output: INFO  [alembic.runtime.migration] No upgrade history found — that's correct
```

---

### EXERCISE 2 — models.py: 5 ORM Models (Block B)
**[Film + Customer fully provided. Rental, Payment, AuditLog — write yourself with hints]**

Create `sprint-03/day-16/models.py`:

```python
#!/usr/bin/env python3
"""
models.py — Day 16 | SQLAlchemy ORM Models
===========================================
ORM representations of DVD Rental tables + new etl_audit_log table.
These models are used by:
  - Alembic (autogenerate detects schema changes)
  - orm_queries.py (query via Python objects)
  - ETL pipelines (write audit records after each run)

Relationship to actual DB:
  - Film, Customer, Rental, Payment → map to EXISTING tables (reflect schema)
  - AuditLog → NEW table (created by Alembic migration today)
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey,
    Integer, Numeric, String, Text, func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# ── Declarative Base — all models inherit from this ───────────────────────────
class Base(DeclarativeBase):
    """
    Modern SQLAlchemy 2.0 DeclarativeBase.
    All ORM models must inherit from this class.
    Alembic reads Base.metadata to understand the full schema.
    """
    pass


# ── Film model — fully provided ───────────────────────────────────────────────
class Film(Base):
    """ORM model for the 'film' table — maps existing DVD Rental table."""
    __tablename__ = "film"

    film_id:          Mapped[int]            = mapped_column(Integer, primary_key=True)
    title:            Mapped[str]            = mapped_column(String(255), nullable=False)
    description:      Mapped[str | None]     = mapped_column(Text)
    release_year:     Mapped[int | None]     = mapped_column(Integer)
    rental_duration:  Mapped[int]            = mapped_column(Integer, nullable=False, default=3)
    rental_rate:      Mapped[Decimal]        = mapped_column(Numeric(4, 2), nullable=False)
    length:           Mapped[int | None]     = mapped_column(Integer)
    replacement_cost: Mapped[Decimal]        = mapped_column(Numeric(5, 2), nullable=False)
    rating:           Mapped[str | None]     = mapped_column(String(10))

    def __repr__(self) -> str:
        return (f"Film(id={self.film_id}, title={self.title!r}, "
                f"rate={self.rental_rate}, rating={self.rating!r})")


# ── Customer model — fully provided ──────────────────────────────────────────
class Customer(Base):
    """ORM model for the 'customer' table."""
    __tablename__ = "customer"

    customer_id:  Mapped[int]        = mapped_column(Integer, primary_key=True)
    first_name:   Mapped[str]        = mapped_column(String(45), nullable=False)
    last_name:    Mapped[str]        = mapped_column(String(45), nullable=False)
    email:        Mapped[str | None] = mapped_column(String(50))
    active:       Mapped[bool]       = mapped_column(Boolean, nullable=False, default=True)
    create_date:  Mapped[datetime]   = mapped_column(DateTime, nullable=False)

    # Relationship — gives you customer.rentals as a list
    rentals: Mapped[list[Rental]] = relationship("Rental", back_populates="customer")

    @property
    def full_name(self) -> str:
        """Computed property — no column, derived from first + last."""
        return f"{self.first_name} {self.last_name}"

    def __repr__(self) -> str:
        return (f"Customer(id={self.customer_id}, "
                f"name={self.full_name!r}, active={self.active})")


# ── Rental model — WRITE THIS YOURSELF ───────────────────────────────────────
class Rental(Base):
    """
    Q1 — YOUR TASK: ORM model for the 'rental' table.

    HINTS — columns to map:
      rental_id     INTEGER PRIMARY KEY
      rental_date   TIMESTAMP NOT NULL
      inventory_id  INTEGER NOT NULL (FK → inventory.inventory_id)
      customer_id   INTEGER NOT NULL (FK → customer.customer_id)
      return_date   TIMESTAMP NULL        ← nullable — open rentals
      staff_id      INTEGER NOT NULL

    HINTS — relationships:
      customer: Mapped[Customer] = relationship("Customer", back_populates="rentals")

    HINTS — __repr__:
      Rental(id=X, customer_id=Y, rental_date=Z, returned=True/False)
      returned = self.return_date is not None
    """
    __tablename__ = "rental"
    # YOUR CODE HERE


# ── Payment model — WRITE THIS YOURSELF ──────────────────────────────────────
class Payment(Base):
    """
    Q2 — YOUR TASK: ORM model for the 'payment' table.

    HINTS — columns to map:
      payment_id    INTEGER PRIMARY KEY
      customer_id   INTEGER NOT NULL (FK → customer.customer_id)
      staff_id      INTEGER NOT NULL
      rental_id     INTEGER NULL (FK → rental.rental_id)
      amount        NUMERIC(5,2) NOT NULL
      payment_date  TIMESTAMP NOT NULL

    HINTS — __repr__:
      Payment(id=X, customer_id=Y, amount=Z)
    """
    __tablename__ = "payment"
    # YOUR CODE HERE


# ── AuditLog model — WRITE THIS YOURSELF ─────────────────────────────────────
class AuditLog(Base):
    """
    Q3 — YOUR TASK: ORM model for NEW 'etl_audit_log' table.
    This table does NOT exist yet — Alembic will create it.

    HINTS — columns to map (match the schema in the Background section):
      id             Integer, primary_key=True, autoincrement=True
      pipeline_name  String(100), nullable=False
      source_table   String(100), nullable=False
      target_table   String(100), nullable=False
      rows_loaded    Integer, nullable=False, default=0
      status         String(20), nullable=False
      elapsed_s      Numeric(8,3), nullable=True
      error_message  Text, nullable=True
      run_at         DateTime, nullable=False, server_default=func.now()
                     ↑ server_default means PostgreSQL fills it in, not Python

    HINTS — @classmethod from_result():
      Takes an ETLResult object, returns an AuditLog instance.
      This is the bridge between your Day 14 ETLResult and the DB table.

    HINTS — __repr__:
      AuditLog(id=X, pipeline=Y, status=Z, rows=N, run_at=T)
    """
    __tablename__ = "etl_audit_log"
    # YOUR CODE HERE

    @classmethod
    def from_result(cls, result: object) -> AuditLog:
        """
        Q3b — Create AuditLog from ETLResult.

        HINTS:
          - Import ETLResult at top of function to avoid circular import:
            from etl_protocols import ETLResult
          - Access: result.pipeline_name, result.source_table,
                    result.target_table, result.rows_loaded,
                    result.status, result.elapsed_seconds, result.error_message
        """
        # YOUR CODE HERE
        raise NotImplementedError("Implement AuditLog.from_result()")
```

**✅ Self-check for Rental:**
```python
python -c "
from models import Rental, Payment, AuditLog
print('Rental tablename:', Rental.__tablename__)
print('Payment tablename:', Payment.__tablename__)
print('AuditLog tablename:', AuditLog.__tablename__)
"
# Should print rental, payment, etl_audit_log
```

---

### EXERCISE 3 — Alembic Migration: etl_audit_log (Block C)
**[Full steps — migration workflow]**

**Step 1: Generate autogenerate migration**
```bash
cd C:\Users\Lenovo\python-de-journey

# Alembic compares Base.metadata (your models) against the live DB
# and generates a migration script for the differences
alembic revision --autogenerate -m "add etl_audit_log table"
```

This creates `alembic/versions/XXXX_add_etl_audit_log_table.py`.

**Step 2: Review the generated migration**
```bash
# Open the generated file and verify it contains:
# op.create_table('etl_audit_log', ...)
# DO NOT run upgrade yet — read it first
type alembic\versions\*add_etl_audit_log*.py
```

**Expected content structure:**
```python
def upgrade() -> None:
    op.create_table('etl_audit_log',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('pipeline_name', sa.String(length=100), nullable=False),
        # ... other columns ...
        sa.PrimaryKeyConstraint('id')
    )

def downgrade() -> None:
    op.drop_table('etl_audit_log')
```

**If Alembic also tries to create film/customer/rental/payment tables:**
That means it doesn't know those tables already exist. Fix by adding
`autoload_with` to those models OR add this to `env.py`:
```python
# In env.py, inside run_migrations_online(), before context.begin_transaction():
with connectable.connect() as connection:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        compare_type=True,
    )
```

**Step 3: Run the migration**
```bash
alembic upgrade head
# Expected:
# INFO  Running upgrade  -> XXXX, add etl_audit_log table
```

**Step 4: Verify in PostgreSQL**
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
```

**✅ Expected — 9 columns printed including `run_at` with `timestamp without time zone`**

---

### EXERCISE 4 — orm_queries.py: ORM Queries + Audit Writer (Block D)
**[Q1 fully provided. Q2 write yourself — hints given]**

Create `sprint-03/day-16/orm_queries.py`:

```python
#!/usr/bin/env python3
"""
orm_queries.py — Day 16 | ORM Queries + Audit Log Writer
=========================================================
Demonstrates:
  - SQLAlchemy ORM Session for queries
  - ORM query vs raw SQL — same result, different syntax
  - Writing ETLResult to etl_audit_log via AuditLog.from_result()

Run: python orm_queries.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day-14"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sqlalchemy.orm import Session
from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger
from models import Film, Customer, Rental, AuditLog
from etl_protocols import ETLResult

logger = get_pipeline_logger("orm_queries")
engine = get_engine()


# ── Q1: ORM queries — fully provided ─────────────────────────────────────────
def q1_top_films_by_rate(limit: int = 5) -> list[Film]:
    """
    ORM equivalent of:
      SELECT * FROM film WHERE rental_rate > 2.99
      ORDER BY rental_rate DESC LIMIT 5

    Key difference from raw SQL:
      - Returns Film objects, not tuples
      - film.title, film.rental_rate — named attributes, not index positions
      - Session manages connection lifecycle — no manual pool.putconn()
    """
    with Session(engine) as session:
        films = (
            session.query(Film)
            .filter(Film.rental_rate > 2.99)
            .order_by(Film.rental_rate.desc(), Film.title.asc())
            .limit(limit)
            .all()
        )
    # Session closes here — connection returned to pool automatically
    return films


def q2_active_customer_count() -> int:
    """ORM equivalent of: SELECT COUNT(*) FROM customer WHERE active = TRUE"""
    with Session(engine) as session:
        count: int = (
            session.query(Customer)
            .filter(Customer.active.is_(True))
            .count()
        )
    return count


# ── Q2: Write this yourself ───────────────────────────────────────────────────
def q3_customer_rental_count(limit: int = 10) -> list[tuple[str, int]]:
    """
    Q2 — YOUR TASK:
    ORM query — top customers by rental count.
    Expected return: list of (full_name, rental_count) tuples, sorted desc.

    HINTS:
      - Use Session(engine) context manager
      - Query Customer, join Rental via relationship or explicit join
      - Use func.count(Rental.rental_id) for counting
      - Group by Customer.customer_id, Customer.first_name, Customer.last_name
      - order_by count descending, limit to `limit` param
      - For full_name in Python (not SQL): use the @property
        OR use (Customer.first_name + " " + Customer.last_name).label("full_name")

    Expected top result:
      ('Eleanor Hunt', 45) or similar high-rental customer

    IMPORTANT: import func from sqlalchemy at top of this function:
      from sqlalchemy import func
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement q3_customer_rental_count")


# ── Audit log writer ──────────────────────────────────────────────────────────
def write_audit_log(result: ETLResult) -> None:
    """
    Write an ETLResult to etl_audit_log table via ORM.
    Called at end of every pipeline run from Day 16 onward.
    """
    audit = AuditLog.from_result(result)
    with Session(engine) as session:
        session.add(audit)
        session.commit()
        logger.info(f"Audit log written | pipeline={result.pipeline_name} "
                    f"status={result.status} rows={result.rows_loaded}")


def read_audit_log(limit: int = 5) -> list[AuditLog]:
    """Read recent audit log entries — verify writes are working."""
    with Session(engine) as session:
        entries = (
            session.query(AuditLog)
            .order_by(AuditLog.run_at.desc())
            .limit(limit)
            .all()
        )
        # Expunge — detach from session so objects can be used after session closes
        for entry in entries:
            session.expunge(entry)
    return entries


def main() -> None:
    logger.info("=" * 52)
    logger.info("ORM Queries Demo — Day 16")
    logger.info("=" * 52)

    # Q1 — top films
    logger.info("\n── Q1: Top 5 Films by Rental Rate ──────────")
    films = q1_top_films_by_rate(5)
    for film in films:
        logger.info(f"  {film!r}")

    # Q2 — active customers
    logger.info("\n── Q2: Active Customer Count ────────────────")
    count = q2_active_customer_count()
    logger.info(f"  Active customers: {count}")

    # Q3 — rental counts (your implementation)
    logger.info("\n── Q3: Top 10 Customers by Rental Count ────")
    try:
        rental_counts = q3_customer_rental_count(10)
        for name, cnt in rental_counts:
            logger.info(f"  {name:<25} {cnt} rentals")
    except NotImplementedError as e:
        logger.warning(f"  {e}")

    # Audit log demo — simulate a pipeline result
    logger.info("\n── Audit Log Write + Read ───────────────────")
    fake_result = ETLResult(
        pipeline_name="DemoETL",
        source_table="customer",
        target_table="analytics_customer_oop",
    )
    fake_result.complete(rows_extracted=599, rows_loaded=599, attempts=1)

    write_audit_log(fake_result)

    entries = read_audit_log(3)
    for entry in entries:
        logger.info(f"  {entry!r}")

    dispose_engine()
    logger.info("Done.")


if __name__ == "__main__":
    main()
```

---

### EXERCISE 5 — Git Push (Block E)

```bash
cd C:\Users\Lenovo\python-de-journey
git status

python scripts/daily_commit.py --day 16 --sprint 3 ^
    --message "SQLAlchemy ORM models (5 tables), Alembic init, etl_audit_log migration, ORM queries, audit writer" ^
    --merge
```

---

## ✅ DAY 16 COMPLETION CHECKLIST

| # | Task                                                                  | Done? |
|---|-----------------------------------------------------------------------|-------|
| 1 | Alembic initialised — `alembic/` folder + `alembic.ini` at root      | [ ]   |
| 2 | `alembic.ini` points to correct DB URL (password URL-encoded)        | [ ]   |
| 3 | `alembic/env.py` imports `Base` from `models.py`                     | [ ]   |
| 4 | `models.py` — `Film` and `Customer` models work                      | [ ]   |
| 5 | **`Rental` ORM model written by you**                                 | [ ]   |
| 6 | **`Payment` ORM model written by you**                                | [ ]   |
| 7 | **`AuditLog` ORM model written by you + `from_result()` implemented**| [ ]   |
| 8 | `alembic revision --autogenerate` creates migration with `etl_audit_log` | [ ]   |
| 9 | `alembic upgrade head` runs without error                             | [ ]   |
|10 | `etl_audit_log` table visible in PostgreSQL with 9 columns            | [ ]   |
|11 | `q1_top_films_by_rate()` returns 5 Film objects with __repr__        | [ ]   |
|12 | **`q3_customer_rental_count()` written by you — Eleanor Hunt at top** | [ ]   |
|13 | `write_audit_log()` inserts row into `etl_audit_log`                  | [ ]   |
|14 | `read_audit_log()` reads it back                                      | [ ]   |
|15 | One clean commit `[DAY-016][S03]` via `daily_commit.py --merge`       | [ ]   |

---

## ⚠️ WATCH OUT FOR

**Alembic detecting existing DVD Rental tables:**
If `alembic revision --autogenerate` tries to CREATE film/customer/rental/payment,
it means Alembic doesn't know those tables are already there. Fix:
```python
# In models.py, add __table_args__ to existing tables:
class Film(Base):
    __tablename__ = "film"
    __table_args__ = {"extend_existing": True}
```
Or simpler — in `env.py` tell Alembic to only include new tables:
```python
def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and reflected and compare_to is None:
        return False   # skip tables that exist in DB but not in models
    return True
# Then in context.configure(): include_object=include_object
```

**`session.expunge()` in `read_audit_log()`:**
Without `expunge()`, accessing ORM object attributes after the session closes
raises `DetachedInstanceError`. Always expunge before returning ORM objects
from a function if the session won't be passed along.

**alembic.ini password with special chars:**
`AppUser@2024!` → URL-encode → `AppUser%402024%21`
You already know this from the SQLAlchemy fix in verify_setup.py.

---

## 🔜 PREVIEW: DAY 17

**Topic:** Pandas depth — window functions, time series, `groupby` patterns  
**What you'll do:** Apply `df.groupby().transform()`, rolling windows,
`pd.Grouper` for time-based aggregations on the DVD Rental payment data.
Output feeds directly into `etl_audit_log` via your new ORM writer.

---

*Day 16 | Sprint 03 | EP-03 | TASK-016*
