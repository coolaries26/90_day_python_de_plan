# 📅 DAY 54 — Sprint 08 | Query Optimisation + Indexes + Partitioning
## Composite Indexes, EXPLAIN ANALYZE Deep Dive, Partition by Month

---

## 🔁 RETROSPECTIVE — Day 53

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| mart_order_metrics incremental | ✅ Pass | |
| Incremental WHERE clause | ✅ Pass | |
| orders_snapshot created | ✅ Pass | |
| q2 status changes = 0 rows | ✅ Expected | Static dataset |
| q3 summary = 1 row | ✅ Pass | |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-08/day-54-query-optimisation
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-11: Advanced SQL & dbt |
| Story           | ST-54: Query Optimisation + Partitioning |
| Task ID         | TASK-054 |
| Sprint          | Sprint 08 (Days 49–56) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | sql, indexes, partitioning, explain-analyze, optimisation, day-54 |
| Acceptance Criteria | 3 composite indexes created with measured speedup; mart_order_metrics_partitioned created and verified; EXPLAIN ANALYZE documented |

---

## 📚 BACKGROUND

### Composite Indexes

```sql
-- Single column index (Day 50):
CREATE INDEX idx_segment ON customer_ltv (value_segment);
-- Helps: WHERE value_segment = 'Platinum'

-- Composite index (two columns):
CREATE INDEX idx_state_revenue ON seller_performance (seller_state, total_revenue DESC);
-- Helps: WHERE seller_state = 'SP' ORDER BY total_revenue DESC
-- NOT helped: WHERE total_revenue > 1000 (leading column must be used)

-- Rule: put the column you filter on FIRST,
--       put the column you sort on SECOND
```

### Table Partitioning

```sql
-- Partition = split a large table into smaller physical chunks
-- Each partition = one month, year, region, etc.
-- Query on partitioned table → PostgreSQL only scans relevant partitions

-- Example: query for May 2018 orders
-- Without partition: scan ALL 96k rows
-- With partition:    scan only May 2018 partition (~8k rows)

-- Creating a partitioned table:
CREATE TABLE mart_order_metrics_partitioned (
    order_id TEXT,
    purchase_month DATE,
    ...
) PARTITION BY RANGE (purchase_month);

-- Create one partition per month:
CREATE TABLE mart_order_metrics_2017_01
    PARTITION OF mart_order_metrics_partitioned
    FOR VALUES FROM ('2017-01-01') TO ('2017-02-01');
```

### Reading EXPLAIN ANALYZE

```
Seq Scan on customer_ltv  (cost=0.00..2800 rows=96218 width=200)
                          (actual time=0.05..45.2 rows=96218 loops=1)
  Filter: (value_segment = 'Platinum')
  Rows Removed by Filter: 96197
Planning Time: 0.8 ms
Execution Time: 45.3 ms

Key things to read:
  cost=X..Y    → estimated cost (X=startup, Y=total)
  rows=N       → estimated rows returned
  actual time  → real execution time in ms
  Rows Removed → how many rows were scanned but discarded
  Execution Time → TOTAL query time — this is your benchmark
```

---

## 🎯 OBJECTIVES

1. Create 3 composite indexes and measure speedup
2. Read and document EXPLAIN ANALYZE output
3. Create `mart_order_metrics_partitioned` table by month
4. Compare query times on partitioned vs non-partitioned table
5. Write `optimisation_report.md`
6. Push clean `[DAY-054][S08]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 10 min | Branch setup |
| B | 40 min | `composite_indexes.py` — 3 indexes + timing |
| C | 40 min | `partitioning.py` — partition table + compare |
| D | 20 min | `optimisation_report.md` |
| E | 10 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — composite_indexes.py (Block B)
**[Index 1 provided. Indexes 2 + 3 write yourself]**

Create `sprint-08/day-54/composite_indexes.py`:

```python
#!/usr/bin/env python3
"""
composite_indexes.py — Day 54 | Composite Index Analysis
=========================================================
Creates 3 composite indexes and measures speedup via EXPLAIN ANALYZE.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "capstone"))

from logger import get_pipeline_logger
from db import get_ecommerce_engine, dispose_ecommerce_engine

logger = get_pipeline_logger("composite_indexes")
engine = get_ecommerce_engine()
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def time_query(sql: str, n: int = 5) -> float:
    """Run query n times, return median time in ms."""
    times = []
    for _ in range(n):
        start = time.perf_counter()
        with engine.connect() as conn:
            conn.execute(text(sql)).fetchall()
        times.append((time.perf_counter() - start) * 1000)
    times.sort()
    return round(times[len(times) // 2], 2)


def get_explain(sql: str) -> str:
    """Get EXPLAIN ANALYZE output."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"EXPLAIN ANALYZE {sql}")).fetchall()
    return "\n".join(r[0] for r in rows)


def create_index(name: str, ddl: str) -> None:
    with engine.connect() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {name}"))
        conn.execute(text(ddl))
        conn.commit()
    logger.info(f"Created: {name}")


def drop_index(name: str) -> None:
    with engine.connect() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {name}"))
        conn.commit()


def benchmark(label: str, sql: str, index_name: str, index_ddl: str) -> dict:
    """Benchmark a query before and after creating an index."""
    # Before
    drop_index(index_name)
    t_before = time_query(sql)
    plan_before = get_explain(sql)
    scan_before = "Index Scan" if "Index Scan" in plan_before else "Seq Scan"

    # Create index
    create_index(index_name, index_ddl)

    # After
    t_after = time_query(sql)
    plan_after = get_explain(sql)
    scan_after = "Index Scan" if "Index Scan" in plan_after else "Seq Scan"

    speedup = round(t_before / max(t_after, 0.1), 1)
    logger.info(f"\n── {label} ───────────────────────────────")
    logger.info(f"  Before: {t_before}ms ({scan_before})")
    logger.info(f"  After:  {t_after}ms ({scan_after})")
    logger.info(f"  Speedup: {speedup}x")

    return {
        "label": label,
        "index": index_name,
        "query_ms_before": t_before,
        "query_ms_after": t_after,
        "speedup": speedup,
        "scan_before": scan_before,
        "scan_after": scan_after,
    }


# ── Index 1: Provided ─────────────────────────────────────────────────────
def index_1_state_revenue() -> dict:
    """
    Composite index: seller_state + total_revenue DESC
    Query: top sellers in a state
    """
    return benchmark(
        label="Seller: state + revenue",
        sql="""
            SELECT seller_id, seller_state, total_revenue
            FROM analytics.seller_performance
            WHERE seller_state = 'SP'
            ORDER BY total_revenue DESC
            LIMIT 10
        """,
        index_name="idx_seller_state_revenue",
        index_ddl="""
            CREATE INDEX idx_seller_state_revenue
            ON analytics.seller_performance (seller_state, total_revenue DESC)
        """,
    )


# ── Index 2: Write yourself ────────────────────────────────────────────────
def index_2_order_month_late() -> dict:
    """
    YOUR TASK: Composite index on mart_order_metrics
    Common query pattern: filter by purchase_month AND is_late

    HINTS:
    Query to optimise:
        SELECT order_id, delivery_days, review_score
        FROM dbt_dev_marts.mart_order_metrics
        WHERE purchase_month = '2018-01-01'   -- or purchased_at date_trunc
          AND is_late = true
        ORDER BY delivery_days DESC

    Index to create:
        CREATE INDEX idx_order_month_late
        ON dbt_dev_marts.mart_order_metrics (purchase_month, is_late)
        -- NOTE: if purchase_month column doesn't exist, use:
        -- DATE_TRUNC('month', purchased_at) — but you can't index expressions
        -- without a function index, so use purchased_at with a range instead:
        -- WHERE purchased_at >= '2018-01-01' AND purchased_at < '2018-02-01'

    Use benchmark() function — see index_1 for the pattern
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement index_2_order_month_late")


# ── Index 3: Write yourself ────────────────────────────────────────────────
def index_3_customer_segment_spend() -> dict:
    """
    YOUR TASK: Composite index on mart_customer_ltv
    Common query pattern: filter by value_segment, sort by total_spent

    HINTS:
    Query to optimise:
        SELECT customer_unique_id, total_spent, avg_review_score
        FROM dbt_dev_marts.mart_customer_ltv
        WHERE value_segment = 'Gold'
        ORDER BY total_spent DESC
        LIMIT 50

    Index to create:
        CREATE INDEX idx_ltv_segment_spend
        ON dbt_dev_marts.mart_customer_ltv (value_segment, total_spent DESC)

    Use benchmark() function
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement index_3_customer_segment_spend")


def main() -> None:
    logger.info("=" * 52)
    logger.info("Composite Indexes — Day 54")
    logger.info("=" * 52)

    results = []
    for fn in [index_1_state_revenue, index_2_order_month_late, index_3_customer_segment_spend]:
        try:
            result = fn()
            results.append(result)
        except NotImplementedError as e:
            logger.warning(f"⏳ {fn.__name__}: {e}")
        except Exception as e:
            logger.error(f"❌ {fn.__name__}: {e}")

    if results:
        df = pd.DataFrame(results)
        df.to_csv(OUTPUT_DIR / "index_benchmarks.csv", index=False)
        logger.info(f"\n── Summary ──────────────────────────────────")
        logger.info(f"\n{df[['label','query_ms_before','query_ms_after','speedup','scan_after']].to_string(index=False)}")

    dispose_ecommerce_engine()


if __name__ == "__main__":
    main()
```

---

### EXERCISE 2 — partitioning.py (Block C)
**[Fully provided — study the partitioning pattern]**

Create `sprint-08/day-54/partitioning.py`:

```python
#!/usr/bin/env python3
"""
partitioning.py — Day 54 | Table Partitioning
==============================================
Creates a partitioned version of mart_order_metrics
and compares query performance.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "capstone"))

from logger import get_pipeline_logger
from db import get_ecommerce_engine, dispose_ecommerce_engine

logger = get_pipeline_logger("partitioning")
engine = get_ecommerce_engine()


def create_partitioned_table() -> None:
    """Create partitioned version of mart_order_metrics."""
    with engine.connect() as conn:
        # Drop existing
        conn.execute(text("""
            DROP TABLE IF EXISTS analytics.mart_order_metrics_partitioned CASCADE
        """))

        # Create parent partitioned table
        conn.execute(text("""
            CREATE TABLE analytics.mart_order_metrics_partitioned (
                order_id        TEXT,
                purchase_month  DATE NOT NULL,
                order_status    TEXT,
                delivery_days   INTEGER,
                is_late         BOOLEAN,
                total_payment   NUMERIC,
                review_score    NUMERIC
            ) PARTITION BY RANGE (purchase_month)
        """))

        # Create monthly partitions (Jan 2017 - Aug 2018)
        months = [
            ('2017-01-01', '2017-02-01'), ('2017-02-01', '2017-03-01'),
            ('2017-03-01', '2017-04-01'), ('2017-04-01', '2017-05-01'),
            ('2017-05-01', '2017-06-01'), ('2017-06-01', '2017-07-01'),
            ('2017-07-01', '2017-08-01'), ('2017-08-01', '2017-09-01'),
            ('2017-09-01', '2017-10-01'), ('2017-10-01', '2017-11-01'),
            ('2017-11-01', '2017-12-01'), ('2017-12-01', '2018-01-01'),
            ('2018-01-01', '2018-02-01'), ('2018-02-01', '2018-03-01'),
            ('2018-03-01', '2018-04-01'), ('2018-04-01', '2018-05-01'),
            ('2018-05-01', '2018-06-01'), ('2018-06-01', '2018-07-01'),
            ('2018-07-01', '2018-08-01'), ('2018-08-01', '2018-09-01'),
        ]

        for start, end in months:
            partition_name = f"mart_order_metrics_{start[:7].replace('-', '_')}"
            conn.execute(text(f"""
                CREATE TABLE analytics.{partition_name}
                    PARTITION OF analytics.mart_order_metrics_partitioned
                    FOR VALUES FROM ('{start}') TO ('{end}')
            """))

        # Load data from dbt mart
        conn.execute(text("""
            INSERT INTO analytics.mart_order_metrics_partitioned
            SELECT
                order_id,
                DATE_TRUNC('month', purchased_at)::DATE AS purchase_month,
                order_status,
                delivery_days,
                is_late,
                total_payment,
                review_score
            FROM dbt_dev_marts.mart_order_metrics
            WHERE purchased_at IS NOT NULL
        """))
        conn.commit()

    logger.info("Partitioned table created with 20 monthly partitions")


def compare_queries() -> None:
    """Compare same query on partitioned vs non-partitioned table."""

    test_cases = [
        {
            "label":    "Single month filter",
            "regular":  """
                SELECT COUNT(*), AVG(review_score), SUM(total_payment)
                FROM dbt_dev_marts.mart_order_metrics
                WHERE purchased_at >= '2018-03-01'
                  AND purchased_at < '2018-04-01'
            """,
            "partitioned": """
                SELECT COUNT(*), AVG(review_score), SUM(total_payment)
                FROM analytics.mart_order_metrics_partitioned
                WHERE purchase_month = '2018-03-01'
            """,
        },
        {
            "label":    "Quarter filter",
            "regular":  """
                SELECT COUNT(*), AVG(delivery_days)
                FROM dbt_dev_marts.mart_order_metrics
                WHERE purchased_at >= '2018-01-01'
                  AND purchased_at < '2018-04-01'
                  AND is_late = true
            """,
            "partitioned": """
                SELECT COUNT(*), AVG(delivery_days)
                FROM analytics.mart_order_metrics_partitioned
                WHERE purchase_month >= '2018-01-01'
                  AND purchase_month < '2018-04-01'
                  AND is_late = true
            """,
        },
    ]

    results = []
    for tc in test_cases:
        times_regular = []
        times_partitioned = []

        for _ in range(5):
            with engine.connect() as conn:
                start = time.perf_counter()
                conn.execute(text(tc["regular"])).fetchall()
                times_regular.append((time.perf_counter() - start) * 1000)

                start = time.perf_counter()
                conn.execute(text(tc["partitioned"])).fetchall()
                times_partitioned.append((time.perf_counter() - start) * 1000)

        times_regular.sort()
        times_partitioned.sort()
        t_reg  = round(times_regular[2], 2)
        t_part = round(times_partitioned[2], 2)
        speedup = round(t_reg / max(t_part, 0.1), 1)

        logger.info(f"\n── {tc['label']} ──────────────────────────")
        logger.info(f"  Regular:     {t_reg}ms")
        logger.info(f"  Partitioned: {t_part}ms")
        logger.info(f"  Speedup:     {speedup}x")

        results.append({
            "query":        tc["label"],
            "regular_ms":   t_reg,
            "partitioned_ms": t_part,
            "speedup":      speedup,
        })

    df = pd.DataFrame(results)
    df.to_csv(Path(__file__).parent / "output" / "partition_benchmark.csv", index=False)
    logger.info(f"\n{df.to_string(index=False)}")


def main() -> None:
    logger.info("=" * 52)
    logger.info("Table Partitioning — Day 54")
    logger.info("=" * 52)

    Path(__file__).parent.joinpath("output").mkdir(exist_ok=True)
    create_partitioned_table()
    compare_queries()
    dispose_ecommerce_engine()
    logger.info("\n✅ Partitioning analysis complete")


if __name__ == "__main__":
    main()
```

---

### EXERCISE 3 — optimisation_report.md (Block D)
**[Write yourself — document your findings]**

Create `sprint-08/day-54/optimisation_report.md`:

```markdown
# Query Optimisation Report — Day 54

## Composite Index Results

| Index | Query | Before | After | Speedup | Scan Type |
|-------|-------|--------|-------|---------|-----------|
| idx_seller_state_revenue | state + revenue | Xms | Xms | Xx | → Index Scan |
| idx_order_month_late | month + is_late | Xms | Xms | Xx | → |
| idx_ltv_segment_spend | segment + spend | Xms | Xms | Xx | → |

## Partitioning Results

| Query | Regular | Partitioned | Speedup |
|-------|---------|-------------|---------|
| Single month | Xms | Xms | Xx |
| Quarter filter | Xms | Xms | Xx |

## Key Findings

1. [Your finding about when indexes help most]
2. [Your finding about composite index column order]
3. [Your finding about partitioning on small datasets]

## Recommendations

- Add composite index on [...] for the Streamlit dashboard
- Partition [...] table when data exceeds [...] rows
- Avoid indexing [...] because [...]
```

---

### EXERCISE 4 — Git Push

```bash
python scripts/daily_commit.py --day 54 --sprint 8 ^
    --message "Query optimisation: 3 composite indexes, partitioned table, benchmark report" ^
    --merge
```

---

## ✅ DAY 54 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | Index 1 (seller state+revenue) — provided | [ ] |
| 2 | **Index 2 (order month+is_late) written** | [ ] |
| 3 | **Index 3 (customer segment+spend) written** | [ ] |
| 4 | `index_benchmarks.csv` saved with before/after times | [ ] |
| 5 | `partitioning.py` runs — 20 partitions created | [ ] |
| 6 | `partition_benchmark.csv` saved | [ ] |
| 7 | **`optimisation_report.md` written** | [ ] |
| 8 | One clean `[DAY-054][S08]` commit | [ ] |

---

## 🔜 PREVIEW: DAY 55

**Topic:** dbt + Airflow + Streamlit integration  
**What you'll do:** Create an Airflow DAG that runs `dbt run` and `dbt test`
automatically after the e-commerce ETL completes. Update the Streamlit
dashboard to read from dbt mart tables instead of raw analytics tables.

---

*Day 54 | Sprint 08 | EP-11 | TASK-054*
