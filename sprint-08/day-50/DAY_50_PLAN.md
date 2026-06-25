# 📅 DAY 50 — Sprint 08 | Self-Joins + Recursive CTEs + EXPLAIN ANALYZE
## Hard Interview SQL + Query Execution Plans + Index Creation

---

## 🔁 RETROSPECTIVE — Day 49

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Q1-Q4 window functions | ✅ Pass | Clean results |
| Q5 category revenue share | ✅ Pass | 71 rows — all categories |
| Q6 seller quartiles | ✅ Pass | 3,095 rows |
| Q7 moving average | ✅ Pass | |
| Q8 cohort analysis | ✅ Pass | 22 months |
| Revenue tail-off Sep-Oct | ℹ️ Note | Dataset boundary — filter to ≤ Aug 2018 |
| Q2 57 rows not 81 | ✅ Correct | Some states have <3 sellers |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-08/day-50-self-joins-explain
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-11: Advanced SQL & dbt |
| Story           | ST-50: Self-Joins + Recursive CTEs + EXPLAIN ANALYZE |
| Task ID         | TASK-050 |
| Sprint          | Sprint 08 (Days 49–56) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | sql, self-join, recursive-cte, explain-analyze, indexes, day-50 |
| Acceptance Criteria | 4 hard interview queries working; EXPLAIN ANALYZE output read and documented; 3 indexes created with measured speedup |

---

## 📚 BACKGROUND

### Self-Joins

```sql
-- A table joined to ITSELF — to compare rows within the same table
-- Classic use: "find customers who ordered in consecutive months"
-- Or: "find sellers whose revenue exceeded the category average"

SELECT a.seller_id, a.total_revenue, b.total_revenue AS state_avg
FROM analytics.seller_performance a
JOIN analytics.seller_performance b
    ON a.seller_state = b.seller_state    ← same table, same state
    AND a.seller_id != b.seller_id        ← different rows
WHERE a.total_revenue > b.total_revenue   ← compare within state
```

### Recursive CTEs

```sql
-- Used when data has hierarchical/chain structure
-- Example: product categories with parent/child relationships
-- Or: "for each month, what was revenue N months ago?"

WITH RECURSIVE date_series AS (
    -- Base case: starting point
    SELECT '2017-01-01'::date AS month_date
    UNION ALL
    -- Recursive case: add one month until done
    SELECT (month_date + INTERVAL '1 month')::date
    FROM date_series
    WHERE month_date < '2018-08-01'
)
SELECT month_date FROM date_series
```

### EXPLAIN ANALYZE

```sql
-- Shows HOW PostgreSQL executes your query and HOW LONG it takes
EXPLAIN ANALYZE
SELECT * FROM analytics.customer_ltv WHERE value_segment = 'Platinum';

-- Output:
-- Seq Scan on customer_ltv  (cost=0.00..2800.18 rows=21 width=...)
--                           (actual time=0.012..45.231 rows=21 loops=1)
--   Filter: (value_segment = 'Platinum')
-- Planning Time: 0.8 ms
-- Execution Time: 45.3 ms   ← THIS is what matters

-- After adding index:
-- Index Scan using idx_segment on customer_ltv
-- Execution Time: 0.2 ms    ← 220x faster
```

---

## 🎯 OBJECTIVES

1. Write 4 hard interview-style SQL queries (self-join + recursive CTE)
2. Run EXPLAIN ANALYZE on slow queries
3. Create 3 indexes on analytics tables
4. Measure before/after execution time
5. Document findings

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 10 min | Branch setup |
| B | 50 min | `advanced_sql.py` — 4 hard queries |
| C | 30 min | `query_optimizer.py` — EXPLAIN + indexes |
| D | 10 min | Document findings |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — advanced_sql.py (Block B)
**[Q1-Q2 provided. Q3-Q4 write yourself]**

Create `sprint-08/day-50/advanced_sql.py`:

```python
#!/usr/bin/env python3
"""
advanced_sql.py — Day 50 | Hard Interview SQL
==============================================
4 advanced SQL patterns: self-joins, recursive CTEs,
gap detection, and comparative analysis.

Run: python sprint-08/day-50/advanced_sql.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "capstone"))

from logger import get_pipeline_logger
from db import get_ecommerce_engine, dispose_ecommerce_engine

logger = get_pipeline_logger("advanced_sql")

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

engine = get_ecommerce_engine()


def run_query(name: str, sql: str) -> pd.DataFrame:
    df = pd.read_sql(sql, engine)
    logger.info(f"✅ {name}: {len(df):,} rows")
    df.to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
    return df


# ── Q1: Sellers Outperforming State Average (Self-Join) ──────────────────
# Interview question: "Find sellers whose revenue exceeds their state average"
Q1_SELLERS_ABOVE_STATE_AVG = """
WITH state_averages AS (
    SELECT
        seller_state,
        ROUND(AVG(total_revenue)::numeric, 2) AS state_avg_revenue,
        ROUND(AVG(avg_review_score)::numeric, 2) AS state_avg_review,
        COUNT(*) AS sellers_in_state
    FROM analytics.seller_performance
    GROUP BY seller_state
)
SELECT
    s.seller_id,
    s.seller_state,
    ROUND(s.total_revenue::numeric, 2)    AS seller_revenue,
    sa.state_avg_revenue,
    ROUND((s.total_revenue - sa.state_avg_revenue)::numeric, 2)
                                          AS revenue_above_avg,
    ROUND(((s.total_revenue - sa.state_avg_revenue)
           / NULLIF(sa.state_avg_revenue, 0) * 100)::numeric, 1)
                                          AS pct_above_avg,
    sa.sellers_in_state
FROM analytics.seller_performance s
JOIN state_averages sa ON s.seller_state = sa.seller_state
WHERE s.total_revenue > sa.state_avg_revenue
ORDER BY pct_above_avg DESC
LIMIT 50
"""


# ── Q2: Monthly Revenue Gap Detection (Recursive CTE) ────────────────────
# Interview question: "Find months where we had no orders" (gap detection)
Q2_MONTHLY_GAPS = """
WITH RECURSIVE month_series AS (
    SELECT '2017-01-01'::date AS month_date
    UNION ALL
    SELECT (month_date + INTERVAL '1 month')::date
    FROM month_series
    WHERE month_date < '2018-08-01'
),
actual_months AS (
    SELECT order_month, total_revenue, total_orders
    FROM analytics.monthly_revenue
    WHERE order_month BETWEEN '2017-01-01' AND '2018-08-01'
)
SELECT
    ms.month_date                          AS expected_month,
    am.total_revenue,
    am.total_orders,
    CASE
        WHEN am.order_month IS NULL THEN 'MISSING ⚠️'
        WHEN am.total_orders < 1000 THEN 'LOW VOLUME ⬇️'
        ELSE 'NORMAL ✅'
    END                                    AS status
FROM month_series ms
LEFT JOIN actual_months am ON ms.month_date = am.order_month
ORDER BY ms.month_date
"""


# ── Q3: Customers Who Improved Review Score Over Time (Self-Join) ─────────
# Interview question: "Find customers whose most recent order review
# was higher than their first order review"
# WRITE THIS YOURSELF
Q3_REVIEW_IMPROVEMENT = """
-- YOUR SQL HERE
-- HINTS:
-- Step 1: Get each customer's first and last order review scores
-- WITH customer_reviews AS (
--     SELECT
--         c.customer_unique_id,
--         o.order_id,
--         r.review_score,
--         o.order_purchase_timestamp::timestamp AS order_date,
--         ROW_NUMBER() OVER (
--             PARTITION BY c.customer_unique_id
--             ORDER BY o.order_purchase_timestamp ASC
--         ) AS order_seq_asc,
--         ROW_NUMBER() OVER (
--             PARTITION BY c.customer_unique_id
--             ORDER BY o.order_purchase_timestamp DESC
--         ) AS order_seq_desc
--     FROM raw.customers c
--     JOIN raw.orders o ON c.customer_id = o.customer_id
--     JOIN raw.order_reviews r ON o.order_id = r.order_id
--     WHERE r.review_score IS NOT NULL
--       AND o.order_status = 'delivered'
-- )
-- Step 2: Self-join first (seq=1) with last (seq_desc=1)
-- SELECT
--     first_order.customer_unique_id,
--     first_order.review_score AS first_review,
--     last_order.review_score  AS last_review,
--     (last_order.review_score - first_order.review_score) AS improvement
-- FROM customer_reviews first_order
-- JOIN customer_reviews last_order
--     ON first_order.customer_unique_id = last_order.customer_unique_id
--     AND first_order.order_seq_asc = 1
--     AND last_order.order_seq_desc = 1
--     AND first_order.order_id != last_order.order_id  ← different orders
-- WHERE last_order.review_score > first_order.review_score
-- ORDER BY improvement DESC
--
-- Expected: customers who bought multiple times and gave a higher review last time
SELECT 'implement Q3' AS placeholder
"""


# ── Q4: Product Category Revenue Trend (Recursive + Window) ──────────────
# Interview question: "For top 5 categories, show quarterly revenue trend"
# WRITE THIS YOURSELF
Q4_CATEGORY_QUARTERLY_TREND = """
-- YOUR SQL HERE
-- HINTS:
-- Step 1: Get top 5 categories by total revenue
-- WITH top_categories AS (
--     SELECT product_category_english
--     FROM analytics.product_analytics
--     ORDER BY total_revenue DESC
--     LIMIT 5
-- ),
-- Step 2: Get quarterly revenue per category from raw data
-- quarterly_revenue AS (
--     SELECT
--         t.product_category_name_english AS category,
--         DATE_TRUNC('quarter', o.order_purchase_timestamp::timestamp)::date AS quarter,
--         ROUND(SUM(oi.price)::numeric, 2) AS quarterly_revenue
--     FROM raw.order_items oi
--     JOIN raw.orders o ON oi.order_id = o.order_id
--     JOIN raw.products p ON oi.product_id = p.product_id
--     JOIN raw.product_category_translation t
--         ON p.product_category_name = t.product_category_name
--     WHERE t.product_category_name_english IN (
--         SELECT product_category_english FROM top_categories
--     )
--     AND o.order_status = 'delivered'
--     GROUP BY category, quarter
-- )
-- Step 3: Add LAG to show quarter-over-quarter change
-- SELECT
--     category, quarter, quarterly_revenue,
--     LAG(quarterly_revenue) OVER (
--         PARTITION BY category ORDER BY quarter
--     ) AS prev_quarter_revenue,
--     ROUND((quarterly_revenue - LAG(quarterly_revenue) OVER (
--         PARTITION BY category ORDER BY quarter
--     ))::numeric, 2) AS qoq_change
-- FROM quarterly_revenue
-- ORDER BY category, quarter
--
-- Expected: ~5 categories × 6-7 quarters = ~30-35 rows
SELECT 'implement Q4' AS placeholder
"""


def main() -> None:
    logger.info("=" * 52)
    logger.info("Advanced SQL — Day 50")
    logger.info("=" * 52)

    queries = [
        ("q1_sellers_above_state_avg",    Q1_SELLERS_ABOVE_STATE_AVG),
        ("q2_monthly_gaps",               Q2_MONTHLY_GAPS),
        ("q3_review_improvement",         Q3_REVIEW_IMPROVEMENT),
        ("q4_category_quarterly_trend",   Q4_CATEGORY_QUARTERLY_TREND),
    ]

    for name, sql in queries:
        try:
            run_query(name, sql)
        except Exception as exc:
            logger.error(f"❌ {name}: {exc}")

    dispose_ecommerce_engine()


if __name__ == "__main__":
    main()
```

---

### EXERCISE 2 — query_optimizer.py (Block C)
**[Fully provided — study EXPLAIN ANALYZE output carefully]**

Create `sprint-08/day-50/query_optimizer.py`:

```python
#!/usr/bin/env python3
"""
query_optimizer.py — Day 50 | EXPLAIN ANALYZE + Index Creation
===============================================================
Measures query performance before and after adding indexes.
Documents the execution plan for slow queries.

Run: python sprint-08/day-50/query_optimizer.py
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

logger = get_pipeline_logger("query_optimizer")
engine = get_ecommerce_engine()


def explain_query(label: str, sql: str) -> str:
    """Run EXPLAIN ANALYZE and return the plan as a string."""
    with engine.connect() as conn:
        result = conn.execute(text(f"EXPLAIN ANALYZE {sql}"))
        plan = "\n".join(row[0] for row in result)
    logger.info(f"\n── EXPLAIN: {label} ─────────────────────────")
    for line in plan.split("\n")[-3:]:   # show last 3 lines (timing)
        logger.info(f"  {line}")
    return plan


def time_query(sql: str, n: int = 3) -> float:
    """Run query n times, return median execution time in ms."""
    times = []
    for _ in range(n):
        start = time.perf_counter()
        with engine.connect() as conn:
            conn.execute(text(sql)).fetchall()
        times.append((time.perf_counter() - start) * 1000)
    times.sort()
    return times[len(times) // 2]


def create_index(index_name: str, ddl: str) -> None:
    """Create an index, skip if already exists."""
    with engine.connect() as conn:
        try:
            conn.execute(text(ddl))
            conn.commit()
            logger.info(f"Created index: {index_name}")
        except Exception as exc:
            if "already exists" in str(exc):
                logger.info(f"Index already exists: {index_name}")
            else:
                logger.error(f"Index creation failed: {exc}")


def main() -> None:
    logger.info("=" * 52)
    logger.info("Query Optimiser — Day 50")
    logger.info("=" * 52)

    # ── Query 1: Filter by segment ────────────────────────────────────────
    Q_SEGMENT = "SELECT * FROM analytics.customer_ltv WHERE value_segment = 'Platinum'"

    logger.info("\n── Q1: Filter by value_segment ──────────────")
    t_before = time_query(Q_SEGMENT)
    explain_query("Before index — segment filter", Q_SEGMENT)
    logger.info(f"Time BEFORE index: {t_before:.1f}ms")

    create_index(
        "idx_customer_ltv_segment",
        "CREATE INDEX IF NOT EXISTS idx_customer_ltv_segment "
        "ON analytics.customer_ltv (value_segment)"
    )

    t_after = time_query(Q_SEGMENT)
    logger.info(f"Time AFTER index:  {t_after:.1f}ms")
    logger.info(f"Speedup: {t_before/max(t_after, 0.1):.1f}x")

    # ── Query 2: Filter sellers by state ──────────────────────────────────
    Q_SELLER_STATE = """
        SELECT * FROM analytics.seller_performance
        WHERE seller_state = 'SP' ORDER BY total_revenue DESC
    """

    logger.info("\n── Q2: Filter sellers by state ──────────────")
    t_before = time_query(Q_SELLER_STATE)
    explain_query("Before index — seller state filter", Q_SELLER_STATE)
    logger.info(f"Time BEFORE index: {t_before:.1f}ms")

    create_index(
        "idx_seller_performance_state",
        "CREATE INDEX IF NOT EXISTS idx_seller_performance_state "
        "ON analytics.seller_performance (seller_state)"
    )

    t_after = time_query(Q_SELLER_STATE)
    logger.info(f"Time AFTER index:  {t_after:.1f}ms")
    logger.info(f"Speedup: {t_before/max(t_after, 0.1):.1f}x")

    # ── Query 3: Order metrics by delivery flag ────────────────────────────
    Q_LATE_ORDERS = """
        SELECT * FROM analytics.order_metrics
        WHERE is_late = 1 ORDER BY delivery_days_actual DESC
    """

    logger.info("\n── Q3: Filter orders by is_late ─────────────")
    t_before = time_query(Q_LATE_ORDERS)
    explain_query("Before index — is_late filter", Q_LATE_ORDERS)
    logger.info(f"Time BEFORE index: {t_before:.1f}ms")

    create_index(
        "idx_order_metrics_is_late",
        "CREATE INDEX IF NOT EXISTS idx_order_metrics_is_late "
        "ON analytics.order_metrics (is_late)"
    )

    t_after = time_query(Q_LATE_ORDERS)
    logger.info(f"Time AFTER index:  {t_after:.1f}ms")
    logger.info(f"Speedup: {t_before/max(t_after, 0.1):.1f}x")

    # ── Summary ───────────────────────────────────────────────────────────
    logger.info("\n── Index Summary ────────────────────────────")
    with engine.connect() as conn:
        indexes = conn.execute(text("""
            SELECT indexname, tablename, indexdef
            FROM pg_indexes
            WHERE schemaname = 'analytics'
            ORDER BY tablename, indexname
        """)).fetchall()

    for idx in indexes:
        logger.info(f"  {idx[1]}.{idx[0]}")

    dispose_ecommerce_engine()
    logger.info("\n✅ Query optimisation complete")


if __name__ == "__main__":
    main()
```

---

### EXERCISE 3 — Git Push

```bash
python scripts/daily_commit.py --day 50 --sprint 8 ^
    --message "Advanced SQL: self-joins, recursive CTEs, EXPLAIN ANALYZE, 3 indexes created" ^
    --merge
```

---

## ✅ DAY 50 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | Q1/Q2 run correctly | [ ] |
| 2 | **Q3: Review improvement via self-join** | [ ] |
| 3 | **Q4: Category quarterly trend with LAG** | [ ] |
| 4 | `query_optimizer.py` runs — 3 indexes created | [ ] |
| 5 | Before/after timing logged for each index | [ ] |
| 6 | EXPLAIN ANALYZE output shows Seq Scan → Index Scan | [ ] |
| 7 | One clean `[DAY-050][S08]` commit | [ ] |

---

## 🔍 EXPECTED OUTPUT

```
Q1 sellers above state avg: 50 rows (LIMIT 50)
Q2 monthly gaps: 20 rows (Jan 2017 - Aug 2018)
Q3 review improvement: varies (~500-2000 repeat customers with higher score)
Q4 category quarterly: ~30-35 rows (5 categories × 6-7 quarters)

Index speedup (typical):
  value_segment filter: 5-20x faster (small result set benefits most)
  seller_state filter:  2-10x faster
  is_late filter:       2-8x faster
```

---

## 🔜 PREVIEW: DAY 51

**Topic:** dbt (data build tool) setup  
**What you'll do:** Install dbt-postgres, initialise a dbt project on ecommerce_db,
convert `analytics_etl.py` SQL into dbt models, run `dbt run` and `dbt test`.
This is the industry-standard way to manage SQL transformations.

---

*Day 50 | Sprint 08 | EP-11 | TASK-050*
