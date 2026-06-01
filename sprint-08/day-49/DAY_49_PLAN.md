# 📅 DAY 49 — Sprint 08 | Advanced SQL
## Window Functions, CTEs, Query Optimisation on Real Data

---

## 🔁 SPRINT 07 CLOSE — Confirmed

```
sprint-07-complete tag: ✅
All 7 sprint tags:      ✅
Capstone: 550k rows, 5 analytics tables, 2 ML models, 5-page dashboard
```

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-08/day-49-window-functions
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-11: Advanced SQL & dbt |
| Story           | ST-49: Window Functions + CTEs |
| Task ID         | TASK-049 |
| Sprint          | Sprint 08 (Days 49–56) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | sql, window-functions, cte, analytics, day-49 |
| Acceptance Criteria | 8 window function queries running on ecommerce_db; results saved to CSV; query patterns documented |

---

## 📚 BACKGROUND

### Why Window Functions?

```
Window functions are the single most-tested SQL skill in DE interviews.
Every major tech company — Amazon, Meta, Uber, Airbnb — asks at least
one window function question.

What they do:
  Perform calculations across a set of rows RELATED to the current row,
  WITHOUT collapsing rows (unlike GROUP BY which loses row-level detail).

Classic interview questions this unlocks:
  "Find the top 3 sellers by revenue in each state"
  "Calculate running total of revenue by month"
  "Rank customers by spend within each segment"
  "Find customers whose spend increased vs previous order"
  "Calculate 7-day rolling average of daily orders"
```

### Syntax

```sql
function_name() OVER (
    PARTITION BY column     ← "group by" for window (optional)
    ORDER BY column         ← ordering within window
    ROWS BETWEEN ... AND ... ← frame (optional)
)

-- Common functions:
ROW_NUMBER()    → sequential rank (no ties)
RANK()          → rank with gaps for ties (1,1,3)
DENSE_RANK()    → rank without gaps (1,1,2)
LAG(col, n)     → value n rows BEFORE current
LEAD(col, n)    → value n rows AFTER current
SUM() OVER      → running total
AVG() OVER      → moving average
FIRST_VALUE()   → first value in window
NTILE(n)        → divide into n buckets
```

### CTEs — Common Table Expressions

```sql
-- Instead of nested subqueries (unreadable):
SELECT * FROM (
    SELECT * FROM (SELECT ...) x
) y

-- Use CTEs (readable, named, reusable):
WITH
customer_summary AS (
    SELECT customer_id, SUM(payment_value) AS total_spent
    FROM orders JOIN payments USING (order_id)
    GROUP BY customer_id
),
ranked AS (
    SELECT *,
           RANK() OVER (ORDER BY total_spent DESC) AS spend_rank
    FROM customer_summary
)
SELECT * FROM ranked WHERE spend_rank <= 10
```

---

## 🎯 OBJECTIVES

Write 8 advanced SQL queries using window functions and CTEs on the ecommerce_db.
Save results to CSV. Document each query's business purpose.

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 10 min | Branch setup |
| B | 60 min | `window_functions.py` — 8 queries |
| C | 20 min | Run + verify + save CSVs |
| D | 10 min | Query pattern notes |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — window_functions.py (Block B)
**[Q1-Q4 provided. Q5-Q8 write yourself]**

Create `sprint-08/day-49/window_functions.py`:

```python
#!/usr/bin/env python3
"""
window_functions.py — Day 49 | Advanced SQL Window Functions
=============================================================
8 window function queries on ecommerce_db.
Each query answers a specific business question.

Run: python sprint-08/day-49/window_functions.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "capstone"))

from logger import get_pipeline_logger
from db import get_ecommerce_engine, dispose_ecommerce_engine

logger = get_pipeline_logger("window_functions")

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

engine = get_ecommerce_engine()


def run_query(name: str, sql: str, save: bool = True) -> pd.DataFrame:
    """Execute SQL, log shape, optionally save to CSV."""
    df = pd.read_sql(sql, engine)
    logger.info(f"✅ {name}: {len(df):,} rows × {len(df.columns)} cols")
    if save:
        df.to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
    return df


# ── Q1: Running Revenue Total ─────────────────────────────────────────────
# Business: How did cumulative revenue grow month by month?
Q1_RUNNING_REVENUE = """
SELECT
    order_month,
    total_revenue,
    ROUND(SUM(total_revenue) OVER (
        ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )::numeric, 2)                                    AS cumulative_revenue,
    ROUND(AVG(total_revenue) OVER (
        ORDER BY order_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )::numeric, 2)                                    AS rolling_3mo_avg
FROM analytics.monthly_revenue
ORDER BY order_month
"""


# ── Q2: Seller Revenue Rank by State ────────────────────────────────────
# Business: Who are the top 3 sellers in each state?
# Classic interview question: "Top N per group"
Q2_SELLER_RANK_BY_STATE = """
WITH seller_ranked AS (
    SELECT
        seller_state,
        seller_id,
        ROUND(total_revenue::numeric, 2)              AS total_revenue,
        ROUND(avg_review_score::numeric, 2)           AS avg_review,
        RANK() OVER (
            PARTITION BY seller_state
            ORDER BY total_revenue DESC
        )                                             AS state_rank
    FROM analytics.seller_performance
    WHERE total_revenue > 0
)
SELECT *
FROM seller_ranked
WHERE state_rank <= 3
ORDER BY seller_state, state_rank
"""


# ── Q3: Customer Spend Percentile ────────────────────────────────────────
# Business: What percentile is each customer in? (for VIP identification)
Q3_CUSTOMER_PERCENTILE = """
SELECT
    customer_unique_id,
    value_segment,
    ROUND(total_spent::numeric, 2)                    AS total_spent,
    ROUND(avg_order_value::numeric, 2)                AS avg_order_value,
    NTILE(100) OVER (ORDER BY total_spent)            AS spend_percentile,
    ROUND(PERCENT_RANK() OVER (
        ORDER BY total_spent
    )::numeric * 100, 1)                              AS percent_rank,
    RANK() OVER (ORDER BY total_spent DESC)           AS overall_rank
FROM analytics.customer_ltv
ORDER BY total_spent DESC
LIMIT 100
"""


# ── Q4: Month-over-Month Revenue Change ──────────────────────────────────
# Business: Is revenue growing or declining? By how much?
Q4_REVENUE_MOM = """
SELECT
    order_month,
    ROUND(total_revenue::numeric, 2)                  AS revenue,
    LAG(total_revenue) OVER (ORDER BY order_month)    AS prev_month_revenue,
    ROUND((total_revenue - LAG(total_revenue) OVER (
        ORDER BY order_month
    ))::numeric, 2)                                   AS revenue_delta,
    ROUND(((total_revenue - LAG(total_revenue) OVER (
        ORDER BY order_month
    )) / NULLIF(LAG(total_revenue) OVER (
        ORDER BY order_month
    ), 0) * 100)::numeric, 1)                         AS mom_growth_pct,
    CASE
        WHEN total_revenue > LAG(total_revenue) OVER (ORDER BY order_month)
        THEN '📈 Growth'
        WHEN total_revenue < LAG(total_revenue) OVER (ORDER BY order_month)
        THEN '📉 Decline'
        ELSE '➡️ Flat'
    END                                               AS trend
FROM analytics.monthly_revenue
ORDER BY order_month
"""


# ── Q5: Top 5 Categories by Revenue with Running Total ───────────────────
# Business: Which categories drive most revenue? What % of total?
# WRITE THIS YOURSELF
Q5_CATEGORY_REVENUE_SHARE = """
-- YOUR SQL HERE
-- HINTS:
--   FROM analytics.product_analytics
--   ORDER BY total_revenue DESC LIMIT 20
--   Use SUM() OVER () without PARTITION to get grand total
--   Use SUM() OVER (ORDER BY total_revenue DESC) for running total
--
-- Target columns:
--   product_category_english, total_revenue,
--   revenue_share_pct  (this category / total * 100),
--   cumulative_revenue (running sum ordered by revenue DESC),
--   cumulative_share_pct (cumulative / total * 100),
--   revenue_rank (RANK by total_revenue DESC)
--
-- This tells you: "top 5 categories = X% of all revenue"
SELECT 'implement Q5' AS placeholder
"""


# ── Q6: Seller Performance Quartiles ─────────────────────────────────────
# Business: Segment sellers into performance tiers (top 25%, etc.)
# WRITE THIS YOURSELF
Q6_SELLER_QUARTILES = """
-- YOUR SQL HERE
-- HINTS:
--   FROM analytics.seller_performance
--   Use NTILE(4) OVER (ORDER BY total_revenue DESC) AS revenue_quartile
--   Use NTILE(4) OVER (ORDER BY avg_review_score DESC) AS rating_quartile
--   Use NTILE(4) OVER (ORDER BY on_time_delivery_rate DESC) AS ontime_quartile
--
-- Target columns:
--   seller_id, seller_state,
--   total_revenue, avg_review_score, on_time_delivery_rate,
--   revenue_quartile (1=top 25%, 4=bottom 25%),
--   rating_quartile,
--   ontime_quartile,
--   overall_score  (revenue_quartile + rating_quartile + ontime_quartile,
--                   lower = better overall performer)
--
-- ORDER BY overall_score ASC (best performers first)
SELECT 'implement Q6' AS placeholder
"""


# ── Q7: Monthly Order Volume with 3-Month Moving Average ──────────────────
# Business: Smooth out noise to see the real trend
# WRITE THIS YOURSELF
Q7_ORDER_VOLUME_TREND = """
-- YOUR SQL HERE
-- HINTS:
--   FROM analytics.monthly_revenue
--   Use AVG(total_orders) OVER (
--       ORDER BY order_month
--       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
--   ) AS moving_avg_3mo
--   Also use LEAD(total_orders, 1) to show NEXT month's orders
--   (useful for "is next month projected to be up or down?")
--
-- Target columns:
--   order_month, total_orders, total_revenue,
--   moving_avg_3mo (3-month moving average of order count),
--   next_month_orders (LEAD of total_orders),
--   orders_vs_moving_avg (total_orders - moving_avg_3mo)
SELECT 'implement Q7' AS placeholder
"""


# ── Q8: Customer Cohort — First Order Month ───────────────────────────────
# Business: How many customers acquired each month? What's their avg LTV?
# WRITE THIS YOURSELF — advanced CTE + window
Q8_CUSTOMER_COHORT = """
-- YOUR SQL HERE
-- HINTS:
-- Step 1: Find first order month per customer
-- WITH first_orders AS (
--     SELECT
--         c.customer_unique_id,
--         DATE_TRUNC('month', MIN(o.order_purchase_timestamp::timestamp))::date
--             AS cohort_month,
--         COUNT(DISTINCT o.order_id) AS lifetime_orders,
--         SUM(op.payment_value)      AS lifetime_value
--     FROM raw.customers c
--     JOIN raw.orders o ON c.customer_id = o.customer_id
--     JOIN raw.order_payments op ON o.order_id = op.order_id
--     WHERE o.order_status = 'delivered'
--     GROUP BY c.customer_unique_id
-- )
-- Step 2: Aggregate by cohort month
-- SELECT
--     cohort_month,
--     COUNT(*) AS cohort_size,
--     ROUND(AVG(lifetime_value)::numeric, 2) AS avg_ltv,
--     SUM(COUNT(*)) OVER (ORDER BY cohort_month) AS cumulative_customers,
--     ROUND(AVG(AVG(lifetime_value)) OVER (
--         ORDER BY cohort_month
--         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
--     )::numeric, 2) AS rolling_avg_ltv
-- FROM first_orders
-- GROUP BY cohort_month
-- ORDER BY cohort_month
--
-- Expected rows: ~22 (one per month Jan 2017 - Aug 2018)
SELECT 'implement Q8' AS placeholder
"""


def main() -> None:
    logger.info("=" * 52)
    logger.info("Window Functions — Day 49")
    logger.info("=" * 52)

    queries = [
        ("q1_running_revenue",      Q1_RUNNING_REVENUE),
        ("q2_seller_rank_by_state", Q2_SELLER_RANK_BY_STATE),
        ("q3_customer_percentile",  Q3_CUSTOMER_PERCENTILE),
        ("q4_revenue_mom",          Q4_REVENUE_MOM),
        ("q5_category_revenue_share", Q5_CATEGORY_REVENUE_SHARE),
        ("q6_seller_quartiles",     Q6_SELLER_QUARTILES),
        ("q7_order_volume_trend",   Q7_ORDER_VOLUME_TREND),
        ("q8_customer_cohort",      Q8_CUSTOMER_COHORT),
    ]

    results = {}
    for name, sql in queries:
        try:
            df = run_query(name, sql)
            results[name] = len(df)
        except Exception as exc:
            logger.error(f"❌ {name}: {exc}")
            results[name] = 0

    dispose_ecommerce_engine()

    logger.info(f"\n── Summary ──────────────────────────────────")
    for name, count in results.items():
        status = "✅" if count > 0 else "❌"
        logger.info(f"  {status} {name:<35} {count:>6,} rows")


if __name__ == "__main__":
    main()
```

---

### EXERCISE 2 — Verify Key Results

```bash
python sprint-08/day-49/window_functions.py

# Spot-check Q2 (top sellers by state)
python -c "
import pandas as pd
df = pd.read_csv('sprint-08/day-49/output/q2_seller_rank_by_state.csv')
print('Top 3 states by seller count:')
print(df['seller_state'].value_counts().head(3))
print('\nTop seller in SP state:')
sp = df[df['seller_state']=='SP'].sort_values('state_rank').head(1)
print(sp[['seller_state','total_revenue','state_rank']].to_string(index=False))
"

# Spot-check Q4 (MoM revenue)
python -c "
import pandas as pd
df = pd.read_csv('sprint-08/day-49/output/q4_revenue_mom.csv')
print('Revenue trend:')
print(df[['order_month','revenue','mom_growth_pct','trend']].tail(6).to_string(index=False))
"
```

---

### EXERCISE 3 — Git Push

```bash
python scripts/daily_commit.py --day 49 --sprint 8 ^
    --message "Advanced SQL: 8 window function queries — running totals, ranking, percentiles, cohort analysis" ^
    --merge
```

---

## ✅ DAY 49 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | `window_functions.py` runs without error | [ ] |
| 2 | Q1-Q4 produce correct results | [ ] |
| 3 | **Q5: Category revenue share with running total** | [ ] |
| 4 | **Q6: Seller performance quartiles** | [ ] |
| 5 | **Q7: Order volume with 3-month moving average** | [ ] |
| 6 | **Q8: Customer cohort analysis** | [ ] |
| 7 | All 8 CSVs in `sprint-08/day-49/output/` | [ ] |
| 8 | One clean `[DAY-049][S08]` commit | [ ] |

---

## 🔍 EXPECTED OUTPUT

```
q1_running_revenue:      22 rows  ← one per month
q2_seller_rank_by_state: ~81 rows ← top 3 per state (27 states × 3)
q3_customer_percentile:  100 rows ← LIMIT 100 top customers
q4_revenue_mom:          22 rows  ← one per month with trend
q5_category_revenue_share: 20 rows ← top 20 categories
q6_seller_quartiles:     3,095 rows ← all sellers with quartile scores
q7_order_volume_trend:   22 rows  ← monthly with moving avg
q8_customer_cohort:      ~22 rows ← monthly cohort acquisition
```

---

## 🔜 PREVIEW: DAY 50

**Topic:** Advanced SQL continued — self-joins, recursive CTEs, query optimisation  
**What you'll do:** Solve 4 "hard" interview-style SQL questions using self-joins
and recursive CTEs. Analyse query execution plans with EXPLAIN ANALYZE.
Add indexes to analytics tables and measure the speedup.

---

*Day 49 | Sprint 08 | EP-11 | TASK-049*
