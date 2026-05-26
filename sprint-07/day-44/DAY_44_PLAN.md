# 📅 DAY 44 — Sprint 07 | Capstone ETL
## raw → 5 Analytics Tables: LTV, Order Metrics, Seller, Product, Revenue

---

## 🔁 RETROSPECTIVE — Day 43

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| 8 CSVs loaded | ✅ Pass | 550,759 rows exact |
| ecommerce_db schemas | ✅ Pass | raw/analytics/ml |
| ARCHITECTURE.md | ⚠️ Empty | Fill with design content before Day 44 ETL |
| Data quality notes | ℹ️ Pre-identified | payments > orders, reviews ≈ orders |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-07/day-44-etl-analytics
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-10: Capstone & Job Readiness                              |
| Story           | ST-44: ETL — raw → Analytics Tables                          |
| Task ID         | TASK-044                                                     |
| Sprint          | Sprint 07 (Days 43–48)                                       |
| Story Points    | 3                                                            |
| Priority        | CRITICAL                                                     |
| Labels          | etl, analytics, ecommerce, sql, pandas, day-44               |
| Acceptance Criteria | 5 analytics tables created with correct row counts and business logic |

---

## 📚 BACKGROUND

### Why These 5 Tables?

Each analytics table answers a specific business question:

| Table | Business Question | Interview Relevance |
|-------|------------------|---------------------|
| `customer_ltv` | Who are our most valuable customers? Will they churn? | LTV is asked in every DE/DS interview |
| `order_metrics` | How fast are we delivering? Are customers happy? | Ops KPI — every e-commerce company tracks this |
| `seller_performance` | Which sellers drive revenue and which hurt ratings? | Marketplace analytics — Amazon, Shopify core |
| `product_analytics` | Which categories generate most revenue? | Inventory/pricing decisions |
| `monthly_revenue` | How is the business growing MoM? | CFO metric — always asked |

### Data Quality Issues to Handle

```
1. order_payments: multiple rows per order_id
   → GROUP BY order_id, SUM(payment_value)

2. order_reviews: ~200 orders have no review
   → LEFT JOIN, NULL review_score handled

3. product_category_name: in Portuguese in products table
   → JOIN to product_category_translation for English name

4. Timestamps: stored as strings in CSV
   → pd.to_datetime() or CAST in SQL

5. delivery_time: order_estimated_delivery_date - order_purchase_timestamp
   → Can be negative (returned orders) — filter or flag
```

---

## 🎯 OBJECTIVES

Build 5 analytics tables in the `analytics` schema of `ecommerce_db`.

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 10 min | Branch setup |
| B | 50 min | `analytics_etl.py` — all 5 tables |
| C | 20 min | Verify row counts + sample data |
| D | 20 min | Write to audit log |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — analytics_etl.py (Block B)
**[T1 customer_ltv provided. T2-T5 write yourself with SQL hints]**

Create `capstone/etl/analytics_etl.py`:

```python
#!/usr/bin/env python3
"""
analytics_etl.py — Day 44 | Capstone ETL
==========================================
Builds 5 analytics tables from raw Olist data.
Each function = one analytics table.

Run: python capstone/etl/analytics_etl.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from logger import get_pipeline_logger
from db import get_ecommerce_engine, dispose_ecommerce_engine

logger = get_pipeline_logger("analytics_etl")


def run_etl(sql: str, target_table: str, engine) -> int:
    """Execute SQL, write result to analytics schema. Returns row count."""
    df = pd.read_sql(sql, engine)
    df.to_sql(
        name=target_table,
        con=engine,
        schema="analytics",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    logger.info(f"✅ analytics.{target_table:<30} {len(df):>8,} rows")
    return len(df)


# ── T1: customer_ltv — PROVIDED ───────────────────────────────────────────
CUSTOMER_LTV_SQL = """
WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        COUNT(DISTINCT o.order_id)                          AS total_orders,
        ROUND(SUM(op.payment_value)::numeric, 2)            AS total_spent,
        MIN(o.order_purchase_timestamp::timestamp)          AS first_order_date,
        MAX(o.order_purchase_timestamp::timestamp)          AS last_order_date,
        EXTRACT(DAY FROM
            NOW() - MAX(o.order_purchase_timestamp::timestamp))::int
                                                            AS days_since_last_order,
        ROUND(AVG(r.review_score)::numeric, 2)              AS avg_review_score,
        COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END) AS delivered_orders,
        COUNT(CASE WHEN o.order_status = 'cancelled' THEN 1 END) AS cancelled_orders
    FROM raw.customers c
    JOIN raw.orders o ON c.customer_id = o.customer_id
    JOIN raw.order_payments op ON o.order_id = op.order_id
    LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
    GROUP BY c.customer_unique_id, c.customer_city, c.customer_state
),
ltv_with_segments AS (
    SELECT *,
        CASE
            WHEN total_spent >= 500  THEN 'Platinum'
            WHEN total_spent >= 200  THEN 'Gold'
            WHEN total_spent >= 100  THEN 'Silver'
            ELSE                          'Bronze'
        END AS value_segment,
        CASE
            WHEN days_since_last_order > 365 THEN 1
            WHEN total_orders = 1
             AND days_since_last_order > 180 THEN 1
            ELSE 0
        END AS is_churned,
        ROUND(total_spent / NULLIF(total_orders, 0), 2) AS avg_order_value
    FROM customer_orders
)
SELECT * FROM ltv_with_segments
ORDER BY total_spent DESC
"""


# ── T2: order_metrics — WRITE THIS YOURSELF ───────────────────────────────
ORDER_METRICS_SQL = """
-- YOUR SQL HERE
-- Target columns:
--   order_id, customer_unique_id, order_status,
--   order_purchase_date (date part only),
--   estimated_delivery_date, actual_delivery_date,
--   delivery_days_estimated (estimated - purchase, as integer),
--   delivery_days_actual (actual - purchase, as integer),
--   is_late (1 if actual > estimated, else 0),
--   days_late (actual - estimated, negative = early),
--   payment_value, payment_type,
--   review_score (NULL if no review),
--   product_count (items in this order)
--
-- HINTS:
--   FROM raw.orders o
--   JOIN raw.customers c ON o.customer_id = c.customer_id
--   LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
--   JOIN raw.order_payments op ON o.order_id = op.order_id
--       AND op.payment_sequential = 1   ← take first payment row only
--   JOIN (SELECT order_id, COUNT(*) AS product_count
--         FROM raw.order_items GROUP BY order_id) items
--       ON o.order_id = items.order_id
--
--   For date arithmetic:
--   EXTRACT(DAY FROM (
--       o.order_delivered_customer_date::timestamp -
--       o.order_purchase_timestamp::timestamp
--   ))::int AS delivery_days_actual
--
--   Only include orders with status = 'delivered'
--   (undelivered orders have NULL delivery dates)
--
-- Expected rows: ~96,000 (delivered orders only)
SELECT 'implement order_metrics' AS placeholder
"""


# ── T3: seller_performance — WRITE THIS YOURSELF ──────────────────────────
SELLER_PERFORMANCE_SQL = """
-- YOUR SQL HERE
-- Target columns:
--   seller_id, seller_city, seller_state,
--   total_orders (distinct order_ids this seller fulfilled),
--   total_revenue (sum of price from order_items),
--   avg_order_value,
--   avg_review_score,
--   on_time_delivery_rate (% of their orders delivered on time),
--   total_products_sold (sum of quantity from order_items),
--   unique_products (distinct product_ids),
--   active_months (distinct YYYY-MM months with orders)
--
-- HINTS:
--   FROM raw.sellers s
--   JOIN raw.order_items oi ON s.seller_id = oi.seller_id
--   JOIN raw.orders o ON oi.order_id = o.order_id
--   LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
--
--   For on_time_rate:
--   AVG(CASE WHEN o.order_delivered_customer_date::timestamp
--            <= o.order_estimated_delivery_date::timestamp
--       THEN 1.0 ELSE 0.0 END)
--
-- Expected rows: 3,095 (one per seller)
SELECT 'implement seller_performance' AS placeholder
"""


# ── T4: product_analytics — WRITE THIS YOURSELF ───────────────────────────
PRODUCT_ANALYTICS_SQL = """
-- YOUR SQL HERE
-- Target columns:
--   product_category_english (from translation table),
--   total_products (distinct product_ids in category),
--   total_orders (orders containing this category),
--   total_revenue (sum of price),
--   avg_price,
--   avg_review_score,
--   avg_product_weight_g,
--   return_rate (cancelled / total as decimal, 0-1)
--
-- HINTS:
--   FROM raw.products p
--   JOIN raw.product_category_translation t
--       ON p.product_category_name = t.product_category_name
--   JOIN raw.order_items oi ON p.product_id = oi.product_id
--   JOIN raw.orders o ON oi.order_id = o.order_id
--   LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
--   GROUP BY t.product_category_name_english
--   ORDER BY total_revenue DESC
--
-- Expected rows: ~71 (one per category)
SELECT 'implement product_analytics' AS placeholder
"""


# ── T5: monthly_revenue — WRITE THIS YOURSELF ─────────────────────────────
MONTHLY_REVENUE_SQL = """
-- YOUR SQL HERE
-- Target columns:
--   order_month (DATE — first day of month),
--   total_orders,
--   total_revenue,
--   avg_order_value,
--   avg_review_score,
--   delivered_count,
--   cancelled_count,
--   on_time_rate
--
-- HINTS:
--   DATE_TRUNC('month', o.order_purchase_timestamp::timestamp)::date AS order_month
--   FROM raw.orders o
--   JOIN raw.order_payments op ON o.order_id = op.order_id
--   LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
--   GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp::timestamp)
--   ORDER BY order_month
--   Filter: WHERE o.order_purchase_timestamp >= '2017-01-01'
--           (2016 data is incomplete — only a few months)
--
-- Expected rows: ~24 (Jan 2017 to Aug 2018)
SELECT 'implement monthly_revenue' AS placeholder
"""


def main() -> None:
    logger.info("=" * 52)
    logger.info("Capstone ETL — Day 44")
    logger.info("=" * 52)

    engine = get_ecommerce_engine()
    results = {}

    etl_tasks = [
        ("customer_ltv",        CUSTOMER_LTV_SQL),
        ("order_metrics",       ORDER_METRICS_SQL),
        ("seller_performance",  SELLER_PERFORMANCE_SQL),
        ("product_analytics",   PRODUCT_ANALYTICS_SQL),
        ("monthly_revenue",     MONTHLY_REVENUE_SQL),
    ]

    for table_name, sql in etl_tasks:
        try:
            count = run_etl(sql, table_name, engine)
            results[table_name] = count
        except Exception as exc:
            logger.error(f"❌ {table_name}: {exc}")
            results[table_name] = 0

    dispose_ecommerce_engine()

    logger.info("\n── Summary ──────────────────────────────────")
    for table, count in results.items():
        status = "✅" if count > 0 else "❌"
        logger.info(f"  {status} analytics.{table:<30} {count:>8,} rows")
    logger.info(f"  Total analytics rows: {sum(results.values()):,}")


if __name__ == "__main__":
    main()
```

---

### EXERCISE 2 — Verify Analytics Tables

```bash
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()

# Row counts
counts = pd.read_sql('''
    SELECT table_name, pg_size_pretty(
        pg_total_relation_size(
            quote_ident(table_schema)||'.'||quote_ident(table_name)::text
        )) AS size
    FROM information_schema.tables
    WHERE table_schema = 'analytics'
    ORDER BY table_name
''', engine)
print('Analytics tables:')
print(counts.to_string(index=False))

# Sample from customer_ltv
print('\nTop 5 customers by LTV:')
top = pd.read_sql('''
    SELECT customer_unique_id, total_orders, total_spent,
           value_segment, is_churned, avg_review_score
    FROM analytics.customer_ltv
    ORDER BY total_spent DESC LIMIT 5
''', engine)
print(top.to_string(index=False))

# Monthly revenue check
print('\nMonthly revenue (last 3 months):')
monthly = pd.read_sql('''
    SELECT order_month, total_orders, ROUND(total_revenue::numeric, 2) AS revenue
    FROM analytics.monthly_revenue
    ORDER BY order_month DESC LIMIT 3
''', engine)
print(monthly.to_string(index=False))

dispose_ecommerce_engine()
"
```

**Expected analytics table counts:**
```
customer_ltv:       ~96,000 rows  (unique customers with orders)
order_metrics:      ~96,000 rows  (delivered orders)
seller_performance:  3,095 rows   (one per seller)
product_analytics:     ~71 rows   (one per category)
monthly_revenue:       ~24 rows   (Jan 2017 – Aug 2018)
```

---

### EXERCISE 3 — Git Push

```bash
python scripts/daily_commit.py --day 44 --sprint 7 ^
    --message "Capstone ETL: 5 analytics tables — customer_ltv, order_metrics, seller_performance, product_analytics, monthly_revenue" ^
    --merge
```

---

## ✅ DAY 44 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | `analytics_etl.py` runs without error | [ ] |
| 2 | `analytics.customer_ltv` — ~96k rows, value_segment + is_churned columns | [ ] |
| 3 | **T2: `order_metrics` written — ~96k rows, delivery_days_actual, is_late** | [ ] |
| 4 | **T3: `seller_performance` written — 3,095 rows, on_time_delivery_rate** | [ ] |
| 5 | **T4: `product_analytics` written — ~71 rows, English category names** | [ ] |
| 6 | **T5: `monthly_revenue` written — ~24 rows, ordered by month** | [ ] |
| 7 | Verify query shows correct top customers and recent months | [ ] |
| 8 | One clean `[DAY-044][S07]` commit via `daily_commit.py --merge` | [ ] |

---

## 🔍 SELF-CHECK — SQL Correctness

```bash
# Churn rate sanity check
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
df = pd.read_sql('''
    SELECT
        is_churned,
        COUNT(*) AS customers,
        ROUND(AVG(total_spent)::numeric, 2) AS avg_spend
    FROM analytics.customer_ltv
    GROUP BY is_churned
    ORDER BY is_churned
''', engine)
print(df.to_string(index=False))
dispose_ecommerce_engine()
"
# is_churned=1 should have lower avg_spend (makes business sense)
# Typical churn rate: 90-95% (most customers only buy once in e-commerce)

# Late delivery rate
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
df = pd.read_sql('''
    SELECT
        is_late,
        COUNT(*) AS orders,
        ROUND(AVG(review_score)::numeric, 2) AS avg_review
    FROM analytics.order_metrics
    WHERE is_late IS NOT NULL
    GROUP BY is_late
''', engine)
print(df.to_string(index=False))
dispose_ecommerce_engine()
"
# is_late=1 orders should have LOWER avg_review_score (late = unhappy customers)
# This is the key business insight from this dataset
```

---

## ⚠️ COMMON SQL MISTAKES

**Mistake 1 — Double-counting payments:**
```sql
-- WRONG: order_payments has multiple rows per order
SELECT o.order_id, SUM(op.payment_value)
FROM raw.orders o
JOIN raw.order_payments op ON o.order_id = op.order_id
-- This counts instalment payments multiple times

-- CORRECT: aggregate payments first, then join
JOIN (
    SELECT order_id, SUM(payment_value) AS total_payment
    FROM raw.order_payments
    GROUP BY order_id
) op ON o.order_id = op.order_id
```

**Mistake 2 — NULL delivery dates:**
```sql
-- WRONG: NULL - timestamp = NULL, EXTRACT of NULL = NULL
EXTRACT(DAY FROM (
    o.order_delivered_customer_date::timestamp -
    o.order_purchase_timestamp::timestamp
))

-- CORRECT: filter to delivered orders only
WHERE o.order_status = 'delivered'
AND o.order_delivered_customer_date IS NOT NULL
```

**Mistake 3 — Portuguese category names:**
```sql
-- WRONG: products has Portuguese names
SELECT p.product_category_name FROM raw.products p

-- CORRECT: join translation table
SELECT t.product_category_name_english
FROM raw.products p
JOIN raw.product_category_translation t
    ON p.product_category_name = t.product_category_name
```

---

## 🔜 PREVIEW: DAY 45

**Topic:** ML on e-commerce data — churn + delivery delay prediction  
**What you'll do:** Use `analytics.customer_ltv` as the feature source for churn prediction.
Use `analytics.order_metrics` for delivery delay prediction (binary: is_late).
Two separate pipelines, two saved models, deployed via the existing ImbPipeline pattern.

---

*Day 44 | Sprint 07 | EP-10 | TASK-044*
