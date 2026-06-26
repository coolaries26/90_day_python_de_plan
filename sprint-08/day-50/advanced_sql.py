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

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04")) #for logger
sys.path.insert(0, str(_here.parent.parent / "capstone"))

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
WITH customer_reviews AS (
    SELECT
        c.customer_unique_id,
        o.order_id,
        r.review_score,
        o.order_purchase_timestamp::timestamp AS order_date,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp ASC
        ) AS order_seq_asc,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp DESC
        ) AS order_seq_desc
    FROM raw.customers c
    JOIN raw.orders o ON c.customer_id = o.customer_id
    JOIN raw.order_reviews r ON o.order_id = r.order_id
    WHERE r.review_score IS NOT NULL
      AND o.order_status = 'delivered'
)
-- Step 2: Self-join first (seq=1) with last (seq_desc=1)
SELECT
    first_order.customer_unique_id,
    first_order.review_score AS first_review,
    last_order.review_score  AS last_review,
    (last_order.review_score - first_order.review_score) AS improvement
FROM customer_reviews first_order
JOIN customer_reviews last_order
    ON first_order.customer_unique_id = last_order.customer_unique_id
    AND first_order.order_seq_asc = 1
    AND last_order.order_seq_desc = 1
    AND first_order.order_id != last_order.order_id  -- different orders
WHERE last_order.review_score > first_order.review_score
ORDER BY improvement DESC
--
-- Expected: customers who bought multiple times and gave a higher review last time
-- SELECT 'implement Q3' AS placeholder
"""


# ── Q4: Product Category Revenue Trend (Recursive + Window) ──────────────
# Interview question: "For top 5 categories, show quarterly revenue trend"
# WRITE THIS YOURSELF
Q4_CATEGORY_QUARTERLY_TREND = """
-- YOUR SQL HERE
-- HINTS:
-- Step 1: Get top 5 categories by total revenue
WITH top_categories AS (
    SELECT product_category_name_english  
    FROM analytics.product_analytics
    ORDER BY total_revenue DESC
    LIMIT 5
),
-- Step 2: Get quarterly revenue per category from raw data
quarterly_revenue AS (
    SELECT
        t.product_category_name_english AS category,
        DATE_TRUNC('quarter', o.order_purchase_timestamp::timestamp)::date AS quarter,
        ROUND(SUM(oi.price)::numeric, 2) AS quarterly_revenue
    FROM raw.order_items oi
    JOIN raw.orders o ON oi.order_id = o.order_id
    JOIN raw.products p ON oi.product_id = p.product_id
    JOIN raw.product_category_translation t
        ON p.product_category_name = t.product_category_name
    WHERE t.product_category_name_english IN (
        SELECT product_category_name_english FROM top_categories
    )
    AND o.order_status = 'delivered'
    GROUP BY category, quarter
)
-- Step 3: Add LAG to show quarter-over-quarter change
SELECT
    category, quarter, quarterly_revenue,
    LAG(quarterly_revenue) OVER (
        PARTITION BY category ORDER BY quarter
    ) AS prev_quarter_revenue,
    ROUND((quarterly_revenue - LAG(quarterly_revenue) OVER (
        PARTITION BY category ORDER BY quarter
    ))::numeric, 2) AS qoq_change
FROM quarterly_revenue
ORDER BY category, quarter
--
-- Expected: ~5 categories × 6-7 quarters = ~30-35 rows
-- SELECT 'implement Q4' AS placeholder
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