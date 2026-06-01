#!/usr/bin/env python3
"""
window_functions.py — Day 49 | Advanced SQL Window Functions
=============================================================
8 window function queries on ecommerce_db.
Each query answers a specific business question.

Run: python sprint-08/day-49/window_functions.py
"""

from __future__ import annotations
from sqlalchemy import text
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
    df = pd.read_sql(text(sql), engine)
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
-- This tells you: "top 5 categories = Xpercentile of all revenue"
-- SELECT 'implement Q5' AS placeholder
    SELECT
        product_category_name_english,
        total_revenue,
        ROUND((total_revenue / SUM(total_revenue) OVER () * 100)::numeric, 1) AS revenue_share_pct,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS cumulative_revenue,
        ROUND((SUM(total_revenue) OVER (ORDER BY total_revenue DESC) / SUM(total_revenue) OVER () * 100)::numeric, 1) AS cumulative_share_pct,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM analytics.product_analytics
    ORDER BY total_revenue DESC
--    LIMIT 20
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
with segment_sellers AS (
    SELECT
        seller_id, seller_state,
        ROUND(total_revenue::numeric, 2) AS total_revenue,
        ROUND(avg_review_score::numeric, 2) AS avg_review_score,
        ROUND(on_time_delivery_rate::numeric, 2) AS on_time_delivery_rate,
        NTILE(4) OVER (ORDER BY total_revenue DESC) AS revenue_quartile,
    NTILE(4) OVER (ORDER BY avg_review_score DESC) AS rating_quartile,
    NTILE(4) OVER (ORDER BY on_time_delivery_rate DESC) AS ontime_quartile,
    (NTILE(4) OVER (ORDER BY total_revenue DESC) + NTILE(4) OVER (ORDER BY avg_review_score DESC) + NTILE(4) OVER (ORDER BY on_time_delivery_rate DESC)) AS overall_score
FROM analytics.seller_performance
)SELECT * FROM segment_sellers WHERE total_revenue > 0
ORDER BY overall_score ASC, total_revenue DESC
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
SELECT
    order_month,
    total_orders,
    total_revenue,
    AVG(total_orders) OVER (
        ORDER BY order_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3mo,
    LEAD(total_orders, 1) OVER (ORDER BY order_month) AS next_month_orders,
    (total_orders - AVG(total_orders) OVER (
        ORDER BY order_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )) AS orders_vs_moving_avg
FROM analytics.monthly_revenue
ORDER BY order_month

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
WITH first_orders AS (
    SELECT
        c.customer_unique_id,
        DATE_TRUNC('month', MIN(o.order_purchase_timestamp::timestamp))::date AS cohort_month,
        COUNT(DISTINCT o.order_id) AS lifetime_orders,
        SUM(op.payment_value) AS lifetime_value
    FROM raw.customers c
    JOIN raw.orders o ON c.customer_id = o.customer_id
    JOIN raw.order_payments op ON o.order_id = op.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)
SELECT
    cohort_month,
    COUNT(*) AS cohort_size,
    ROUND(AVG(lifetime_value)::numeric, 2) AS avg_ltv,
    SUM(COUNT(*)) OVER (ORDER BY cohort_month) AS cumulative_customers,
    ROUND(AVG(AVG(lifetime_value)) OVER (
        ORDER BY cohort_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )::numeric, 2) AS rolling_avg_ltv
FROM first_orders
GROUP BY cohort_month
ORDER BY cohort_month
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