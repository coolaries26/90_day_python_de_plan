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
            WHEN total_orders = 1 THEN 1
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
--
-- Expected rows: ~96,000 (delivered orders only)
with implement_order_metrics AS (
    SELECT 
    o.order_id, 
    c.customer_unique_id,
    order_status,
    EXTRACT(DAY FROM o.order_purchase_timestamp::timestamp) AS order_purchase_date,
    o.order_estimated_delivery_date,
    o.order_delivered_customer_date,
    EXTRACT(DAY FROM (
        o.order_estimated_delivery_date::timestamp -
        o.order_purchase_timestamp::timestamp
    ))::int AS delivery_days_estimated,
    EXTRACT(DAY FROM (
        order_delivered_customer_date::timestamp -
        order_purchase_timestamp::timestamp
    ))::int AS delivery_days_actual,
    CASE  WHEN order_delivered_customer_date::timestamp > order_estimated_delivery_date::timestamp
          THEN 1 ELSE 0 END AS is_late,
    EXTRACT(DAY FROM (
        order_delivered_customer_date::timestamp -
        order_estimated_delivery_date::timestamp
    ))::int AS days_late,
    payment_value,
    payment_type,
    review_score,
    product_count
   FROM raw.orders o
   JOIN raw.customers c ON o.customer_id = c.customer_id
   LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
   JOIN raw.order_payments op ON o.order_id = op.order_id
       AND op.payment_sequential = 1   
   JOIN (SELECT order_id, COUNT(*) AS product_count
        FROM raw.order_items GROUP BY order_id) items
       ON o.order_id = items.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.order_id, c.customer_unique_id, order_status, o.order_purchase_timestamp, order_purchase_date,
             o.order_estimated_delivery_date, order_delivered_customer_date,
             payment_value, payment_type, review_score, product_count
)
SELECT * FROM implement_order_metrics
order by order_purchase_date

"""


# ── T3: seller_performance — WRITE THIS YOURSELF ──────────────────────────
SELLER_PERFORMANCE_SQL = """
--
-- Expected rows: 3,095 (one per seller)
with implement_seller_performance AS (
    SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.price)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2) AS avg_order_value,
    ROUND(AVG(r.review_score)::NUMERIC, 2) AS avg_review_score,
    ROUND((AVG(CASE WHEN o.order_delivered_customer_date::timestamp <= o.order_estimated_delivery_date::timestamp THEN 1.0 ELSE 0.0 END)*100)::NUMERIC, 2) AS on_time_delivery_rate,
    count(oi.order_id) AS total_products_sold,
    COUNT(DISTINCT oi.product_id) AS unique_products,
    COUNT(DISTINCT DATE_TRUNC('month', o.order_purchase_timestamp::timestamp)) AS active_months
    FROM raw.sellers s
    JOIN raw.order_items oi ON s.seller_id = oi.seller_id
    JOIN raw.orders o ON oi.order_id = o.order_id
    LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
    GROUP BY s.seller_id, s.seller_city, s.seller_state
)
SELECT * FROM implement_seller_performance
ORDER BY total_revenue DESC
"""


# ── T4: product_analytics — WRITE THIS YOURSELF ───────────────────────────
PRODUCT_ANALYTICS_SQL = """
-- YOUR SQL HERE
--
-- Expected rows: ~71 (one per category)
with implement_product_analytics AS (
    SELECT t.product_category_name_english,
    COUNT(DISTINCT p.product_id) AS total_products,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price) AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2) AS avg_price,
    ROUND(AVG(r.review_score)::NUMERIC, 2) AS avg_review_score,
    ROUND(AVG(p.product_weight_g)::NUMERIC, 2) AS avg_product_weight_g,
    ROUND(((CAST(SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) AS float) / COUNT(*))* 100 )::numeric,2) AS return_rate
    FROM raw.products p
    JOIN raw.product_category_translation t ON p.product_category_name = t.product_category_name
    JOIN raw.order_items oi ON p.product_id = oi.product_id
    JOIN raw.orders o ON oi.order_id = o.order_id
    LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
    GROUP BY t.product_category_name_english
)
SELECT * FROM implement_product_analytics
order by total_revenue DESC
"""


# ── T5: monthly_revenue — WRITE THIS YOURSELF ─────────────────────────────
MONTHLY_REVENUE_SQL = """
-- YOUR SQL HERE
--
-- Expected rows: ~24 (Jan 2017 to Aug 2018)
with implement_monthly_revenue AS (
    SELECT 
    DATE_TRUNC('month', o.order_purchase_timestamp::timestamp)::date AS order_month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(op.payment_value)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(op.payment_value)::NUMERIC, 2) AS avg_order_value,
    ROUND(AVG(r.review_score)::NUMERIC, 2) AS avg_review_score,
    COUNT(DISTINCT CASE WHEN o.order_status = 'delivered' THEN o.order_id END) AS delivered_count,
    COUNT(DISTINCT CASE WHEN o.order_status = 'canceled' THEN o.order_id END) AS cancelled_count,
    ROUND((CAST(SUM(CASE WHEN o.order_delivered_customer_date::timestamp <= o.order_estimated_delivery_date::timestamp THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100)::NUMERIC, 2) AS on_time_rate
    FROM raw.orders o
    JOIN raw.order_payments op ON o.order_id = op.order_id
    LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
    WHERE o.order_purchase_timestamp >= '2017-01-01'
    GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp::timestamp)
    ORDER BY order_month
)
SELECT * FROM implement_monthly_revenue
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