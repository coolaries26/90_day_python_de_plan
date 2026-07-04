-- Replace the existing config block:
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns'
) }}

WITH order_data AS (
    SELECT
        o.order_id,
        c.customer_unique_id,
        o.order_status,
        o.purchased_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.delivery_days,
        COALESCE(o.is_late, false) AS is_late,  --o.is_late,
        p.total_payment,
        p.payment_types,
        r.review_score,
        items.product_count
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    LEFT JOIN (
        SELECT order_id,
               ROUND(AVG(review_score)::numeric, 2) AS review_score
        FROM {{ source('raw', 'order_reviews') }}
        GROUP BY order_id
    ) r ON o.order_id = r.order_id
    JOIN (
        SELECT order_id, COUNT(*) AS product_count
        FROM {{ source('raw', 'order_items') }}
        GROUP BY order_id
    ) items ON o.order_id = items.order_id
    WHERE o.order_status = 'delivered'

-- YOUR TASK: Add incremental filter below
-- HINTS:
{% if is_incremental() %}
    AND o.purchased_at > (
        SELECT COALESCE(MAX(purchased_at), '2000-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}
--
-- {{ this }} refers to the existing mart_order_metrics table
-- COALESCE handles the case where the table is empty on first run
-- This means: "only process orders newer than what we already have"
)
SELECT * FROM order_data


-- -- mart_order_metrics.sql
-- -- YOUR TASK: Order-level metrics using staging models
-- -- Requirements (same logic as analytics.order_metrics from Day 44
-- --               but using ref() instead of raw schema):
-- --   FROM {{ ref('stg_orders') }} o
-- --   JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
-- --   JOIN {{ ref('stg_order_payments') }} p ON o.order_id = p.order_id
-- --   LEFT JOIN {{ source('raw', 'order_reviews') }} r ON o.order_id = r.order_id
-- --   JOIN (SELECT order_id, COUNT(*) AS product_count FROM raw.order_items GROUP BY order_id) items
-- --       ON o.order_id = items.order_id
-- --   WHERE o.order_status = 'delivered'
-- --
-- -- Columns: order_id, customer_unique_id, order_status,
-- --          purchased_at, delivered_at, estimated_delivery_at,
-- --          delivery_days (from stg_orders), is_late (from stg_orders),
-- --          total_payment (from stg_order_payments),
-- --          review_score, product_count
-- -- Config: materialized='table'
-- 
-- {{ config(materialized='table') }}
-- 
-- SELECT
--     o.order_id,
--     c.customer_unique_id,
--     o.order_status,
--     o.purchased_at,
--     o.delivered_at,
--     o.estimated_delivery_at,
--     o.delivery_days,
--     COALESCE(o.is_late, false) AS is_late,
--     p.total_payment,
--     r.review_score,
--     items.product_count
-- FROM {{ ref('stg_orders') }} o
-- JOIN {{ ref('stg_customers') }} c
--     ON o.customer_id = c.customer_id
-- JOIN {{ ref('stg_order_payments') }} p
--     ON o.order_id = p.order_id
-- LEFT JOIN (
--     SELECT order_id, AVG(review_score) AS review_score
--     FROM {{ source('raw', 'order_reviews') }}
--     GROUP BY order_id
--     ) r ON o.order_id = r.order_id
-- JOIN (
--         SELECT order_id, COUNT(*) AS product_count
--         FROM {{ source('raw', 'order_items') }}
--         GROUP BY order_id
--     ) items
--     ON o.order_id = items.order_id
-- WHERE o.order_status = 'delivered'
-- 