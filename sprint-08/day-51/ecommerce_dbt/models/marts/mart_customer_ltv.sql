-- mart_customer_ltv.sql
-- Customer Lifetime Value model
-- Uses staging models via ref() — dbt handles dependency order

{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        COUNT(DISTINCT o.order_id)                     AS total_orders,
        ROUND(SUM(p.total_payment)::numeric, 2)        AS total_spent,
        MIN(o.purchased_at)                            AS first_order_date,
        MAX(o.purchased_at)                            AS last_order_date,
        EXTRACT(DAY FROM NOW() - MAX(o.purchased_at))::INT
                                                       AS days_since_last_order,
        AVG(r.review_score)                            AS avg_review_score,
        COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END)
                                                       AS delivered_orders,
        COUNT(CASE WHEN o.order_status = 'canceled' THEN 1 END)
                                                       AS cancelled_orders
    FROM {{ ref('stg_customers') }} c
    JOIN {{ ref('stg_orders') }} o
        ON c.customer_id = o.customer_id
    JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    LEFT JOIN {{ source('raw', 'order_reviews') }} r
        ON o.order_id = r.order_id
    GROUP BY c.customer_unique_id, c.customer_city, c.customer_state
)
SELECT
    *,
    CASE
        WHEN total_spent >= 500 THEN 'Platinum'
        WHEN total_spent >= 200 THEN 'Gold'
        WHEN total_spent >= 100 THEN 'Silver'
        ELSE                         'Bronze'
    END                              AS value_segment,
    CASE
        WHEN total_orders = 1 THEN 1
        ELSE 0
    END                              AS is_churned,
    ROUND((total_spent / NULLIF(total_orders, 0))::numeric, 2)
--    ROUND(total_spent / NULLIF(total_orders, 0), 2)
                                     AS avg_order_value
FROM customer_orders
ORDER BY total_spent DESC
