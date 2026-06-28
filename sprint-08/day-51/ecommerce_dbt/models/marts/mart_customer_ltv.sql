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
        ROUND(AVG(r.review_score)::numeric, 2)            AS avg_review_score,
        COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END)
                                                       AS delivered_orders,
        COUNT(CASE WHEN o.order_status = 'canceled' THEN 1 END)
                                                       AS cancelled_orders
    FROM {{ ref('stg_customers') }} c
    JOIN {{ ref('stg_orders') }} o
        ON c.customer_id = o.customer_id
    JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    LEFT JOIN (
        SELECT order_id, AVG(review_score) AS review_score
        FROM {{ source('raw', 'order_reviews') }}
        GROUP BY order_id
        ) r ON o.order_id = r.order_id
    GROUP BY c.customer_unique_id, c.customer_city, c.customer_state
),
ltv AS (
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
    ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY total_spent DESC
    ) AS rn,
    ROUND((total_spent / NULLIF(total_orders, 0))::numeric, 2)
--    ROUND(total_spent / NULLIF(total_orders, 0), 2)
                                     AS avg_order_value
FROM customer_orders
ORDER BY total_spent DESC
)
SELECT customer_unique_id,
       customer_city,
       customer_state,
       total_orders,
       total_spent,
       first_order_date,
       last_order_date,
       days_since_last_order,
       avg_review_score,
       delivered_orders,
       cancelled_orders,
       value_segment,
       is_churned,
       rn,
       avg_order_value
FROM ltv
WHERE rn = 1             -- keep only the highest-spend row per customer
