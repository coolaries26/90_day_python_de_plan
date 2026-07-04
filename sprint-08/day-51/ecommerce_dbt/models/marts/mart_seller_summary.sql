-- **Requirements:**
-- - Source: use `{{ ref('stg_orders') }}` and `{{ source('raw', 'order_items') }}`
--   and `{{ source('raw', 'sellers') }}`
-- - One row per `seller_id`
-- - Columns: `seller_id`, `seller_state`, `total_orders`, `total_revenue`,
--   `avg_delivery_days`, `late_order_count`, `on_time_rate`
-- - Config: `materialized='table'`
-- - Add to `marts/schema.yml`: description + `unique` test on `seller_id`

{{ config(materialized='table',
          unique_key='seller_id',
          on_schema_change='sync_all_columns',
          tags=['mart', 'seller_summary'] 
) }}

with seller_data AS (
    SELECT
        s.seller_id,
        s.seller_state,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(p.total_payment) AS total_revenue,
        ROUND(AVG(o.delivery_days)::numeric, 2) AS avg_delivery_days,
        SUM(CASE WHEN o.is_late THEN 1 ELSE 0 END) AS late_order_count,
        ROUND(
            (SUM(CASE WHEN o.is_late THEN 0 ELSE 1 END)::numeric / COUNT(DISTINCT o.order_id)) * 100, 2
        ) AS on_time_rate
    FROM {{ ref('stg_orders') }} o
    JOIN {{ source('raw', 'order_items') }} oi
        ON o.order_id = oi.order_id
    JOIN {{ source('raw', 'sellers') }} s
        ON oi.seller_id = s.seller_id
    JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY s.seller_id, s.seller_state
)

{% if is_incremental() %}
    AND o.purchased_at > (
        SELECT COALESCE(MAX(purchased_at), '2000-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}

SELECT * FROM seller_data