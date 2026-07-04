{{ config(materialized='table',
          unique_key='order_id',
          on_schema_change='sync_all_columns',
          tags=['mart', 'order_metrics'] 
) }}

with order_data AS (
    SELECT
        o.order_id,
        c.customer_unique_id,
        o.order_status,
        o.purchased_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.delivery_days,
        p.total_payment,
        p.payment_types,
        items.product_count,
        COALESCE(o.is_late, false) AS is_late
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    LEFT JOIN (
        SELECT order_id, COUNT(product_id) AS product_count
        FROM {{ source('raw', 'order_items') }}
        GROUP BY order_id
    ) items
        ON o.order_id = items.order_id
    WHERE o.order_status = 'delivered'
)

{% if is_incremental() %}
    AND o.purchased_at > (
        SELECT COALESCE(MAX(purchased_at), '2000-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}

select * from order_data