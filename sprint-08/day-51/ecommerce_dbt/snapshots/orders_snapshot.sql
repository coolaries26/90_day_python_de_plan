{% snapshot orders_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_id',
        strategy='check',
        check_cols=['order_status', 'delivered_at'],
    )
}}

-- Snapshot tracks changes to order_status and delivery date
-- Each time order_status changes, a new row is inserted with:
--   dbt_valid_from = when this status started
--   dbt_valid_to   = when it changed (NULL = current)

SELECT
    order_id,
    customer_id,
    order_status,
    purchased_at,
    approved_at,
    shipped_at,
    delivered_at,
    estimated_delivery_at,
    is_late
FROM {{ ref('stg_orders') }}

{% endsnapshot %}