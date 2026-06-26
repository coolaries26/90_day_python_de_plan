-- stg_orders.sql
-- Cleans and standardises the raw orders table
-- Casts timestamps, adds derived columns

{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    order_status,

    -- Cast string timestamps to proper TIMESTAMP
    order_purchase_timestamp::TIMESTAMP      AS purchased_at,
    order_approved_at::TIMESTAMP             AS approved_at,
    order_delivered_carrier_date::TIMESTAMP  AS shipped_at,
    order_delivered_customer_date::TIMESTAMP AS delivered_at,
    order_estimated_delivery_date::TIMESTAMP AS estimated_delivery_at,

    -- Derived columns
    DATE_TRUNC('month', order_purchase_timestamp::TIMESTAMP)::DATE
                                             AS purchase_month,
    CASE
        WHEN order_status = 'delivered'
         AND order_delivered_customer_date IS NOT NULL
         AND order_estimated_delivery_date IS NOT NULL
        THEN (order_delivered_customer_date::TIMESTAMP >
              order_estimated_delivery_date::TIMESTAMP)
        ELSE NULL
    END                                      AS is_late,

    EXTRACT(DAY FROM (
        order_delivered_customer_date::TIMESTAMP -
        order_purchase_timestamp::TIMESTAMP
    ))::INT                                  AS delivery_days

FROM {{ source('raw', 'orders') }}
