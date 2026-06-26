-- stg_order_payments.sql
-- YOUR TASK: Aggregate payments to one row per order
-- Requirements:
--   - GROUP BY order_id
--   - SUM(payment_value) AS total_payment
--   - COUNT(*) AS payment_installments
--   - STRING_AGG(DISTINCT payment_type, ', ') AS payment_types
--   - Use {{ source('raw', 'order_payments') }}
-- This solves the double-counting problem from Day 44
-- Config: materialized='view'
{{ config(materialized='view') }}

SELECT
    order_id,
    SUM(payment_value) AS total_payment,
    COUNT(*) AS payment_installments,
    STRING_AGG(DISTINCT payment_type, ', ') AS payment_types
FROM {{ source('raw', 'order_payments') }}
GROUP BY order_id
