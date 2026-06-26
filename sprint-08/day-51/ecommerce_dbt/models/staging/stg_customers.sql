-- stg_customers.sql
-- YOUR TASK: Clean the customers table
-- Requirements:
--   - Select: customer_id, customer_unique_id, customer_city,
--             customer_state, customer_zip_code_prefix
--   - No transforms needed — just rename/select from source
--   - Use {{ source('raw', 'customers') }}
-- Config: materialized='view'
{{ config(materialized='view') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    customer_zip_code_prefix
FROM {{ source('raw', 'customers') }}
