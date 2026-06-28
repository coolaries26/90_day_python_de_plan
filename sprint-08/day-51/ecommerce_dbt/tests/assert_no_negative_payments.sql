-- Custom test: no order should have negative total payment
-- This test FAILS if any rows are returned

SELECT
    order_id,
    total_payment
FROM {{ ref('stg_order_payments') }}
WHERE total_payment < 0