-- YOUR TASK: Custom test
-- Business rule: if is_late = TRUE, the order MUST have a delivered_at date
-- A NULL delivered_at with is_late = TRUE indicates a data quality issue
--
-- HINTS:
-- SELECT order_id, is_late, delivered_at
-- FROM {{ ref('stg_orders') }}
-- WHERE is_late = TRUE
--   AND delivered_at IS NULL
--
-- Expected: 0 rows (test passes)
-- If rows returned: data quality issue in raw data

SELECT
    order_id,
    is_late,
    delivered_at
FROM {{ ref('stg_orders') }}
WHERE is_late = TRUE
  AND delivered_at IS NULL