### TASK T2 — dbt Model from Scratch (25 min)

**Brief:** Without referencing existing models, write a new dbt model
`mart_seller_summary.sql` in `models/marts/`.

**Requirements:**
- Source: use `{{ ref('stg_orders') }}` and `{{ source('raw', 'order_items') }}`
  and `{{ source('raw', 'sellers') }}`
- One row per `seller_id`
- Columns: `seller_id`, `seller_state`, `total_orders`, `total_revenue`,
  `avg_delivery_days`, `late_order_count`, `on_time_rate`
- Config: `materialized='table'`
- Add to `marts/schema.yml`: description + `unique` test on `seller_id`

**Run and verify:**
```bash
cd sprint-08/day-51/ecommerce_dbt
dbt run --select mart_seller_summary --profiles-dir "C:\90_day_python_de_plan\.dbt"
dbt test --select mart_seller_summary --profiles-dir "C:\90_day_python_de_plan\.dbt"
```

**Pass criteria:**
```
dbt run: 1/1 OK
dbt test: unique test passes
Row count: 3,095 (one per seller)
```

---
