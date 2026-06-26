# 📅 DAY 51 — Sprint 08 | dbt Setup + First Models
## Install dbt-postgres, Initialise Project, Convert ETL SQL to dbt Models

---

## 🔁 RETROSPECTIVE — Day 50

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Q1/Q2 correct | ✅ Pass | |
| Q3 self-join | ✅ Pass | 472 repeat customers who improved |
| Q4 quarterly trend | ✅ Pass | 41 rows |
| 3 indexes created | ✅ Pass | |
| Speedup 1.2-1.6x | ✅ Expected | Low cardinality + large result set |
| develop push error | ⚠️ Fix first | upstream branch reference lost |

### Fix develop upstream + branch setup
```bash
cd C:\90_day_python_de_plan

# Fix develop upstream
git checkout develop
git push --set-upstream origin develop

# Create Day 51 branch
git checkout -b sprint-08/day-51-dbt-setup
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-11: Advanced SQL & dbt |
| Story           | ST-51: dbt Setup + First Models |
| Task ID         | TASK-051 |
| Sprint          | Sprint 08 (Days 49–56) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | dbt, data-modeling, sql, ecommerce, day-51 |
| Acceptance Criteria | dbt project initialised; profiles.yml configured; 3 staging models + 2 mart models running; dbt run passes |

---

## 📚 BACKGROUND

### What is dbt?

```
dbt (data build tool) is the industry-standard way to manage SQL transforms.

Without dbt (what you've been doing):
  Python script → runs SQL → writes to analytics table
  No tests, no documentation, no lineage tracking, no incremental loads

With dbt:
  SQL files (called "models") → dbt compiles + runs them in dependency order
  Built-in tests (not null, unique, accepted values, referential integrity)
  Auto-generated documentation with lineage graphs
  Incremental models (only process new data, not full refresh)
  Jinja templating (reuse SQL logic with macros)

Who uses it: Airbnb, Spotify, GitLab, Shopify — every modern data stack
Interview question: "Have you used dbt?" — now you can say yes
```

### dbt Project Structure

```
ecommerce_dbt/
├── dbt_project.yml          ← project config
├── profiles.yml             ← DB connection (usually in ~/.dbt/)
├── models/
│   ├── staging/             ← clean + rename raw tables
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   └── schema.yml       ← tests + documentation
│   └── marts/               ← business logic (analytics tables)
│       ├── mart_customer_ltv.sql
│       ├── mart_order_metrics.sql
│       └── schema.yml
├── macros/                  ← reusable SQL functions
└── tests/                   ← custom data tests
```

### Key dbt Concepts

```sql
-- 1. Ref function — reference another model (handles dependency order)
SELECT * FROM {{ ref('stg_orders') }}   -- not FROM raw.orders

-- 2. Source function — reference raw tables
SELECT * FROM {{ source('raw', 'orders') }}

-- 3. Config block — set materialisation
{{ config(materialized='table') }}      -- always recreate full table
{{ config(materialized='view') }}       -- SQL view, no storage
{{ config(materialized='incremental') }}-- only process new rows

-- 4. Schema.yml — tests and docs
models:
  - name: stg_orders
    description: "Cleaned orders from raw"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
```

---

## 🎯 OBJECTIVES

1. Install `dbt-postgres`
2. Initialise dbt project `ecommerce_dbt`
3. Configure `profiles.yml` for ecommerce_db
4. Write 3 staging models (clean raw tables)
5. Write 2 mart models (business analytics)
6. Run `dbt run` + `dbt test`
7. Push clean `[DAY-051][S08]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 20 min | Install + init + profiles.yml |
| B | 40 min | 3 staging models |
| C | 30 min | 2 mart models |
| D | 10 min | dbt run + dbt test |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Install + Init (Block A)

```bash
# Install dbt-postgres in your Windows venv
cd C:\90_day_python_de_plan
.venv\Scripts\activate
pip install dbt-postgres==1.7.4

# Verify
dbt --version
# Expected: Core: 1.7.x  Plugins: postgres 1.7.x

# Initialise dbt project inside sprint-08/
mkdir sprint-08\day-51
cd sprint-08\day-51
dbt init ecommerce_dbt
# When prompted:
#   Which database adapter? → postgres (option 1 or 2)
#   Project name: ecommerce_dbt
```

**Configure profiles.yml** — dbt looks for this in `~/.dbt/profiles.yml`:

```bash
# Create/update C:\Users\Lenovo\.dbt\profiles.yml
```

```yaml
# C:\Users\Lenovo\.dbt\profiles.yml
ecommerce_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: 127.0.0.1
      port: 5432
      user: appuser
      password: "AppUser@2024!"
      dbname: ecommerce_db
      schema: dbt_dev        # dbt creates this schema for output
      threads: 4
```

```bash
# Test connection
cd sprint-08/day-51/ecommerce_dbt
dbt debug
# Expected: All checks passed!
```

---

### EXERCISE 2 — Configure dbt_project.yml (Block A)

Open `sprint-08/day-51/ecommerce_dbt/dbt_project.yml` and replace the models section:

```yaml
name: 'ecommerce_dbt'
version: '1.0.0'
config-version: 2

profile: 'ecommerce_dbt'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
docs-paths: ["docs"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  ecommerce_dbt:
    staging:
      +materialized: view        # staging = views (lightweight)
      +schema: staging
    marts:
      +materialized: table       # marts = tables (pre-computed)
      +schema: marts
```

---

### EXERCISE 3 — Add sources.yml (Block B)

Create `sprint-08/day-51/ecommerce_dbt/models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: raw
    database: ecommerce_db
    schema: raw
    description: "Raw Olist e-commerce data loaded from CSV"
    tables:
      - name: orders
        description: "99,441 customer orders with status and timestamps"
        columns:
          - name: order_id
            tests:
              - unique
              - not_null

      - name: order_items
        description: "112,650 line items within orders"

      - name: order_payments
        description: "103,886 payment records (multiple per order)"

      - name: order_reviews
        description: "99,224 customer reviews"

      - name: customers
        description: "99,441 unique customer records"
        columns:
          - name: customer_id
            tests:
              - not_null

      - name: sellers
        description: "3,095 marketplace sellers"

      - name: products
        description: "32,951 product listings"

      - name: product_category_translation
        description: "71 category name translations (PT → EN)"
```

---

### EXERCISE 4 — 3 Staging Models (Block B)
**[stg_orders provided. stg_customers + stg_order_payments write yourself]**

Create `sprint-08/day-51/ecommerce_dbt/models/staging/stg_orders.sql`:

```sql
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
```

**Create `stg_customers.sql` — WRITE THIS YOURSELF:**
```sql
-- stg_customers.sql
-- YOUR TASK: Clean the customers table
-- Requirements:
--   - Select: customer_id, customer_unique_id, customer_city,
--             customer_state, customer_zip_code_prefix
--   - No transforms needed — just rename/select from source
--   - Use {{ source('raw', 'customers') }}
-- Config: materialized='view'
```

**Create `stg_order_payments.sql` — WRITE THIS YOURSELF:**
```sql
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
```

---

### EXERCISE 5 — Staging schema.yml (Block B)

Create `sprint-08/day-51/ecommerce_dbt/models/staging/schema.yml`:

```yaml
version: 2

models:
  - name: stg_orders
    description: "Cleaned orders with proper timestamps and derived columns"
    columns:
      - name: order_id
        description: "Primary key"
        tests:
          - unique
          - not_null
      - name: order_status
        tests:
          - accepted_values:
              values: ['delivered', 'shipped', 'canceled', 'unavailable',
                       'invoiced', 'processing', 'created', 'approved']

  - name: stg_customers
    description: "Customer dimension table"
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null

  - name: stg_order_payments
    description: "Payments aggregated to one row per order"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
```

---

### EXERCISE 6 — 2 Mart Models (Block C)
**[mart_customer_ltv provided. mart_order_metrics write yourself]**

Create `sprint-08/day-51/ecommerce_dbt/models/marts/mart_customer_ltv.sql`:

```sql
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
        SUM(p.total_payment)                           AS total_spent,
        MIN(o.purchased_at)                            AS first_order_date,
        MAX(o.purchased_at)                            AS last_order_date,
        EXTRACT(DAY FROM NOW() - MAX(o.purchased_at))::INT
                                                       AS days_since_last_order,
        AVG(r.review_score)                            AS avg_review_score,
        COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END)
                                                       AS delivered_orders,
        COUNT(CASE WHEN o.order_status = 'canceled' THEN 1 END)
                                                       AS cancelled_orders
    FROM {{ ref('stg_customers') }} c
    JOIN {{ ref('stg_orders') }} o
        ON c.customer_id = o.customer_id
    JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    LEFT JOIN {{ source('raw', 'order_reviews') }} r
        ON o.order_id = r.order_id
    GROUP BY c.customer_unique_id, c.customer_city, c.customer_state
)
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
    ROUND(total_spent / NULLIF(total_orders, 0), 2)
                                     AS avg_order_value
FROM customer_orders
ORDER BY total_spent DESC
```

**Create `mart_order_metrics.sql` — WRITE THIS YOURSELF:**
```sql
-- mart_order_metrics.sql
-- YOUR TASK: Order-level metrics using staging models
-- Requirements (same logic as analytics.order_metrics from Day 44
--               but using ref() instead of raw schema):
--   FROM {{ ref('stg_orders') }} o
--   JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
--   JOIN {{ ref('stg_order_payments') }} p ON o.order_id = p.order_id
--   LEFT JOIN {{ source('raw', 'order_reviews') }} r ON o.order_id = r.order_id
--   JOIN (SELECT order_id, COUNT(*) AS product_count FROM raw.order_items GROUP BY order_id) items
--       ON o.order_id = items.order_id
--   WHERE o.order_status = 'delivered'
--
-- Columns: order_id, customer_unique_id, order_status,
--          purchased_at, delivered_at, estimated_delivery_at,
--          delivery_days (from stg_orders), is_late (from stg_orders),
--          total_payment (from stg_order_payments),
--          review_score, product_count
-- Config: materialized='table'
```

---

### EXERCISE 7 — dbt run + dbt test (Block D)

```bash
cd sprint-08/day-51/ecommerce_dbt

# Run all models
dbt run

# Expected output:
# 1 of 5 START sql view model dbt_dev_staging.stg_customers ..... [OK]
# 2 of 5 START sql view model dbt_dev_staging.stg_order_payments . [OK]
# 3 of 5 START sql view model dbt_dev_staging.stg_orders ......... [OK]
# 4 of 5 START sql table model dbt_dev_marts.mart_customer_ltv .... [OK]
# 5 of 5 START sql table model dbt_dev_marts.mart_order_metrics ... [OK]
# Finished running 5 models in Xs

# Run tests
dbt test

# Expected:
# PASS: stg_orders.order_id unique
# PASS: stg_orders.order_id not_null
# PASS: stg_orders.order_status accepted_values
# etc.

# Generate and view docs
dbt docs generate
dbt docs serve --port 8580
# Open: http://localhost:8580
```

---

### EXERCISE 8 — Git Push

```bash
cd C:\90_day_python_de_plan
python scripts/daily_commit.py --day 51 --sprint 8 ^
    --message "dbt: project init, 3 staging models, 2 mart models, dbt run + test passing" ^
    --merge
```

---

## ✅ DAY 51 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | `dbt --version` shows 1.7.x | [ ] |
| 2 | `dbt debug` passes — All checks passed! | [ ] |
| 3 | `stg_orders.sql` created — provided | [ ] |
| 4 | **`stg_customers.sql` written** | [ ] |
| 5 | **`stg_order_payments.sql` written — aggregated to 1 row per order** | [ ] |
| 6 | `sources.yml` + `schema.yml` created with tests | [ ] |
| 7 | `mart_customer_ltv.sql` created — provided | [ ] |
| 8 | **`mart_order_metrics.sql` written — uses ref() throughout** | [ ] |
| 9 | `dbt run` completes: 5/5 models OK | [ ] |
|10 | `dbt test` passes all tests | [ ] |
|11 | One clean `[DAY-051][S08]` commit | [ ] |

---

## 🔍 VERIFY dbt OUTPUT IN PostgreSQL

```bash
# dbt creates tables/views in dbt_dev_staging and dbt_dev_marts schemas
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
result = pd.read_sql('''
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema LIKE 'dbt_%'
    ORDER BY table_schema, table_name
''', engine)
print(result.to_string(index=False))
dispose_ecommerce_engine()
"
# Expected:
# dbt_dev_marts    mart_customer_ltv   BASE TABLE
# dbt_dev_marts    mart_order_metrics  BASE TABLE
# dbt_dev_staging  stg_customers       VIEW
# dbt_dev_staging  stg_order_payments  VIEW
# dbt_dev_staging  stg_orders          VIEW
```

---

## 🔜 PREVIEW: DAY 52

**Topic:** dbt tests + documentation  
**What you'll do:** Add 10+ tests to schema.yml (unique, not_null, accepted_values,
relationships), write model descriptions in YAML, run `dbt docs generate`,
and explore the lineage graph in the dbt docs UI.

---

*Day 51 | Sprint 08 | EP-11 | TASK-051*
