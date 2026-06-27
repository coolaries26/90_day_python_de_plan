# 📅 DAY 52 — Sprint 08 | dbt Tests + Documentation + Lineage
## Expand Tests, Write YAML Docs, Explore Lineage Graph, Custom Tests

---

## 🔁 RETROSPECTIVE — Day 51

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| dbt run 6/6 models | ✅ Pass | |
| dbt test 14/14 | ✅ Pass | |
| stg_customers written | ✅ Pass | |
| stg_order_payments written | ✅ Pass | aggregated to 1 row per order |
| mart_order_metrics written | ✅ Pass | 97,006 rows |
| Unicode fix in profiles.yml | ✅ Pass | |
| appuser CREATE SCHEMA | ✅ Fixed | Grant permanently |
| mart_customer_ltv missing | ⚠️ Fix | Check dbt ls output |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-08/day-52-dbt-tests-docs

# Fix mart_customer_ltv if not running
cd sprint-08/day-51/ecommerce_dbt
dbt ls
# If mart_customer_ltv missing, verify file exists and dbt_project.yml is correct
dbt run --select mart_customer_ltv
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-11: Advanced SQL & dbt |
| Story           | ST-52: dbt Tests + Documentation |
| Task ID         | TASK-052 |
| Sprint          | Sprint 08 (Days 49–56) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | dbt, testing, documentation, lineage, day-52 |
| Acceptance Criteria | 20+ tests passing; all models documented in YAML; lineage graph visible in docs UI; 1 custom test written |

---

## 📚 BACKGROUND

### dbt Test Types

```yaml
# 1. Generic tests (built-in)
columns:
  - name: order_id
    tests:
      - unique           # no duplicate values
      - not_null         # no NULL values
      - accepted_values: # value must be in this list
          values: ['delivered', 'canceled']
      - relationships:   # foreign key check
          to: ref('stg_orders')
          field: order_id

# 2. Custom singular tests (SQL files in tests/ folder)
# A test FAILS if the SQL returns ANY rows
-- tests/assert_positive_revenue.sql
SELECT order_id
FROM {{ ref('mart_order_metrics') }}
WHERE total_payment < 0    -- returns rows if negative payment exists
                           -- test fails if any rows returned
```

### dbt Documentation

```yaml
# In schema.yml — descriptions appear in dbt docs UI
models:
  - name: mart_customer_ltv
    description: |
      Customer Lifetime Value model.
      One row per unique customer.
      Aggregates all orders, payments and reviews.
      Used by: Streamlit dashboard, churn ML model.
    columns:
      - name: customer_unique_id
        description: "Unique customer identifier across all orders"
      - name: is_churned
        description: "1 if customer made only one purchase, 0 if repeat buyer"
```

### dbt Lineage Graph

```
Running `dbt docs serve` opens a web UI showing:
  raw.orders ──────────────────────────────────────────────────────┐
  raw.customers ──> stg_customers ──> mart_customer_ltv            │
  raw.order_payments ──> stg_order_payments ──> mart_customer_ltv  │
  raw.orders ──> stg_orders ──────────────────> mart_customer_ltv  │
                                                mart_order_metrics <┘

This lineage graph is what interviewers want to see — it proves you
understand data dependencies, not just SQL syntax.
```

---

## 🎯 OBJECTIVES

1. Add 10+ tests to staging and mart schema.yml files
2. Write full descriptions for all 5 models + key columns
3. Write 2 custom singular tests
4. Run `dbt test` — all green
5. Generate docs + view lineage graph
6. Push clean `[DAY-052][S08]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 10 min | Branch + fix mart_customer_ltv |
| B | 30 min | Expand schema.yml with tests + docs |
| C | 25 min | 2 custom singular tests |
| D | 20 min | dbt test + docs |
| E | 15 min | Lineage graph exploration |
| F | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Expand staging/schema.yml (Block B)
**[Provided — replace your existing schema.yml]**

Replace `models/staging/schema.yml`:

```yaml
version: 2

models:
  - name: stg_orders
    description: |
      Cleaned orders from raw.orders.
      Casts string timestamps to TIMESTAMP.
      Adds derived columns: is_late, delivery_days, purchase_month.
      One row per order_id.
    columns:
      - name: order_id
        description: "Primary key — unique order identifier"
        tests:
          - unique
          - not_null
      - name: customer_id
        description: "Foreign key to stg_customers"
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: order_status
        description: "Current order status"
        tests:
          - accepted_values:
              values: ['delivered', 'shipped', 'canceled', 'unavailable',
                       'invoiced', 'processing', 'created', 'approved']
      - name: is_late
        description: "True if actual delivery exceeded estimated delivery"
        tests:
          - accepted_values:
              values: [true, false]
              quote: false
              config:
                where: "is_late IS NOT NULL"
      - name: delivery_days
        description: "Days from purchase to delivery (NULL if not delivered)"

  - name: stg_customers
    description: |
      Customer dimension from raw.customers.
      One row per customer_id (not customer_unique_id —
      a unique customer can place orders with different customer_ids).
    columns:
      - name: customer_id
        description: "Primary key — per-order customer identifier"
        tests:
          - unique
          - not_null
      - name: customer_unique_id
        description: "True unique customer identifier across orders"
        tests:
          - not_null
      - name: customer_state
        description: "Brazilian state abbreviation (e.g. SP, RJ, MG)"

  - name: stg_order_payments
    description: |
      Payments aggregated to one row per order.
      Raw table has multiple rows per order (instalments, vouchers).
      This model solves the double-counting problem.
    columns:
      - name: order_id
        description: "Primary key — one row per order"
        tests:
          - unique
          - not_null
      - name: total_payment
        description: "Sum of all payment values for this order"
        tests:
          - not_null
      - name: payment_installments
        description: "Number of payment records for this order"
      - name: payment_types
        description: "Comma-separated list of payment methods used"
```

---

### EXERCISE 2 — Expand marts/schema.yml (Block B)
**[mart_customer_ltv provided. mart_order_metrics — write yourself]**

Replace `models/marts/schema.yml`:

```yaml
version: 2

models:
  - name: mart_customer_ltv
    description: |
      Customer Lifetime Value — one row per unique customer.
      Aggregates all orders, payments and reviews per customer.
      Used by:
        - Streamlit dashboard (Customers page)
        - ML churn prediction model
        - KMeans customer segmentation
      Key metrics: total_spent, is_churned, value_segment, avg_review_score.
    columns:
      - name: customer_unique_id
        description: "Primary key — unique customer across all orders"
        tests:
          - unique
          - not_null
      - name: total_orders
        description: "Number of orders placed by this customer"
        tests:
          - not_null
      - name: total_spent
        description: "Lifetime spend in BRL (Brazilian Reais)"
        tests:
          - not_null
      - name: value_segment
        description: "Spend-based segment: Bronze/Silver/Gold/Platinum"
        tests:
          - accepted_values:
              values: ['Bronze', 'Silver', 'Gold', 'Platinum']
      - name: is_churned
        description: "1 if single-purchase customer, 0 if repeat buyer"
        tests:
          - accepted_values:
              values: [0, 1]
              quote: false

  - name: mart_order_metrics
    description: |
      YOUR TASK: Write a description for mart_order_metrics.
      Include:
        - What the model contains (one sentence)
        - Where the data comes from (refs used)
        - Who uses this model
        - Key columns to highlight
    columns:
      - name: order_id
        description: "Primary key"
        tests:
          - unique
          - not_null
      # YOUR TASK: Add tests for at least 3 more columns:
      # - is_late: accepted_values [true, false] where not null
      # - review_score: accepted_values [1,2,3,4,5] where not null
      # - total_payment: not_null
```

---

### EXERCISE 3 — 2 Custom Singular Tests (Block C)
**[Test 1 provided. Test 2 write yourself]**

Create `sprint-08/day-51/ecommerce_dbt/tests/assert_no_negative_payments.sql`:

```sql
-- Custom test: no order should have negative total payment
-- This test FAILS if any rows are returned

SELECT
    order_id,
    total_payment
FROM {{ ref('stg_order_payments') }}
WHERE total_payment < 0
```

Create `tests/assert_late_orders_have_delivery_date.sql` — **WRITE THIS YOURSELF:**
```sql
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
```

---

### EXERCISE 4 — dbt test + docs (Block D)

```bash
cd sprint-08/day-51/ecommerce_dbt

# Run all tests
dbt test

# Expected summary:
# PASS=XX WARN=0 ERROR=0 SKIP=0

# If any FAIL — check the test name, fix the SQL or YAML

# Generate fresh docs with descriptions
dbt docs generate

# Serve docs (opens browser)
dbt docs serve --port 8580
# Open: http://localhost:8580
# Click: mart_customer_ltv → see description + column docs + lineage graph
```

---

### EXERCISE 5 — Lineage Graph Exploration (Block E)

In the dbt docs UI at `http://localhost:8580`:

1. Click **"View Lineage Graph"** (bottom right)
2. Find `mart_customer_ltv` — you should see:
   ```
   raw.customers → stg_customers ──────────────────────┐
   raw.orders → stg_orders ────────────────────────────┤→ mart_customer_ltv
   raw.order_payments → stg_order_payments ────────────┘
   ```
3. Click any model → see its SQL, description, tests, columns
4. Take a screenshot for your portfolio README

---

### EXERCISE 6 — Git Push

```bash
cd C:\90_day_python_de_plan
python scripts/daily_commit.py --day 52 --sprint 8 ^
    --message "dbt: expanded tests 20+, full model documentation, 2 custom tests, lineage graph" ^
    --merge
```

---

## ✅ DAY 52 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | mart_customer_ltv running (from Day 51 fix) | [ ] |
| 2 | staging/schema.yml has relationship test (customer_id FK) | [ ] |
| 3 | **mart_order_metrics description written** | [ ] |
| 4 | **mart_order_metrics has 3+ column tests** | [ ] |
| 5 | `assert_no_negative_payments.sql` created | [ ] |
| 6 | **`assert_late_orders_have_delivery_date.sql` written** | [ ] |
| 7 | `dbt test` — all PASS, 0 ERROR | [ ] |
| 8 | `dbt docs generate` completes | [ ] |
| 9 | Lineage graph visible in docs UI | [ ] |
|10 | One clean `[DAY-052][S08]` commit | [ ] |

---

## 🔍 EXPECTED dbt test OUTPUT

```
PASS=22+ WARN=0 ERROR=0

Tests should include:
  PASS unique + not_null on all PKs
  PASS accepted_values on order_status (8 values)
  PASS accepted_values on value_segment (4 values)
  PASS accepted_values on is_churned (0, 1)
  PASS relationships: stg_orders.customer_id → stg_customers.customer_id
  PASS assert_no_negative_payments (custom)
  PASS assert_late_orders_have_delivery_date (custom)
```

---

## ⚠️ WATCH OUT FOR

**relationship test syntax:**
```yaml
# The 'to' field uses ref() — but in YAML, no {{ }}:
- relationships:
    to: ref('stg_customers')    # correct
    field: customer_id
```

**accepted_values for boolean:**
```yaml
# PostgreSQL booleans need quote: false
- accepted_values:
    values: [true, false]
    quote: false                # without this, dbt quotes as strings
    config:
      where: "is_late IS NOT NULL"  # skip NULLs (undelivered orders)
```

**Custom test returns rows = FAIL:**
```
If your custom test returns 0 rows → PASS (no violations found)
If your custom test returns any rows → FAIL (violations exist)
Always write SELECT of the violations, not SELECT of the passing rows
```

---

## 🔜 PREVIEW: DAY 53

**Topic:** dbt incremental models + snapshots  
**What you'll do:** Convert `mart_order_metrics` to an incremental model
(only processes new orders on each run), create a snapshot of `stg_orders`
to track status changes over time, run `dbt snapshot` and query the history table.

---

*Day 52 | Sprint 08 | EP-11 | TASK-052*
