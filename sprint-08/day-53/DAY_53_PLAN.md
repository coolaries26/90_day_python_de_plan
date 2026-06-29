# 📅 DAY 53 — Sprint 08 | dbt Incremental Models + Snapshots
## Only Process New Data, Track Historical Changes, SCD Type 2

---

## 🔁 RETROSPECTIVE — Day 52

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| 31/31 tests passing | ✅ Pass | Including 2 custom tests |
| FK relationship test | ✅ Pass | stg_orders → stg_customers |
| assert_late_orders | ✅ Pass | Custom singular test |
| docs generated | ✅ Pass | Lineage graph visible |
| Unicode/profile fix | ✅ Pass | TEMP profile resolved |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-08/day-53-incremental-snapshots
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-11: Advanced SQL & dbt |
| Story           | ST-53: dbt Incremental + Snapshots |
| Task ID         | TASK-053 |
| Sprint          | Sprint 08 (Days 49–56) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | dbt, incremental, snapshots, scd2, day-53 |
| Acceptance Criteria | mart_order_metrics converted to incremental; snapshot of stg_orders working; dbt snapshot creates history table |

---

## 📚 BACKGROUND

### Why Incremental Models?

```
Current mart_order_metrics (table materialisation):
  Every dbt run → DELETE all 96,588 rows → INSERT 96,588 rows
  For 96k rows: fast enough
  For 100M rows: takes hours, wastes compute

Incremental materialisation:
  First run   → full load (same as table)
  Subsequent  → only INSERT/UPDATE rows WHERE updated_at > last_run
  For 100M rows: processes only today's new rows (maybe 50k)
  10-100x faster in production

Real-world use:
  - Event tables (clicks, pageviews) — millions of rows/day
  - Order tables — new orders daily
  - Log tables — append-only
```

### Incremental Model Syntax

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',          -- used to UPSERT (update if exists)
    on_schema_change='sync_all_columns'
) }}

SELECT ...
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    -- This block only runs on incremental runs (not the first full run)
    WHERE purchased_at > (
        SELECT MAX(purchased_at) FROM {{ this }}
    )
{% endif %}
```

### dbt Snapshots — SCD Type 2

```
SCD Type 2 (Slowly Changing Dimension):
  Track HOW data changes over time, not just current state

Example: order_status changes
  2024-01-01: order_id=123, status='processing'
  2024-01-02: order_id=123, status='shipped'
  2024-01-03: order_id=123, status='delivered'

Without snapshot: you only see 'delivered' (current state)
With snapshot:    you see ALL three states with timestamps

dbt snapshot creates:
  dbt_scd_stg_orders table with columns:
    order_id, order_status, ...,
    dbt_scd_id (hash),
    dbt_valid_from (when this version became active),
    dbt_valid_to   (when it was superseded, NULL if current),
    dbt_is_current (True/False)
```

---

## 🎯 OBJECTIVES

1. Convert `mart_order_metrics` to incremental materialisation
2. Verify incremental run only processes new rows
3. Create `orders_snapshot.sql` — track order status changes
4. Run `dbt snapshot` and query the history table
5. Write a query showing orders that changed status
6. Push clean `[DAY-053][S08]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 10 min | Branch setup |
| B | 35 min | Convert mart_order_metrics to incremental |
| C | 40 min | Create + run orders_snapshot |
| D | 20 min | Query snapshot history |
| E | 15 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Convert mart_order_metrics to Incremental (Block B)
**[Config provided. WHERE clause — write yourself]**

Open `models/marts/mart_order_metrics.sql` and update the config + add incremental filter:

```sql
-- Replace the existing config block:
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns'
) }}

WITH order_data AS (
    SELECT
        o.order_id,
        c.customer_unique_id,
        o.order_status,
        o.purchased_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.delivery_days,
        o.is_late,
        p.total_payment,
        p.payment_types,
        r.review_score,
        items.product_count
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    JOIN {{ ref('stg_order_payments') }} p
        ON o.order_id = p.order_id
    LEFT JOIN (
        SELECT order_id,
               ROUND(AVG(review_score)::numeric, 2) AS review_score
        FROM {{ source('raw', 'order_reviews') }}
        GROUP BY order_id
    ) r ON o.order_id = r.order_id
    JOIN (
        SELECT order_id, COUNT(*) AS product_count
        FROM {{ source('raw', 'order_items') }}
        GROUP BY order_id
    ) items ON o.order_id = items.order_id
    WHERE o.order_status = 'delivered'

    -- YOUR TASK: Add incremental filter below
    -- HINTS:
    -- {% if is_incremental() %}
    --     AND o.purchased_at > (
    --         SELECT COALESCE(MAX(purchased_at), '2000-01-01'::timestamp)
    --         FROM {{ this }}
    --     )
    -- {% endif %}
    --
    -- {{ this }} refers to the existing mart_order_metrics table
    -- COALESCE handles the case where the table is empty on first run
    -- This means: "only process orders newer than what we already have"
)
SELECT * FROM order_data
```

**Test incremental run:**
```bash
# First: full refresh (rebuild from scratch)
dbt run --select mart_order_metrics --full-refresh \
    --profiles-dir "C:\90_day_python_de_plan\.dbt"

# Check row count
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()
df = pd.read_sql('SELECT COUNT(*) FROM dbt_dev_marts.mart_order_metrics', engine)
print(f'Full refresh rows: {df.iloc[0,0]:,}')
dispose_ecommerce_engine()
"

# Second: incremental run (should process 0 new rows since data hasn't changed)
dbt run --select mart_order_metrics \
    --profiles-dir "C:\90_day_python_de_plan\.dbt"

# The log should show something like:
# mart_order_metrics: INSERT 0
# (0 new rows because all data already loaded)
```

---

### EXERCISE 2 — Create orders_snapshot.sql (Block C)
**[Fully provided]**

Create `sprint-08/day-51/ecommerce_dbt/snapshots/orders_snapshot.sql`:

```sql
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
```

**Run the snapshot:**
```bash
dbt snapshot --profiles-dir "C:\90_day_python_de_plan\.dbt"

# Expected output:
# 1 of 1 START snapshot snapshots.orders_snapshot .... [RUN]
# 1 of 1 OK snapshot snapshots.orders_snapshot ....... [INSERT X in Xs]

# Check what was created in PostgreSQL
python -c "
import sys; sys.path.insert(0, 'capstone')
from db import get_ecommerce_engine, dispose_ecommerce_engine
import pandas as pd
engine = get_ecommerce_engine()

# Check snapshot table structure
df = pd.read_sql('''
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'snapshots'
      AND table_name = 'orders_snapshot'
    ORDER BY ordinal_position
''', engine)
print('Snapshot columns:')
print(df.to_string(index=False))

# Check row count
cnt = pd.read_sql('SELECT COUNT(*) FROM snapshots.orders_snapshot', engine)
print(f'\nSnapshot rows: {cnt.iloc[0,0]:,}')
dispose_ecommerce_engine()
"
```

---

### EXERCISE 3 — Query Snapshot History (Block D)
**[Write yourself — hints given]**

Create `sprint-08/day-53/snapshot_queries.py`:

```python
#!/usr/bin/env python3
"""
snapshot_queries.py — Day 53 | Query dbt Snapshot History
==========================================================
Demonstrates how to query the snapshot table to see
how order statuses changed over time.
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "capstone"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))

from db import get_ecommerce_engine, dispose_ecommerce_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("snapshot_queries")
engine = get_ecommerce_engine()
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ── Q1: Current state of all orders ──────────────────────────────────────
Q1_CURRENT_STATE = """
-- YOUR SQL HERE
-- HINTS:
-- SELECT order_id, order_status, is_late,
--        dbt_valid_from, dbt_valid_to, dbt_updated_at
-- FROM snapshots.orders_snapshot
-- WHERE dbt_valid_to IS NULL    ← current records only
-- ORDER BY dbt_valid_from DESC
-- LIMIT 10
SELECT 'implement Q1' AS placeholder
"""

# ── Q2: Orders that changed status ────────────────────────────────────────
Q2_STATUS_CHANGES = """
-- YOUR SQL HERE
-- Find order_ids that appear more than once in the snapshot
-- (meaning their status changed at least once)
-- HINTS:
-- WITH status_counts AS (
--     SELECT order_id, COUNT(*) AS version_count
--     FROM snapshots.orders_snapshot
--     GROUP BY order_id
--     HAVING COUNT(*) > 1
-- )
-- SELECT s.order_id, s.order_status,
--        s.dbt_valid_from, s.dbt_valid_to
-- FROM snapshots.orders_snapshot s
-- JOIN status_counts sc ON s.order_id = sc.order_id
-- ORDER BY s.order_id, s.dbt_valid_from
-- LIMIT 20
SELECT 'implement Q2' AS placeholder
"""

# ── Q3: Snapshot summary ──────────────────────────────────────────────────
Q3_SNAPSHOT_SUMMARY = """
-- YOUR SQL HERE
-- Show: total rows, current rows, historical rows, unique orders
-- HINTS:
-- SELECT
--     COUNT(*) AS total_rows,
--     COUNT(CASE WHEN dbt_valid_to IS NULL THEN 1 END) AS current_rows,
--     COUNT(CASE WHEN dbt_valid_to IS NOT NULL THEN 1 END) AS historical_rows,
--     COUNT(DISTINCT order_id) AS unique_orders
-- FROM snapshots.orders_snapshot
SELECT 'implement Q3' AS placeholder
"""


def main():
    logger.info("Snapshot Queries — Day 53")

    for name, sql in [
        ("q1_current_state",   Q1_CURRENT_STATE),
        ("q2_status_changes",  Q2_STATUS_CHANGES),
        ("q3_summary",         Q3_SNAPSHOT_SUMMARY),
    ]:
        try:
            df = pd.read_sql(sql, engine)
            logger.info(f"✅ {name}: {len(df)} rows")
            df.to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
        except Exception as exc:
            logger.error(f"❌ {name}: {exc}")

    dispose_ecommerce_engine()


if __name__ == "__main__":
    main()
```

---

### EXERCISE 4 — Git Push

```bash
cd C:\90_day_python_de_plan
python scripts/daily_commit.py --day 53 --sprint 8 ^
    --message "dbt incremental mart_order_metrics, orders_snapshot SCD2, snapshot history queries" ^
    --merge
```

---

## ✅ DAY 53 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | `mart_order_metrics` config updated to `incremental` | [ ] |
| 2 | **Incremental WHERE clause written** | [ ] |
| 3 | `dbt run --full-refresh` succeeds | [ ] |
| 4 | `dbt run` (incremental) processes 0 new rows | [ ] |
| 5 | `orders_snapshot.sql` created in snapshots/ | [ ] |
| 6 | `dbt snapshot` creates `snapshots.orders_snapshot` | [ ] |
| 7 | **Q1: current state query written** | [ ] |
| 8 | **Q2: status changes query written** | [ ] |
| 9 | **Q3: snapshot summary query written** | [ ] |
| 10 | One clean `[DAY-053][S08]` commit | [ ] |

---

## 🔍 EXPECTED SNAPSHOT OUTPUT

```
Snapshot columns include:
  order_id, order_status, purchased_at, delivered_at,
  dbt_scd_id, dbt_updated_at, dbt_valid_from,
  dbt_valid_to, dbt_is_current

Q3 summary (since data is static — no real status changes):
  total_rows:      ~99,441  (one per order — all current)
  current_rows:    ~99,441  (all dbt_valid_to IS NULL)
  historical_rows: 0        (no changes detected yet)
  unique_orders:   ~99,441

Note: In a live system, historical_rows would grow as statuses change.
Since Olist is a static dataset, the snapshot shows the initial load.
```

---

## ⚠️ WATCH OUT FOR

**Snapshot target_schema permissions:**
```sql
-- If dbt can't create the snapshots schema:
-- Run as postgres:
CREATE SCHEMA IF NOT EXISTS snapshots;
GRANT ALL ON SCHEMA snapshots TO appuser;
```

**Incremental model on first run:**
```
First run with incremental config = full table build (same as table)
Subsequent runs = only new rows
Use --full-refresh flag to force rebuild from scratch:
  dbt run --select mart_order_metrics --full-refresh
```

**`{{ this }}` in incremental models:**
```
{{ this }} = the existing table being updated
It's only available inside {% if is_incremental() %} blocks
Outside that block = compilation error
```

---

## 🔜 PREVIEW: DAY 54

**Topic:** Query optimisation + table partitioning  
**What you'll do:** Add composite indexes on the mart tables, use
`EXPLAIN ANALYZE` to compare before/after, partition `mart_order_metrics`
by `purchase_month` and measure query speedup on date-range filters.

---

*Day 53 | Sprint 08 | EP-11 | TASK-053*
