# 📅 DAY 58 — Sprint 09 | GX Expectations on Order Metrics + Seller Performance
## Business-Rule Expectations, Delivery Quality, Seller Data Quality

---

## 🔁 RETROSPECTIVE — Day 57

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| GX setup | ✅ Pass | |
| ecommerce_db datasource | ✅ Pass | |
| 10/10 expectations | ✅ Pass | 100% success |
| total_spent=0 finding | ✅ Pass | 2 cancelled orders documented |
| avg_review_score mean = 4.09 | ✅ Pass | Useful business metric |
| Data Docs HTML | ✅ Pass | |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-09/day-58-order-seller-expectations
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-12: Data Quality & Great Expectations |
| Story           | ST-58: Expectations on Order + Seller Tables |
| Task ID         | TASK-058 |
| Sprint          | Sprint 09 (Days 57–64) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | great-expectations, data-quality, orders, sellers, day-58 |
| Acceptance Criteria | 10+ expectations on mart_order_metrics; 8+ on seller_performance; all passing; Data Docs updated |

---

## 🎯 OBJECTIVES

1. Write expectation suite for `dbt_dev_marts.mart_order_metrics`
2. Write expectation suite for `analytics.seller_performance`
3. Run both validations
4. Update Data Docs
5. Push clean `[DAY-058][S09]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 10 min | Branch setup |
| B | 45 min | `order_metrics_expectations.py` |
| C | 35 min | `seller_expectations.py` |
| D | 10 min | Run both + Data Docs |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — order_metrics_expectations.py (Block B)
**[E1-E5 provided. E6-E10 write yourself]**

Create `sprint-09/day-58/order_metrics_expectations.py`:

```python
#!/usr/bin/env python3
"""
order_metrics_expectations.py — Day 58
=======================================
Expectation suite for dbt_dev_marts.mart_order_metrics.
Covers delivery time, payment, review score, late delivery.
"""

from __future__ import annotations

import sys
from pathlib import Path
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
from logger import get_pipeline_logger

logger = get_pipeline_logger("order_metrics_expectations")

GX_DIR = Path(__file__).resolve().parent.parent / "day-57" / "gx_project" / "great_expectations"


def build_suite() -> None:
    context = gx.data_context.DataContext(context_root_dir=str(GX_DIR))

    suite_name = "mart_order_metrics.basic"
    try:
        context.get_expectation_suite(suite_name)
        context.delete_expectation_suite(suite_name)
    except Exception:
        pass
    context.add_expectation_suite(expectation_suite_name=suite_name)

    batch_request = BatchRequest(
        datasource_name="ecommerce_db",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="dbt_dev_marts.mart_order_metrics",
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # ── E1: Row count ─────────────────────────────────────────────────────
    # ~96k delivered orders
    validator.expect_table_row_count_to_be_between(
        min_value=90_000, max_value=110_000
    )

    # ── E2: order_id not null and unique ──────────────────────────────────
    validator.expect_column_values_to_not_be_null(column="order_id")
    validator.expect_column_values_to_be_unique(column="order_id")

    # ── E3: delivery_days realistic range ─────────────────────────────────
    # Deliveries should take 1-120 days (some remote regions take months)
    validator.expect_column_values_to_be_between(
        column="delivery_days",
        min_value=1,
        max_value=120,
        mostly=0.999,   # allow tiny fraction of edge cases
    )

    # ── E4: review_score valid range ──────────────────────────────────────
    # Reviews are 1-5 stars, but many orders have no review (NULL)
    validator.expect_column_values_to_be_between(
        column="review_score",
        min_value=1,
        max_value=5,
        mostly=0.99,
    )

    # ── E5: total_payment positive ────────────────────────────────────────
    validator.expect_column_values_to_be_between(
        column="total_payment",
        min_value=0.01,
        max_value=15_000,
        mostly=0.999,
    )

    # ── E6: delivery_days mean check — WRITE THIS YOURSELF ─────────────────
    # Average delivery should be between 8 and 20 days (Brazilian logistics)
    # HINT: expect_column_mean_to_be_between
    # YOUR CODE HERE

    # ── E7: is_late not null for delivered orders — WRITE THIS YOURSELF ────
    # All rows in this table are delivered, so is_late should never be NULL
    # HINT: expect_column_values_to_not_be_null(column="is_late")
    # YOUR CODE HERE

    # ── E8: late delivery rate — WRITE THIS YOURSELF ──────────────────────
    # We know ~8.1% of orders are late — check this stays within bounds
    # HINT: expect_column_mean_to_be_between
    #   is_late is boolean (True/False), mean = proportion of True values
    #   Expected: mean between 0.05 and 0.15 (5-15% late rate)
    # YOUR CODE HERE

    # ── E9: review_score mean — WRITE THIS YOURSELF ───────────────────────
    # Average review score should be between 3.5 and 5.0
    # (we know it's ~4.09 from Day 44 analysis)
    # HINT: expect_column_mean_to_be_between
    # YOUR CODE HERE

    # ── E10: product_count positive — WRITE THIS YOURSELF ─────────────────
    # Every order must have at least 1 product
    # HINT: expect_column_values_to_be_between(min_value=1)
    # YOUR CODE HERE

    # Save + validate
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info(f"Suite saved: {suite_name}")

    results = validator.validate()
    stats = results.statistics

    for result in results.results:
        if not result.success:
            logger.warning(f"❌ FAILED: {result.expectation_config.expectation_type}")
            logger.warning(f"   Column: {result.expectation_config.kwargs.get('column','N/A')}")
            logger.warning(f"   Result: {result.result}")

    logger.info(f"\n── Validation Results ───────────────────────")
    logger.info(f"  Evaluated:  {stats['evaluated_expectations']}")
    logger.info(f"  Successful: {stats['successful_expectations']}")
    logger.info(f"  Failed:     {stats['unsuccessful_expectations']}")
    logger.info(f"  Success %:  {stats['success_percent']:.1f}%")

    context.build_data_docs()
    logger.info("Data Docs updated")


if __name__ == "__main__":
    build_suite()
```

---

### EXERCISE 2 — seller_expectations.py (Block C)
**[Write yourself — follow order_metrics pattern]**

Create `sprint-09/day-58/seller_expectations.py`:

```python
"""
seller_expectations.py — Day 58
================================
YOUR TASK: Write 8 expectations for analytics.seller_performance.

Table columns available:
  seller_id, seller_city, seller_state,
  total_orders, total_revenue, avg_order_value,
  avg_review_score, on_time_delivery_rate,
  total_products_sold, unique_products, active_months

Requirements — write these 8 expectations:
  1. Row count between 2,900 and 3,200
  2. seller_id not null
  3. seller_id unique
  4. total_revenue >= 0
  5. avg_review_score between 1.0 and 5.0 (where not null)
  6. on_time_delivery_rate between 0 and 1 (it's a proportion)
  7. on_time_delivery_rate mean between 0.80 and 0.95
     (we know it's ~85.28% from Day 44)
  8. total_orders >= 1

HINTS:
  - Use same pattern as order_metrics_expectations.py
  - data_asset_name="analytics.seller_performance"
  - suite_name="analytics.seller_performance.basic"
  - For on_time_delivery_rate mean: expect_column_mean_to_be_between
  - For rate between 0-1: expect_column_values_to_be_between(min=0, max=1)
"""
# YOUR CODE HERE
```

---

### EXERCISE 3 — Run both + verify

```bash
python sprint-09/day-58/order_metrics_expectations.py
python sprint-09/day-58/seller_expectations.py

# Expected:
# order_metrics: Evaluated=10, Successful=10, Failed=0
# seller_performance: Evaluated=8, Successful=8, Failed=0
```

---

### EXERCISE 4 — Git Push

```bash
python scripts/daily_commit.py --day 58 --sprint 9 ^
    --message "GX: order_metrics 10 expectations, seller_performance 8 expectations, all passing" ^
    --merge
```

---

## ✅ DAY 58 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | E1-E5 order_metrics provided — run correctly | [ ] |
| 2 | **E6: delivery_days mean written** | [ ] |
| 3 | **E7: is_late not null written** | [ ] |
| 4 | **E8: late delivery rate 5-15% written** | [ ] |
| 5 | **E9: review_score mean written** | [ ] |
| 6 | **E10: product_count ≥ 1 written** | [ ] |
| 7 | order_metrics: 10/10 passing | [ ] |
| 8 | **`seller_expectations.py` written — 8 expectations** | [ ] |
| 9 | seller_performance: 8/8 passing | [ ] |
|10 | Data Docs shows 3 expectation suites | [ ] |
|11 | One clean `[DAY-058][S09]` commit | [ ] |

---

## 🔍 EXPECTED VALUES TO TUNE EXPECTATIONS

```
mart_order_metrics:
  delivery_days mean:    ~12.1 days   → expect 8-20
  is_late proportion:    ~0.081       → expect 0.05-0.15
  review_score mean:     ~4.09        → expect 3.5-5.0
  product_count range:   1-21         → expect ≥ 1

analytics.seller_performance:
  on_time_delivery_rate mean: ~0.8528 → expect 0.80-0.95
  total_revenue range:        0-229k  → expect ≥ 0
  avg_review_score mean:      ~3.97   → check 3.5-5.0
```

---

## 🔜 PREVIEW: DAY 59

**Topic:** GX Checkpoints — run all suites in one command  
**What you'll do:** Create a GX Checkpoint that runs all 3 expectation
suites (customer_ltv, order_metrics, seller_performance) in a single
`checkpoint.run()` call. The checkpoint saves results to Data Docs
and can be triggered from Airflow.

---

*Day 58 | Sprint 09 | EP-12 | TASK-058*
