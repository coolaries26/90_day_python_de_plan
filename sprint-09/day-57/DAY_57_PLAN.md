# 📅 DAY 57 — Sprint 09 | Great Expectations Setup + First Expectations
## Install GX, Connect to PostgreSQL, Write Expectations on ecommerce_db

---

## 🔁 SPRINT 08 CLOSE — Confirmed

```
Sprint 08 test: 93/100
All 8 sprint tags: ✅ (after main upstream fix)
```

### Fix main upstream + create sprint-08 tag if not done
```bash
cd C:\90_day_python_de_plan
git checkout main
git push --set-upstream origin main
git tag sprint-08-complete
git push origin sprint-08-complete
git checkout develop
git pull origin develop
git checkout -b sprint-09/day-57-great-expectations-setup
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-12: Data Quality & Great Expectations |
| Story           | ST-57: GX Setup + First Expectations |
| Task ID         | TASK-057 |
| Sprint          | Sprint 09 (Days 57–64) |
| Story Points    | 3 |
| Priority        | HIGH |
| Labels          | great-expectations, data-quality, gx, ecommerce, day-57 |
| Acceptance Criteria | GX installed; data source connected to ecommerce_db; 10+ expectations on mart_customer_ltv; validation run passes |

---

## 📚 BACKGROUND

### What is Great Expectations?

```
Great Expectations (GX) is the industry-standard data quality framework.
Used by: Airbnb, Comcast, Superside, hundreds of data teams.

Without GX (what you have now):
  - dbt tests: unique, not_null, accepted_values (schema-level)
  - Custom SQL: assert_no_negative_payments (hand-written)
  - No statistical tests, no distribution checks, no drift detection

With GX:
  - Expectation suites: 100+ built-in expectation types
  - Statistical tests: min/max/mean/std within expected range
  - Distribution checks: % of values in each category
  - Data docs: auto-generated HTML report of all validation results
  - Checkpoints: run validations on a schedule (integrates with Airflow)

GX Expectation examples:
  expect_column_values_to_not_be_null
  expect_column_values_to_be_between          ← min/max range
  expect_column_mean_to_be_between            ← statistical
  expect_column_value_lengths_to_be_between   ← string length
  expect_column_proportion_of_unique_values_to_be_between
  expect_table_row_count_to_be_between        ← row count guard
  expect_column_values_to_match_regex         ← pattern matching
```

### GX Core Concepts

```
Data Source    → connection to your data (PostgreSQL, CSV, etc.)
Data Asset     → a specific table or query
Batch Request  → a slice of data to validate (whole table or partition)
Expectation    → a single assertion about the data
Expectation Suite → a named collection of expectations
Validator      → applies expectations to a batch
Checkpoint     → runs a suite against data + saves results
Data Docs      → HTML report of all validation results
```

### GX Version Note

```
GX has two major APIs:
  GX 0.15.x  → "classic" API (older, more docs online)
  GX 1.x     → "fluent" API (newer, cleaner but less documentation)

We use GX 0.18.x — stable, well-documented, works with Python 3.12
```

---

## 🎯 OBJECTIVES

1. Install `great-expectations`
2. Initialise GX project (`great_expectations init`)
3. Connect GX to ecommerce_db PostgreSQL
4. Write 10+ expectations on `dbt_dev_marts.mart_customer_ltv`
5. Run validation and view Data Docs HTML report
6. Push clean `[DAY-057][S09]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 20 min | Install + init + connect datasource |
| B | 40 min | Write expectations suite |
| C | 20 min | Run validation + view Data Docs |
| D | 20 min | Fix any failures + rerun |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Install + Init (Block A)

```bash
cd C:\90_day_python_de_plan
.venv\Scripts\activate

# Install GX (pinned version for stability)
pip install great-expectations==0.18.19

# Verify
python -c "import great_expectations as gx; print(gx.__version__)"
# Expected: 0.18.x

# Create GX project directory
mkdir sprint-09\day-57\gx_project
cd sprint-09\day-57\gx_project

# Initialise GX
great_expectations init
# This creates:
#   great_expectations/
#     great_expectations.yml   ← main config
#     expectations/            ← expectation suites stored here
#     checkpoints/             ← checkpoint configs
#     uncommitted/             ← local data docs + validation results
```

---

### EXERCISE 2 — Connect to ecommerce_db (Block A)
**[Fully provided]**

Create `sprint-09/day-57/setup_gx.py`:

```python
#!/usr/bin/env python3
"""
setup_gx.py — Day 57 | Great Expectations Setup
=================================================
Connects GX to ecommerce_db and creates the first expectation suite.

Run: python sprint-09/day-57/setup_gx.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))

from logger import get_pipeline_logger

logger = get_pipeline_logger("setup_gx")

GX_DIR = Path(__file__).parent / "gx_project" / "great_expectations"


def get_context() -> gx.DataContext:
    """Load GX data context from project directory."""
    return gx.data_context.DataContext(context_root_dir=str(GX_DIR))


def add_datasource(context: gx.DataContext) -> None:
    """Add ecommerce_db as a GX datasource."""
    datasource_config = {
        "name": "ecommerce_db",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": (
                "postgresql+psycopg2://appuser:AppUser%402024%21"
                "@127.0.0.1:5432/ecommerce_db"
            ),
        },
        "data_connectors": {
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetSqlDataConnector",
                "include_schema_name": True,
            },
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
        },
    }

    # Add if not already present
    try:
        context.add_datasource(**datasource_config)
        logger.info("Datasource 'ecommerce_db' added")
    except Exception:
        logger.info("Datasource 'ecommerce_db' already exists")


def get_validator(context: gx.DataContext, table: str, schema: str) -> gx.validator.validator.Validator:
    """Get a GX validator for a specific table."""
    batch_request = BatchRequest(
        datasource_name="ecommerce_db",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name=f"{schema}.{table}",
    )
    return context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=f"{schema}.{table}",
        create_expectation_suite_if_not_exists=True,
    )


def main() -> None:
    logger.info("Setting up Great Expectations...")
    context = get_context()
    add_datasource(context)

    # Test connection by getting a validator
    validator = get_validator(context, "mart_customer_ltv", "dbt_dev_marts")
    logger.info(f"Validator ready: {validator.active_batch_definition.data_asset_name}")
    logger.info("✅ GX setup complete")


if __name__ == "__main__":
    main()
```

---

### EXERCISE 3 — Write Expectations Suite (Block B)
**[E1-E6 provided. E7-E10 write yourself]**

Create `sprint-09/day-57/customer_ltv_expectations.py`:

```python
#!/usr/bin/env python3
"""
customer_ltv_expectations.py — Day 57 | Expectation Suite
==========================================================
Writes 10+ expectations on dbt_dev_marts.mart_customer_ltv.
Covers: completeness, range checks, categorical values,
        statistical properties, row count.

Run: python sprint-09/day-57/customer_ltv_expectations.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
from logger import get_pipeline_logger

logger = get_pipeline_logger("customer_ltv_expectations")

GX_DIR = Path(__file__).parent / "gx_project" / "great_expectations"


def build_suite() -> None:
    context = gx.data_context.DataContext(context_root_dir=str(GX_DIR))

    batch_request = BatchRequest(
        datasource_name="ecommerce_db",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="dbt_dev_marts.mart_customer_ltv",
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="mart_customer_ltv.basic",
        create_expectation_suite_if_not_exists=True,
    )

    # ── E1: Table row count ────────────────────────────────────────────────
    # Expect between 90k and 110k customers
    validator.expect_table_row_count_to_be_between(
        min_value=90_000, max_value=110_000
    )

    # ── E2: Primary key not null ───────────────────────────────────────────
    validator.expect_column_values_to_not_be_null(
        column="customer_unique_id"
    )

    # ── E3: Primary key unique ─────────────────────────────────────────────
    validator.expect_column_values_to_be_unique(
        column="customer_unique_id"
    )

    # ── E4: total_spent range ─────────────────────────────────────────────
    # All customers should have spent > 0 and < 20,000 BRL
    validator.expect_column_values_to_be_between(
        column="total_spent",
        min_value=0.01,
        max_value=20_000,
    )

    # ── E5: value_segment categorical values ──────────────────────────────
    validator.expect_column_values_to_be_in_set(
        column="value_segment",
        value_set=["Bronze", "Silver", "Gold", "Platinum"],
    )

    # ── E6: is_churned only 0 or 1 ───────────────────────────────────────
    validator.expect_column_values_to_be_in_set(
        column="is_churned",
        value_set=[0, 1],
    )

    # ── E7: avg_review_score range — WRITE THIS YOURSELF ─────────────────
    # Expect avg_review_score between 1.0 and 5.0
    # Where not null (some customers have no reviews)
    # HINT: validator.expect_column_values_to_be_between(
    #     column="avg_review_score",
    #     min_value=1.0, max_value=5.0,
    #     mostly=0.99,   ← allow 1% outside range (edge cases)
    # )
    # YOUR CODE HERE

    # ── E8: total_orders positive integer — WRITE THIS YOURSELF ──────────
    # All customers should have at least 1 order
    # HINT: expect_column_values_to_be_between(min_value=1)
    # YOUR CODE HERE

    # ── E9: avg_order_value statistical check — WRITE THIS YOURSELF ───────
    # Mean avg_order_value should be between 100 and 300 BRL
    # (statistical expectation — checks the column mean, not individual values)
    # HINT: validator.expect_column_mean_to_be_between(
    #     column="avg_order_value",
    #     min_value=100, max_value=300,
    # )
    # YOUR CODE HERE

    # ── E10: Platinum customers proportion — WRITE THIS YOURSELF ──────────
    # Platinum customers should be < 5% of all customers (they're rare/valuable)
    # HINT: validator.expect_column_proportion_of_unique_values_to_be_between
    # ACTUALLY: use a custom SQL expectation or:
    # validator.expect_column_values_to_be_in_set with mostly parameter
    # OR: expect the proportion of 'Platinum' to be within a range
    #
    # Simpler approach:
    # validator.expect_column_value_z_scores_to_be_less_than(...)
    # OR just use expect_column_values_to_be_in_set with mostly=0.97
    # to confirm that 97%+ of customers are NOT Platinum
    #
    # Alternative — check that Platinum count is reasonable:
    # validator.expect_column_values_to_be_in_set(
    #     column="value_segment",
    #     value_set=["Bronze", "Silver", "Gold"],  # exclude Platinum
    #     mostly=0.95,   # at least 95% are non-Platinum
    # )
    # YOUR CODE HERE
    validator
    
    # Save the expectation suite
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("Expectation suite saved: mart_customer_ltv.basic")

    # Run validation
    logger.info("\nRunning validation...")
    results = validator.validate()

    # Summary
    stats = results.statistics
    logger.info(f"\n── Validation Results ───────────────────────")
    logger.info(f"  Evaluated:  {stats['evaluated_expectations']}")
    logger.info(f"  Successful: {stats['successful_expectations']}")
    logger.info(f"  Failed:     {stats['unsuccessful_expectations']}")
    logger.info(f"  Success %:  {stats['success_percent']:.1f}%")

    # Build Data Docs
    context.build_data_docs()
    logger.info(f"\nData Docs generated at:")
    logger.info(f"  {GX_DIR}/uncommitted/data_docs/local_site/index.html")
    logger.info("Open that file in your browser to see the HTML report")

    return results


if __name__ == "__main__":
    build_suite()
```

---

### EXERCISE 4 — Run + View Data Docs (Block C + D)

```bash
# Run the expectations
python sprint-09/day-57/customer_ltv_expectations.py

# Open Data Docs in browser:
# Windows: open the file directly
start sprint-09\day-57\gx_project\great_expectations\uncommitted\data_docs\local_site\index.html

# OR copy path and paste in browser:
# C:\90_day_python_de_plan\sprint-09\day-57\gx_project\great_expectations\uncommitted\data_docs\local_site\index.html
```

**Expected validation output:**
```
Evaluated:   10
Successful:  10
Failed:       0
Success %:  100.0%
```

If any expectations fail — read the failure message and either:
- Fix the expectation threshold (if your assumption was wrong)
- Note it as a data quality finding (if the data is genuinely bad)

---

### EXERCISE 5 — Git Push

```bash
cd C:\90_day_python_de_plan
python scripts/daily_commit.py --day 57 --sprint 9 ^
    --message "Great Expectations: GX setup, ecommerce_db datasource, 10 expectations on mart_customer_ltv, Data Docs" ^
    --merge
```

---

## ✅ DAY 57 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | `great-expectations==0.18.19` installed | [ ] |
| 2 | GX project initialised in gx_project/ | [ ] |
| 3 | `ecommerce_db` datasource connected | [ ] |
| 4 | E1-E6 expectations run (provided) | [ ] |
| 5 | **E7: avg_review_score range written** | [ ] |
| 6 | **E8: total_orders ≥ 1 written** | [ ] |
| 7 | **E9: avg_order_value mean check written** | [ ] |
| 8 | **E10: Platinum proportion check written** | [ ] |
| 9 | All 10 expectations pass | [ ] |
|10 | Data Docs HTML opens in browser | [ ] |
|11 | One clean `[DAY-057][S09]` commit | [ ] |

---

## 🔍 EXPECTED VALIDATION OUTPUT

```
expect_table_row_count_to_be_between          PASS  96,218 rows
expect_column_values_to_not_be_null           PASS  0 nulls in customer_unique_id
expect_column_values_to_be_unique             PASS  0 duplicates
expect_column_values_to_be_between (spend)    PASS  all values 0-20k
expect_column_values_to_be_in_set (segment)   PASS  Bronze/Silver/Gold/Platinum only
expect_column_values_to_be_in_set (churned)   PASS  0 or 1 only
expect_column_values_to_be_between (review)   PASS  1.0-5.0
expect_column_values_to_be_between (orders)   PASS  ≥ 1
expect_column_mean_to_be_between (order val)  PASS  mean between 100-300
expect_column_values_to_be_in_set (Platinum)  PASS  ≥95% non-Platinum
```

---

## ⚠️ WATCH OUT FOR

**GX directory path on Windows:**
```python
# Use raw string or forward slashes in paths:
GX_DIR = Path(__file__).parent / "gx_project" / "great_expectations"
# NOT: "sprint-09\day-57\gx_project\..."
```

**Connection string password encoding:**
```python
# @ in password must be URL-encoded as %40:
"postgresql+psycopg2://appuser:AppUser%402024%21@127.0.0.1:5432/ecommerce_db"
#                                    ^^^^                 ^^^
#                                    @                    !
```

**GX version compatibility:**
```
great-expectations 0.18.x uses the "classic" Datasource API
(BatchRequest, InferredAssetSqlDataConnector)
GX 1.x uses a different "fluent" API — don't mix them
```

---

## 🔜 PREVIEW: DAY 58

**Topic:** Expectation suites on all analytics tables + order_metrics  
**What you'll do:** Write expectation suites for `mart_order_metrics`
and `analytics.seller_performance`. Focus on business-rule expectations
(delivery days realistic, review scores valid, revenue positive).
Create a GX checkpoint that runs all suites in one command.

---

*Day 57 | Sprint 09 | EP-12 | TASK-057*
