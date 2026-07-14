# 📅 DAY 59 — Sprint 09 | GX Checkpoints
## Run All Suites in One Command, Save Results, Checkpoint Config

---

## 🔁 RETROSPECTIVE — Day 58

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| order_metrics 11/11 | ✅ Pass | You added extra expectation |
| seller_performance 8/8 | ✅ Pass | |
| delivery_days mean = 12.09 | ✅ Pass | Matches Day 44 finding |
| review_score mean = 4.16 | ✅ Pass | Consistent |
| 29 total expectations | ✅ Pass | Across 3 suites |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-09/day-59-checkpoints
```

---

## 🗂️ JIRA CARD

| Field           | Value |
|-----------------|-------|
| Epic            | EP-12: Data Quality & Great Expectations |
| Story           | ST-59: GX Checkpoints |
| Task ID         | TASK-059 |
| Sprint          | Sprint 09 (Days 57–64) |
| Story Points    | 2 |
| Priority        | HIGH |
| Labels          | great-expectations, checkpoint, data-quality, day-59 |
| Acceptance Criteria | Checkpoint created; all 3 suites run via single checkpoint.run(); results saved to Data Docs |

---

## 📚 BACKGROUND

### What is a GX Checkpoint?

```
Problem without checkpoints:
  You have 3 expectation suites — you run 3 separate Python scripts.
  In production: 10+ suites, each needing manual triggering.
  No unified results view, no single pass/fail signal.

With checkpoints:
  One checkpoint config references all suites + data assets.
  checkpoint.run() → runs all validations → saves all results.
  Returns a single CheckpointResult with overall pass/fail.
  Integrates cleanly with Airflow (one BashOperator or PythonOperator).

checkpoint.run() returns:
  result.success          → True if ALL validations passed
  result.run_results      → dict of individual suite results
  result.list_validation_results() → detailed per-suite breakdown
```

### Checkpoint Config Structure

```python
checkpoint_config = {
    "name": "ecommerce_data_quality",
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": {...},            # which data
            "expectation_suite_name": "...",   # which suite
        },
        # one entry per suite
    ],
}
```

---

## 🎯 OBJECTIVES

1. Create `ecommerce_data_quality` checkpoint with all 3 suites
2. Run checkpoint — verify all 29 expectations pass
3. Write `run_checkpoint.py` — reusable runner script
4. Verify results saved to Data Docs
5. Push clean `[DAY-059][S09]`

---

## ⏱️ TIME BUDGET (1.5 hrs)

| Block | Duration | Activity |
|-------|----------|----------|
| A | 10 min | Branch setup |
| B | 40 min | `create_checkpoint.py` |
| C | 20 min | `run_checkpoint.py` |
| D | 10 min | Verify Data Docs |
| E | 20 min | Git push |

---

## 📝 EXERCISES

---

### EXERCISE 1 — create_checkpoint.py (Block B)
**[Fully provided]**

Create `sprint-09/day-59/create_checkpoint.py`:

```python
#!/usr/bin/env python3
"""
create_checkpoint.py — Day 59 | GX Checkpoint Creation
=======================================================
Creates a checkpoint that runs all 3 expectation suites
in a single call.

Run once: python sprint-09/day-59/create_checkpoint.py
Then use: python sprint-09/day-59/run_checkpoint.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
from logger import get_pipeline_logger

logger = get_pipeline_logger("create_checkpoint")

GX_DIR = Path(__file__).resolve().parent.parent / "day-57" / "gx_project" / "great_expectations"

CHECKPOINT_NAME = "ecommerce_data_quality"


def make_batch_request(schema: str, table: str) -> dict:
    """Build batch request dict for checkpoint config."""
    return {
        "datasource_name": "ecommerce_db",
        "data_connector_name": "default_inferred_data_connector_name",
        "data_asset_name": f"{schema}.{table}",
    }


def create_checkpoint() -> None:
    context = gx.data_context.DataContext(context_root_dir=str(GX_DIR))

    checkpoint_config = {
        "name": CHECKPOINT_NAME,
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y%m%d-%H%M%S-ecommerce-dq",
        "validations": [
            {
                "batch_request": make_batch_request("dbt_dev_marts", "mart_customer_ltv"),
                "expectation_suite_name": "mart_customer_ltv.basic",
            },
            {
                "batch_request": make_batch_request("dbt_dev_marts", "mart_order_metrics"),
                "expectation_suite_name": "mart_order_metrics.basic",
            },
            {
                "batch_request": make_batch_request("analytics", "seller_performance"),
                "expectation_suite_name": "analytics.seller_performance.basic",
            },
        ],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
    }

    # Delete existing checkpoint if present
    try:
        context.delete_checkpoint(CHECKPOINT_NAME)
        logger.info(f"Deleted existing checkpoint: {CHECKPOINT_NAME}")
    except Exception:
        pass

    context.add_checkpoint(**checkpoint_config)
    logger.info(f"Checkpoint created: {CHECKPOINT_NAME}")
    logger.info(f"Validations: {len(checkpoint_config['validations'])} suites")


if __name__ == "__main__":
    create_checkpoint()
    logger.info("✅ Run: python sprint-09/day-59/run_checkpoint.py")
```

---

### EXERCISE 2 — run_checkpoint.py (Block C)
**[Write yourself — hints given]**

Create `sprint-09/day-59/run_checkpoint.py`:

```python
#!/usr/bin/env python3
"""
run_checkpoint.py — Day 59 | Run GX Checkpoint
===============================================
YOUR TASK: Run the ecommerce_data_quality checkpoint
and print a summary of results.

HINTS:
Step 1: Load context
    context = gx.data_context.DataContext(context_root_dir=str(GX_DIR))

Step 2: Run checkpoint
    result = context.run_checkpoint(checkpoint_name="ecommerce_data_quality")

Step 3: Print overall result
    logger.info(f"Overall success: {result.success}")

Step 4: Print per-suite results
    for validation_result in result.list_validation_results():
        suite = validation_result.meta["expectation_suite_name"]
        stats = validation_result.statistics
        status = "✅" if validation_result.success else "❌"
        logger.info(
            f"  {status} {suite}: "
            f"{stats['successful_expectations']}/{stats['evaluated_expectations']} "
            f"({stats['success_percent']:.0f}%)"
        )

Step 5: Build Data Docs
    context.build_data_docs()
    logger.info("Data Docs updated")

Step 6: Return exit code based on success
    import sys
    sys.exit(0 if result.success else 1)
    # Exit code 1 = failure → Airflow will catch this as task failure
"""
# YOUR CODE HERE
```

---

### EXERCISE 3 — Run + Verify

```bash
# Step 1: Create the checkpoint
python sprint-09/day-59/create_checkpoint.py

# Step 2: Run it
python sprint-09/day-59/run_checkpoint.py

# Expected output:
# Overall success: True
#   ✅ mart_customer_ltv.basic:          10/10 (100%)
#   ✅ mart_order_metrics.basic:         11/11 (100%)
#   ✅ analytics.seller_performance.basic: 8/8 (100%)
# Data Docs updated

# Step 3: Verify Data Docs shows all 3 runs
start sprint-09\day-57\gx_project\great_expectations\uncommitted\data_docs\local_site\index.html
```

---

### EXERCISE 4 — Git Push

```bash
python scripts/daily_commit.py --day 59 --sprint 9 ^
    --message "GX Checkpoint: ecommerce_data_quality runs all 3 suites, 29/29 expectations pass" ^
    --merge
```

---

## ✅ DAY 59 COMPLETION CHECKLIST

| # | Task | Done? |
|---|------|-------|
| 1 | `create_checkpoint.py` runs — checkpoint saved | [ ] |
| 2 | **`run_checkpoint.py` written — runs all 3 suites** | [ ] |
| 3 | Overall success: True | [ ] |
| 4 | Per-suite results printed | [ ] |
| 5 | `sys.exit(0 if success else 1)` — correct exit code | [ ] |
| 6 | Data Docs shows 3 validation runs | [ ] |
| 7 | One clean `[DAY-059][S09]` commit | [ ] |

---

## ⚠️ WATCH OUT FOR

**`list_validation_results()` method:**
In GX 0.18.x this may be `result.run_results.values()` instead:
```python
# If list_validation_results() doesn't work:
for key, val in result.run_results.items():
    vr = val["validation_result"]
    suite = vr.meta["expectation_suite_name"]
    stats = vr.statistics
    status = "✅" if vr.success else "❌"
    logger.info(f"  {status} {suite}: {stats['successful_expectations']}/{stats['evaluated_expectations']}")
```

**Exit code for Airflow:**
```python
import sys
sys.exit(0 if result.success else 1)
```
When this script runs in Airflow via BashOperator, exit code 1 causes the task to fail — which is exactly what you want when data quality fails.

---

## 🔜 PREVIEW: DAY 60

**Topic:** Airflow + GX integration  
**What you'll do:** Add a GX checkpoint task to `dag_ecommerce_etl`
that runs after dbt completes. If any expectation fails, the task
fails and `on_failure_callback` logs it to `etl_audit_log`.

---

*Day 59 | Sprint 09 | EP-12 | TASK-059*
