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

#Step 1: Load context
#    context = gx.data_context.DataContext(context_root_dir=str(GX_DIR))
from __future__ import annotations

import sys
from pathlib import Path
import great_expectations as gx     #type: ignore
from great_expectations.core.batch import BatchRequest  #type: ignore

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
from logger import get_pipeline_logger  #type: ignore

logger = get_pipeline_logger("create_checkpoint")

GX_DIR = Path(__file__).resolve().parent.parent / "day-57" / "gx_project" / "great_expectations"



context = gx.data_context.DataContext(context_root_dir=str(GX_DIR))
#Step 2: Run checkpoint
result = context.run_checkpoint(checkpoint_name="ecommerce_data_quality")
#Step 3: Print overall result
#    logger.info(f"Overall success: {result.success}")

logger.info("✅ Run: python sprint-09/day-59/run_checkpoint.py")
logger.info(f"Overall success: {result.success}")

#Step 4: Print per-suite results
for validation_result in result.list_validation_results():
    suite = validation_result.meta["expectation_suite_name"]
    stats = validation_result.statistics
    status = "✅" if validation_result.success else "❌"
    logger.info(
        f"  {status} {suite}: "
        f"{stats['successful_expectations']}/{stats['evaluated_expectations']} "
        f"({stats['success_percent']:.0f}%)"
        )
#Step 5: Build Data Docs
context.build_data_docs()
logger.info("Data Docs updated")

#Step 6: Return exit code based on success
sys.exit(0 if result.success else 1)
# Exit code 1 = failure → Airflow will catch this as task failure

