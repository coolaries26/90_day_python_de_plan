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
from wsgiref.validate import validator
import great_expectations as gx #type: ignore
from great_expectations.core.batch import BatchRequest #type: ignore

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
    
from logger import get_pipeline_logger  #type: ignore
    
logger = get_pipeline_logger("customer_ltv_expectations")

GX_DIR = Path(__file__).parent / "gx_project" / "great_expectations"
print(f"GX_DIR: {GX_DIR.resolve()}")

def build_suite() -> None:
    context = gx.data_context.DataContext(context_root_dir=str(GX_DIR))

    batch_request = BatchRequest(
        datasource_name="ecommerce_db",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="dbt_dev_marts.mart_customer_ltv",
    )
    suite_name = "mart_customer_ltv.basic"

# Create if not exists
    try:
        context.get_expectation_suite(suite_name)
    except Exception:
        context.add_expectation_suite(expectation_suite_name=suite_name)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
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
        min_value=0,
        max_value=20_000,  # No upper limit specified
        mostly=0.999  # Allow 1% outside range (edge cases)
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
    validator.expect_column_values_to_be_between(
        column="avg_review_score",
        min_value=1.0,
        max_value=5.0,
        mostly=0.99,  # Allow 1% outside range (edge cases)
    )
    # ── E8: total_orders positive integer — WRITE THIS YOURSELF ──────────
    validator.expect_column_values_to_be_between(
        column="total_orders",
        min_value=1,  # At least 1 order
        max_value=None,  # No upper limit specified
    )
    # ── E9: avg_order_value statistical check — WRITE THIS YOURSELF ───────
    validator.expect_column_mean_to_be_between(
        column="avg_order_value",
        min_value=150,
        max_value=250,
    )
#   # E10: At least 95% of customers are non-Platinum
#   validator.expect_column_values_to_be_in_set(
#       column="value_segment",
#       value_set=["Bronze", "Silver", "Gold"],
#       mostly=0.95,
#   )
# E10: avg_review_score mean check (more useful than duplicate segment check)
    validator.expect_column_mean_to_be_between(
        column="avg_review_score",
        min_value=3.5,
        max_value=5.0,
    )
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("Expectation suite saved: mart_customer_ltv.basic")

    # Run validation
    logger.info("\nRunning validation...")
    results = validator.validate()
    for result in results.results:
        if not result.success:
            print(f"FAILED: {result.expectation_config.expectation_type}")
            print(f"  kwargs: {result.expectation_config.kwargs}")
            print(f"  result: {result.result}")
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