#!/usr/bin/env python3
"""
customer_ltv_expectations.py — Day 57 | Expectation Suite
==========================================================
Writes 10+ expectations on dbt_dev_marts.mart_order_metrics.
Covers: completeness, range checks, categorical values,
        statistical properties, row count.

Run: python sprint-09/day-57/customer_ltv_expectations.py
"""

from __future__ import annotations

import sys
from pathlib import Path
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
        data_asset_name="dbt_dev_marts.mart_order_metrics",
    )
    suite_name = "mart_order_metrics.basic"

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
        column="total_payment",
        min_value=0.01,
        max_value=None,  # No upper limit specified
    )

    # ── E5: delivered categorical values ──────────────────────────────
    validator.expect_column_values_to_be_in_set(
        column="order_status",
        value_set=["delivered", "not_delivered"],
    )

    # ── E6: is_late only 0 or 1 ───────────────────────────────────────
    validator.expect_column_values_to_be_in_set(
        column="is_late",
        value_set=[True, False],
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
    validator.expect_column_values_to_be_between(
        column="avg_review_score",
        min_value=1.0,
        max_value=5.0,
        mostly=0.99,  # Allow 1% outside range (edge cases)
    )
    # ── E8: total_orders positive integer — WRITE THIS YOURSELF ──────────
    # All customers should have at least 1 order
    # HINT: expect_column_values_to_be_between(min_value=1)
    # YOUR CODE HERE
    validator.expect_column_values_to_be_between(
        column="delivery_days",
        min_value=1,  # At least 1 delivery day
        max_value=None,  # No upper limit specified
    )
    # ── E9: avg_order_value statistical check — WRITE THIS YOURSELF ───────
    # Mean avg_order_value should be between 100 and 300 BRL
    # (statistical expectation — checks the column mean, not individual values)
    # HINT: validator.expect_column_mean_to_be_between(
    #     column="avg_order_value",
    #     min_value=100, max_value=300,
    # )
    # YOUR CODE HERE
    validator.expect_column_mean_to_be_between(
        column="avg_order_value",
        min_value=100,
        max_value=300,
    )
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
    validator.expect_column_values_to_be_in_set(
        column="order_status",
        value_set=["delivered", "not_delivered"],  # Exclude Platinum
        mostly=0.95,  # At least 95% are non-Platinum
    )
    # Save the expectation suite
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("Expectation suite saved: mart_order_metrics.basic")

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