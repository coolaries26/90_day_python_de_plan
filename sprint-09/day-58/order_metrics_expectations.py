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
import great_expectations as gx #type: ignore
from great_expectations.core.batch import BatchRequest  #type: ignore

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
from logger import get_pipeline_logger  #type: ignore

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
        min_value=90_000, 
        max_value=110_000
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
    validator.expect_column_mean_to_be_between(
        column="delivery_days",
        min_value=8,
        max_value=20
    )

    # ── E7: is_late not null for delivered orders — WRITE THIS YOURSELF ────
    # All rows in this table are delivered, so is_late should never be NULL
    validator.expect_column_values_to_not_be_null(column="is_late")

    # ── E8: late delivery rate — WRITE THIS YOURSELF ──────────────────────
    # We know ~8.1% of orders are late — check this stays within bounds
    # HINT: expect_column_mean_to_be_between
    #   is_late is boolean (True/False), mean = proportion of True values
    #   Expected: mean between 0.05 and 0.15 (5-15% late rate)
    # YOUR CODE HERE
    validator.expect_column_values_to_be_in_set(
        column="is_late",
        value_set=[False],
        mostly=0.85
    )

    # ── E9: review_score mean — WRITE THIS YOURSELF ───────────────────────
    # Average review score should be between 3.5 and 5.0
    # (we know it's ~4.09 from Day 44 analysis)
    validator.expect_column_mean_to_be_between(
        column="review_score",
        min_value=3.5,
        max_value=5.0
    )

    # ── E10: product_count positive — WRITE THIS YOURSELF ─────────────────
    # Every order must have at least 1 product
    validator.expect_column_values_to_be_between(
        column="product_count",
        min_value=1
    )

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