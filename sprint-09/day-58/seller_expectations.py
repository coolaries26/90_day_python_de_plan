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
from __future__ import annotations

import sys
from pathlib import Path
import great_expectations as gx #type: ignore
from great_expectations.core.batch import BatchRequest  #type: ignore

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
from logger import get_pipeline_logger  #type: ignore

logger = get_pipeline_logger("seller_expectations")

GX_DIR = Path(__file__).resolve().parent.parent / "day-57" / "gx_project" / "great_expectations"


def build_suite() -> None:
    context = gx.data_context.DataContext(context_root_dir=str(GX_DIR))

    suite_name = "analytics.seller_performance.basic"
    try:
        context.get_expectation_suite(suite_name)
        context.delete_expectation_suite(suite_name)
    except Exception:
        pass
    context.add_expectation_suite(expectation_suite_name=suite_name)

    batch_request = BatchRequest(
        datasource_name="ecommerce_db",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="analytics.seller_performance",
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

## ── E1: Row count ─────────────────────────────────────────────────────
#  1. Row count between 2,900 and 3,200

    validator.expect_table_row_count_to_be_between(
        min_value=2_900,
        max_value=3_200
    )
# E2: seller_id not null
    validator.expect_column_values_to_not_be_null(column="seller_id")

# E3: seller_id unique
    validator.expect_column_values_to_be_unique(column="seller_id")

# E4: total_revenue >= 0
    validator.expect_column_values_to_be_between(
        column="total_revenue",
        min_value=0,
#        max_value=1_000_000
    )

# E5: avg_review_score between 1.0 and 5.0 (where not null)
    validator.expect_column_values_to_be_between(
        column="avg_review_score",
        min_value=1.0,
        max_value=5.0
    )

# E6:   on_time_delivery_rate between 0 and 1 (it's a proportion)
    validator.expect_column_values_to_be_between(
        column="on_time_delivery_rate",
        min_value=0,
        max_value=100
    )

#  E7. on_time_delivery_rate mean between 0.80 and 0.95
    validator.expect_column_mean_to_be_between(
        column="on_time_delivery_rate",
        min_value=80,
        max_value=95
    )

#  E8. total_orders >= 1
    validator.expect_column_values_to_be_between(
        column="total_orders",
        min_value=1,
        max_value=10_000
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