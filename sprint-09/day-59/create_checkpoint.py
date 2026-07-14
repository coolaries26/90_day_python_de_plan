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
import great_expectations as gx     #type: ignore
from great_expectations.core.batch import BatchRequest  #type: ignore

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
from logger import get_pipeline_logger  #type: ignore

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
