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
import great_expectations as gx #type: ignore
from great_expectations.core.batch import BatchRequest #type: ignore

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))

from logger import get_pipeline_logger  #type: ignore

logger = get_pipeline_logger("setup_gx")

GX_DIR = Path(__file__).parent / "gx_project" / "great_expectations"
print(f"GX_DIR: {GX_DIR.resolve()}")

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
#                "@127.0.0.1:5432/ecommerce_db"
                 "@172.28.224.1:5432/ecommerce_db"
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


#def get_validator(context: gx.DataContext, table: str, schema: str) -> gx.validator.validator.Validator:
def get_validator(context, table, schema) :
    print(f"Getting validator for {schema}.{table}...")
    suite_name = f"{schema}.{table}"
    print(f"suite_name: {suite_name}")
    """Get a GX validator for a specific table."""
    batch_request = BatchRequest(
        datasource_name="ecommerce_db",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name=suite_name,
    )

    # Create suite if it doesn't exist
    try:
        context.get_expectation_suite(suite_name)
    except Exception:
        context.add_expectation_suite(
            expectation_suite_name=suite_name)
#            overwrite_existing=False
        

    return context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
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