#!/usr/bin/env python3
import sys
from pathlib import Path
import click

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))  # For logger
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-12" ))  # For resilient ETL pipeline

from logger import get_pipeline_logger
from etl_resilient import ResilientETLPipeline   # from Day 12

logger = get_pipeline_logger("etl_cli")
#----------------------------------------------------------------------------------

# This script provides a simple CLI interface to run the Resilient ETL Pipeline with customizable parameters.

@click.command()
@click.option('--pipeline-name', default='customer_lifetime', prompt='Name of the ETL pipeline:', help='Name of the ETL pipeline for logging purposes.')
@click.option('--target-table', default='analytics_customer_lifetime_v2', prompt='Target table name:', help='Target table name.')
@click.option('--max-retries', default=3, type=int, prompt='Maximum number of retries on failure:', help='Maximum number of retries on failure.')

def main(pipeline_name: str, target_table: str, max_retries: int):
    """CLI entry point for resilient ETL pipeline."""
    logger.info("Starting CLI ETL pipeline '{}' → table '{}'", pipeline_name, target_table)

    pipeline = ResilientETLPipeline(max_retries=max_retries)
    pipeline.run()

    logger.info("🎉 CLI ETL pipeline completed successfully!")

if __name__ == "__main__":
    main()
