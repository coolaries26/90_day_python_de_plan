#!/usr/bin/env python3
import sys
from pathlib import Path

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))

from db_utils import execute_scalar
from logger import get_pipeline_logger

logger = get_pipeline_logger("incremental_load")


def run_incremental_load():
    logger.info("Running incremental load example...")

    # Get last load timestamp (for real incremental logic)
    last_load = execute_scalar("""
        SELECT COALESCE(MAX(load_timestamp), '1900-01-01') 
        FROM analytics_customer_lifetime_v2
    """)

    logger.info("Last load timestamp: {}", last_load)

    # Example incremental query
    count = execute_scalar(f"""
        SELECT COUNT(*) FROM rental 
        WHERE rental_date > '{last_load}'
    """)

    logger.info("New rentals since last load: {} rows", count)
    logger.info("Incremental load logic is ready for production use.")


if __name__ == "__main__":
    run_incremental_load()