#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))   #db_utils
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))   #logger

from db_utils import execute_scalar
from logger import get_pipeline_logger

logger = get_pipeline_logger("data_quality")


def run_quality_checks():
    logger.info("Running data quality checks on dvdrental...")

    checks = [
        ("film row count", "SELECT COUNT(*) FROM film", 1000),
        ("rental row count", "SELECT COUNT(*) FROM rental", 16044),
        ("payment row count", "SELECT COUNT(*) FROM payment", 14596),
        ("No negative amounts", "SELECT COUNT(*) FROM payment WHERE amount < 0", 0),
        ("No future rental dates", "SELECT COUNT(*) FROM rental WHERE rental_date > NOW()", 0),
    ]

    for name, sql, expected in checks:
        result = execute_scalar(sql)
        status = "✅ PASS" if result == expected else f"❌ FAIL (got {result})"
        logger.info(f"{status} | {name}")

    logger.info("Data quality checks completed.")


if __name__ == "__main__":
    run_quality_checks()