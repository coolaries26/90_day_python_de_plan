#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
import time

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))  # For db_utils
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))  # For logger
#sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-11"))  # For config

from db_utils import execute_scalar, get_engine
from logger import get_pipeline_logger
#from config.etl_config import settings   # from Day 11

logger = get_pipeline_logger("etl_resilient")

#---------------------------------------------------

def run_quality_checks():
    logger.info("Starting resilient data quality checks...")

    checks = [
        ("film row count", "SELECT COUNT(*) FROM film", 1000),
        ("actor row count", "SELECT COUNT(*) FROM actor", 200),
        ("customer row count", "SELECT COUNT(*) FROM customer", 599),
        ("rental row count", "SELECT COUNT(*) FROM rental", 16044),
        ("payment row count", "SELECT COUNT(*) FROM payment", 14596),
        ("No negative payment amounts", "SELECT COUNT(*) FROM payment WHERE amount < 0", 0),
        ("No future rental dates", "SELECT COUNT(*) FROM rental WHERE rental_date > NOW()", 0),
    ]

    passed = 0
    failed = 0

    for name, sql, expected in checks:
        try:
            result = execute_scalar(sql)
            if result == expected:
                logger.info("✅ PASS | {} → {} (expected {})", name, result, expected)
                passed += 1
            else:
                logger.warning("⚠️  FAIL | {} → {} (expected {})", name, result, expected)
                failed += 1
        except Exception as e:
            logger.error("❌ ERROR | {} → Query failed: {}", name, str(e))
            failed += 1

    logger.info("=" * 60)
    logger.info("Data Quality Summary: {} PASSED, {} FAILED", passed, failed)
    logger.info("=" * 60)

    if failed == 0:
        logger.info("🎉 All data quality checks passed!")
    else:
        logger.warning("Some data quality checks failed. Please review.")

    return passed, failed


if __name__ == "__main__":
    run_quality_checks()
