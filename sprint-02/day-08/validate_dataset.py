#!/usr/bin/env python3
import sys
from pathlib import Path

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))

from db_utils import execute_scalar
from logger import get_pipeline_logger

logger = get_pipeline_logger("dataset_validation")


def main():
    logger.info("🔍 Starting DVD Rental Dataset Validation")
    logger.info("=" * 60)

    tables = {
        "film": 1000, "actor": 200, "customer": 599,
        "rental": 16044, "payment": 14596, "inventory": 4581,
        "store": 2, "category": 16
    }

    for table, expected in tables.items():
        count = execute_scalar(f"SELECT COUNT(*) FROM {table}")
        status = "✅" if count == expected else "⚠️"
        logger.info(f"{status} {table:<12} → {count:,} rows (expected {expected:,})")

    logger.info("🎉 Dataset validation completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()