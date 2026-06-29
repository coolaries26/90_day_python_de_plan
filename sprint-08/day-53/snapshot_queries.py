#!/usr/bin/env python3
"""
snapshot_queries.py — Day 53 | Query dbt Snapshot History
==========================================================
Demonstrates how to query the snapshot table to see
how order statuses changed over time.
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "capstone"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))

from db import get_ecommerce_engine, dispose_ecommerce_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("snapshot_queries")
engine = get_ecommerce_engine()
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ── Q1: Current state of all orders ──────────────────────────────────────
Q1_CURRENT_STATE = """
SELECT order_id, order_status, is_late,
       dbt_valid_from, dbt_valid_to, dbt_updated_at
FROM snapshots.orders_snapshot
WHERE dbt_valid_to IS NULL    --# ← current records only
ORDER BY dbt_valid_from DESC
LIMIT 10
"""

# ── Q2: Orders that changed status ────────────────────────────────────────
Q2_STATUS_CHANGES = """
-- Find order_ids that appear more than once in the snapshot
WITH status_counts AS (
    SELECT order_id, COUNT(*) AS version_count
    FROM snapshots.orders_snapshot
    GROUP BY order_id
    HAVING COUNT(*) > 1
)
SELECT s.order_id, s.order_status,
       s.dbt_valid_from, s.dbt_valid_to
FROM snapshots.orders_snapshot s
JOIN status_counts sc ON s.order_id = sc.order_id
ORDER BY s.order_id, s.dbt_valid_from
LIMIT 20
-- SELECT 'implement Q2' AS placeholder
"""

# ── Q3: Snapshot summary ──────────────────────────────────────────────────
Q3_SNAPSHOT_SUMMARY = """
-- Show: total rows, current rows, historical rows, unique orders
SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN dbt_valid_to IS NULL THEN 1 END) AS current_rows,
    COUNT(CASE WHEN dbt_valid_to IS NOT NULL THEN 1 END) AS historical_rows,
    COUNT(DISTINCT order_id) AS unique_orders
FROM snapshots.orders_snapshot
-- SELECT 'implement Q3' AS placeholder
"""


def main():
    logger.info("Snapshot Queries — Day 53")

    for name, sql in [
        ("q1_current_state",   Q1_CURRENT_STATE),
        ("q2_status_changes",  Q2_STATUS_CHANGES),
        ("q3_summary",         Q3_SNAPSHOT_SUMMARY),
    ]:
        try:
            df = pd.read_sql(sql, engine)
            logger.info(f"✅ {name}: {len(df)} rows")
            df.to_csv(OUTPUT_DIR / f"{name}.csv", index=False)
        except Exception as exc:
            logger.error(f"❌ {name}: {exc}")

    dispose_ecommerce_engine()


if __name__ == "__main__":
    main()