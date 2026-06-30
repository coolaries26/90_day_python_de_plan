#!/usr/bin/env python3
"""
partitioning.py — Day 54 | Table Partitioning
==============================================
Creates a partitioned version of mart_order_metrics
and compares query performance.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
import pandas as pd #type: ignore
from sqlalchemy import text #type: ignore

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "capstone"))

from logger import get_pipeline_logger #type: ignore
from db import get_ecommerce_engine, dispose_ecommerce_engine #type: ignore

logger = get_pipeline_logger("partitioning")
engine = get_ecommerce_engine()


def create_partitioned_table() -> None:
    """Create partitioned version of mart_order_metrics."""
    with engine.connect() as conn:
        # Drop existing
        conn.execute(text("""
            DROP TABLE IF EXISTS analytics.mart_order_metrics_partitioned CASCADE
        """))

        # Create parent partitioned table
        conn.execute(text("""
            CREATE TABLE analytics.mart_order_metrics_partitioned (
                order_id        TEXT,
                purchase_month  DATE NOT NULL,
                order_status    TEXT,
                delivery_days   INTEGER,
                is_late         BOOLEAN,
                total_payment   NUMERIC,
                review_score    NUMERIC
            ) PARTITION BY RANGE (purchase_month)
        """))

        # Create monthly partitions (Jan 2017 - Aug 2018)
        months = [
            ('2017-01-01', '2017-02-01'), ('2017-02-01', '2017-03-01'),
            ('2017-03-01', '2017-04-01'), ('2017-04-01', '2017-05-01'),
            ('2017-05-01', '2017-06-01'), ('2017-06-01', '2017-07-01'),
            ('2017-07-01', '2017-08-01'), ('2017-08-01', '2017-09-01'),
            ('2017-09-01', '2017-10-01'), ('2017-10-01', '2017-11-01'),
            ('2017-11-01', '2017-12-01'), ('2017-12-01', '2018-01-01'),
            ('2018-01-01', '2018-02-01'), ('2018-02-01', '2018-03-01'),
            ('2018-03-01', '2018-04-01'), ('2018-04-01', '2018-05-01'),
            ('2018-05-01', '2018-06-01'), ('2018-06-01', '2018-07-01'),
            ('2018-07-01', '2018-08-01'), ('2018-08-01', '2018-09-01'),
        ]

        for start, end in months:
            partition_name = f"mart_order_metrics_{start[:7].replace('-', '_')}"
            conn.execute(text(f"""
                CREATE TABLE analytics.{partition_name}
                    PARTITION OF analytics.mart_order_metrics_partitioned
                    FOR VALUES FROM ('{start}') TO ('{end}')
            """))

        # Load data from dbt mart
        conn.execute(text("""
            INSERT INTO analytics.mart_order_metrics_partitioned
            SELECT
                order_id,
                DATE_TRUNC('month', purchased_at)::DATE AS purchase_month,
                order_status,
                delivery_days,
                is_late,
                total_payment,
                review_score
            FROM dbt_dev_marts.mart_order_metrics
            WHERE purchased_at >= '2017-01-01' AND purchased_at <= '2018-09-01'
        """))
        conn.commit()

    logger.info("Partitioned table created with 20 monthly partitions")


def compare_queries() -> None:
    """Compare same query on partitioned vs non-partitioned table."""

    test_cases = [
        {
            "label":    "Single month filter",
            "regular":  """
                SELECT COUNT(*), AVG(review_score), SUM(total_payment)
                FROM dbt_dev_marts.mart_order_metrics
                WHERE purchased_at >= '2018-03-01'
                  AND purchased_at < '2018-04-01'
            """,
            "partitioned": """
                SELECT COUNT(*), AVG(review_score), SUM(total_payment)
                FROM analytics.mart_order_metrics_partitioned
                WHERE purchase_month = '2018-03-01'
            """,
        },
        {
            "label":    "Quarter filter",
            "regular":  """
                SELECT COUNT(*), AVG(delivery_days)
                FROM dbt_dev_marts.mart_order_metrics
                WHERE purchased_at >= '2018-01-01'
                  AND purchased_at < '2018-04-01'
                  AND is_late = true
            """,
            "partitioned": """
                SELECT COUNT(*), AVG(delivery_days)
                FROM analytics.mart_order_metrics_partitioned
                WHERE purchase_month >= '2018-01-01'
                  AND purchase_month < '2018-04-01'
                  AND is_late = true
            """,
        },
    ]

    results = []
    for tc in test_cases:
        times_regular = []
        times_partitioned = []

        for _ in range(5):
            with engine.connect() as conn:
                start = time.perf_counter()
                conn.execute(text(tc["regular"])).fetchall()
                times_regular.append((time.perf_counter() - start) * 1000)

                start = time.perf_counter()
                conn.execute(text(tc["partitioned"])).fetchall()
                times_partitioned.append((time.perf_counter() - start) * 1000)

        times_regular.sort()
        times_partitioned.sort()
        t_reg  = round(times_regular[2], 2)
        t_part = round(times_partitioned[2], 2)
        speedup = round(t_reg / max(t_part, 0.1), 1)

        logger.info(f"\n── {tc['label']} ──────────────────────────")
        logger.info(f"  Regular:     {t_reg}ms")
        logger.info(f"  Partitioned: {t_part}ms")
        logger.info(f"  Speedup:     {speedup}x")

        results.append({
            "query":        tc["label"],
            "regular_ms":   t_reg,
            "partitioned_ms": t_part,
            "speedup":      speedup,
        })

    df = pd.DataFrame(results)
    df.to_csv(Path(__file__).parent / "output" / "partition_benchmark.csv", index=False)
    logger.info(f"\n{df.to_string(index=False)}")


def main() -> None:
    logger.info("=" * 52)
    logger.info("Table Partitioning — Day 54")
    logger.info("=" * 52)

    Path(__file__).parent.joinpath("output").mkdir(exist_ok=True)
    create_partitioned_table()
    compare_queries()
    dispose_ecommerce_engine()
    logger.info("\n✅ Partitioning analysis complete")


if __name__ == "__main__":
    main()