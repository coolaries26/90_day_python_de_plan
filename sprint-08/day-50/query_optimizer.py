#!/usr/bin/env python3
"""
query_optimizer.py — Day 50 | EXPLAIN ANALYZE + Index Creation
===============================================================
Measures query performance before and after adding indexes.
Documents the execution plan for slow queries.

Run: python sprint-08/day-50/query_optimizer.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "capstone"))

from logger import get_pipeline_logger
from db import get_ecommerce_engine, dispose_ecommerce_engine

logger = get_pipeline_logger("query_optimizer")
engine = get_ecommerce_engine()


def explain_query(label: str, sql: str) -> str:
    """Run EXPLAIN ANALYZE and return the plan as a string."""
    with engine.connect() as conn:
        result = conn.execute(text(f"EXPLAIN ANALYZE {sql}"))
        plan = "\n".join(row[0] for row in result)
    logger.info(f"\n── EXPLAIN: {label} ─────────────────────────")
    for line in plan.split("\n")[-3:]:   # show last 3 lines (timing)
        logger.info(f"  {line}")
    return plan


def time_query(sql: str, n: int = 3) -> float:
    """Run query n times, return median execution time in ms."""
    times = []
    for _ in range(n):
        start = time.perf_counter()
        with engine.connect() as conn:
            conn.execute(text(sql)).fetchall()
        times.append((time.perf_counter() - start) * 1000)
    times.sort()
    return times[len(times) // 2]


def create_index(index_name: str, ddl: str) -> None:
    """Create an index, skip if already exists."""
    with engine.connect() as conn:
        try:
            conn.execute(text(ddl))
            conn.commit()
            logger.info(f"Created index: {index_name}")
        except Exception as exc:
            if "already exists" in str(exc):
                logger.info(f"Index already exists: {index_name}")
            else:
                logger.error(f"Index creation failed: {exc}")


def main() -> None:
    logger.info("=" * 52)
    logger.info("Query Optimiser — Day 50")
    logger.info("=" * 52)

    # ── Query 1: Filter by segment ────────────────────────────────────────
    Q_SEGMENT = "SELECT * FROM analytics.customer_ltv WHERE value_segment = 'Platinum'"

    logger.info("\n── Q1: Filter by value_segment ──────────────")
    t_before = time_query(Q_SEGMENT)
    explain_query("Before index — segment filter", Q_SEGMENT)
    logger.info(f"Time BEFORE index: {t_before:.1f}ms")

    create_index(
        "idx_customer_ltv_segment",
        "CREATE INDEX IF NOT EXISTS idx_customer_ltv_segment "
        "ON analytics.customer_ltv (value_segment)"
    )

    t_after = time_query(Q_SEGMENT)
    logger.info(f"Time AFTER index:  {t_after:.1f}ms")
    logger.info(f"Speedup: {t_before/max(t_after, 0.1):.1f}x")

    # ── Query 2: Filter sellers by state ──────────────────────────────────
    Q_SELLER_STATE = """
        SELECT * FROM analytics.seller_performance
        WHERE seller_state = 'SP' ORDER BY total_revenue DESC
    """

    logger.info("\n── Q2: Filter sellers by state ──────────────")
    t_before = time_query(Q_SELLER_STATE)
    explain_query("Before index — seller state filter", Q_SELLER_STATE)
    logger.info(f"Time BEFORE index: {t_before:.1f}ms")

    create_index(
        "idx_seller_performance_state",
        "CREATE INDEX IF NOT EXISTS idx_seller_performance_state "
        "ON analytics.seller_performance (seller_state)"
    )

    t_after = time_query(Q_SELLER_STATE)
    logger.info(f"Time AFTER index:  {t_after:.1f}ms")
    logger.info(f"Speedup: {t_before/max(t_after, 0.1):.1f}x")

    # ── Query 3: Order metrics by delivery flag ────────────────────────────
    Q_LATE_ORDERS = """
        SELECT * FROM analytics.order_metrics
        WHERE is_late = 1 ORDER BY delivery_days_actual DESC
    """

    logger.info("\n── Q3: Filter orders by is_late ─────────────")
    t_before = time_query(Q_LATE_ORDERS)
    explain_query("Before index — is_late filter", Q_LATE_ORDERS)
    logger.info(f"Time BEFORE index: {t_before:.1f}ms")

    create_index(
        "idx_order_metrics_is_late",
        "CREATE INDEX IF NOT EXISTS idx_order_metrics_is_late "
        "ON analytics.order_metrics (is_late)"
    )

    t_after = time_query(Q_LATE_ORDERS)
    logger.info(f"Time AFTER index:  {t_after:.1f}ms")
    logger.info(f"Speedup: {t_before/max(t_after, 0.1):.1f}x")

    # ── Summary ───────────────────────────────────────────────────────────
    logger.info("\n── Index Summary ────────────────────────────")
    with engine.connect() as conn:
        indexes = conn.execute(text("""
            SELECT indexname, tablename, indexdef
            FROM pg_indexes
            WHERE schemaname = 'analytics'
            ORDER BY tablename, indexname
        """)).fetchall()

    for idx in indexes:
        logger.info(f"  {idx[1]}.{idx[0]}")

    dispose_ecommerce_engine()
    logger.info("\n✅ Query optimisation complete")


if __name__ == "__main__":
    main()