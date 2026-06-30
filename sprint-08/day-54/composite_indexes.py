#!/usr/bin/env python3
"""
composite_indexes.py — Day 54 | Composite Index Analysis
=========================================================
Creates 3 composite indexes and measures speedup via EXPLAIN ANALYZE.
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

logger = get_pipeline_logger("composite_indexes")
engine = get_ecommerce_engine()
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def time_query(sql: str, n: int = 5) -> float:
    """Run query n times, return median time in ms."""
    times = []
    for _ in range(n):
        start = time.perf_counter()
        with engine.connect() as conn:
            conn.execute(text(sql)).fetchall()
        times.append((time.perf_counter() - start) * 1000)
    times.sort()
    return round(times[len(times) // 2], 2)


def get_explain(sql: str) -> str:
    """Get EXPLAIN ANALYZE output."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"EXPLAIN ANALYZE {sql}")).fetchall()
    return "\n".join(r[0] for r in rows)


def create_index(name: str, ddl: str) -> None:
    with engine.connect() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {name}"))
        conn.execute(text(ddl))
        conn.commit()
    logger.info(f"Created: {name}")


def drop_index(name: str) -> None:
    with engine.connect() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {name}"))
        conn.commit()


def benchmark(label: str, sql: str, index_name: str, index_ddl: str) -> dict:
    """Benchmark a query before and after creating an index."""
    # Before
    drop_index(index_name)
    t_before = time_query(sql)
    plan_before = get_explain(sql)
    scan_before = "Index Scan" if "Index Scan" in plan_before else "Seq Scan"

    # Create index
    create_index(index_name, index_ddl)

    # After
    t_after = time_query(sql)
    plan_after = get_explain(sql)
    scan_after = "Index Scan" if "Index Scan" in plan_after else "Seq Scan"

    speedup = round(t_before / max(t_after, 0.1), 1)
    logger.info(f"\n── {label} ───────────────────────────────")
    logger.info(f"  Before: {t_before}ms ({scan_before})")
    logger.info(f"  After:  {t_after}ms ({scan_after})")
    logger.info(f"  Speedup: {speedup}x")

    return {
        "label": label,
        "index": index_name,
        "query_ms_before": t_before,
        "query_ms_after": t_after,
        "speedup": speedup,
        "scan_before": scan_before,
        "scan_after": scan_after,
    }


# ── Index 1: Provided ─────────────────────────────────────────────────────
def index_1_state_revenue() -> dict:
    """
    Composite index: seller_state + total_revenue DESC
    Query: top sellers in a state
    """
    return benchmark(
        label="Seller: state + revenue",
        sql="""
            SELECT seller_id, seller_state, total_revenue
            FROM analytics.seller_performance
            WHERE seller_state = 'SP'
            ORDER BY total_revenue DESC
            LIMIT 10
        """,
        index_name="analytics.idx_seller_state_revenue",
        index_ddl="""
            CREATE INDEX idx_seller_state_revenue
            ON analytics.seller_performance (seller_state, total_revenue DESC)
        """,
    )


# ── Index 2: Write yourself ────────────────────────────────────────────────
def index_2_order_month_late() -> dict:
    """
    YOUR TASK: Composite index on mart_order_metrics
    Common query pattern: filter by purchase_month AND is_late

    HINTS:
    Query to optimise:
        SELECT order_id, delivery_days, review_score
        FROM dbt_dev_marts.mart_order_metrics
        WHERE purchase_month = '2018-01-01'   -- or purchased_at date_trunc
          AND is_late = true
        ORDER BY delivery_days DESC

    Index to create:
        CREATE INDEX idx_order_month_late
        ON dbt_dev_marts.mart_order_metrics (purchase_month, is_late)
        -- NOTE: if purchase_month column doesn't exist, use:
        -- DATE_TRUNC('month', purchased_at) — but you can't index expressions
        -- without a function index, so use purchased_at with a range instead:
        -- WHERE purchased_at >= '2018-01-01' AND purchased_at < '2018-02-01'

    Use benchmark() function — see index_1 for the pattern
    """
    # YOUR CODE HERE
    return benchmark(
        label="Order: month + late",
        sql="""
            SELECT order_id, delivery_days, review_score
            FROM dbt_dev_marts.mart_order_metrics
            WHERE purchased_at >= '2018-01-01' AND purchased_at < '2018-02-01'
              AND is_late = true
            ORDER BY delivery_days DESC
        """,
        index_name="dbt_dev_marts.idx_order_month_late",
        index_ddl="""
            CREATE INDEX idx_order_month_late
            ON dbt_dev_marts.mart_order_metrics (purchased_at, is_late)
        """,
    )

# ── Index 3: Write yourself ────────────────────────────────────────────────
def index_3_customer_segment_spend() -> dict:
    """
    YOUR TASK: Composite index on mart_customer_ltv
    Common query pattern: filter by value_segment, sort by total_spent

    HINTS:
    Query to optimise:
        SELECT customer_unique_id, total_spent, avg_review_score
        FROM dbt_dev_marts.mart_customer_ltv
        WHERE value_segment = 'Gold'
        ORDER BY total_spent DESC
        LIMIT 50

    Index to create:
        CREATE INDEX idx_ltv_segment_spend
        ON dbt_dev_marts.mart_customer_ltv (value_segment, total_spent DESC)

    Use benchmark() function
    """
    # YOUR CODE HERE
    return benchmark(
        label="Customer: segment + spend",
        sql="""
            SELECT customer_unique_id, total_spent, avg_review_score
            FROM dbt_dev_marts.mart_customer_ltv
            WHERE value_segment = 'Gold'
            ORDER BY total_spent DESC
            LIMIT 50
        """,
        index_name="dbt_dev_marts.idx_ltv_segment_spend",
        index_ddl="""
            CREATE INDEX idx_ltv_segment_spend
            ON dbt_dev_marts.mart_customer_ltv (value_segment, total_spent DESC)
        """,
    )   


def main() -> None:
    logger.info("=" * 52)
    logger.info("Composite Indexes — Day 54")
    logger.info("=" * 52)

    results = []
    for fn in [index_1_state_revenue, index_2_order_month_late, index_3_customer_segment_spend]:
        try:
            result = fn()
            results.append(result)
        except NotImplementedError as e:
            logger.warning(f"⏳ {fn.__name__}: {e}")
        except Exception as e:
            logger.error(f"❌ {fn.__name__}: {e}")

    if results:
        df = pd.DataFrame(results)
        df.to_csv(OUTPUT_DIR / "index_benchmarks.csv", index=False)
        logger.info(f"\n── Summary ──────────────────────────────────")
        logger.info(f"\n{df[['label','query_ms_before','query_ms_after','speedup','scan_after']].to_string(index=False)}")

    dispose_ecommerce_engine()


if __name__ == "__main__":
    main()