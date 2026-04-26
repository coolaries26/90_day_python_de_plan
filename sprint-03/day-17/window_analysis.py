#!/usr/bin/env python3
"""
window_analysis.py — Day 17 | Pandas Window Functions
======================================================
Patterns demonstrated:
  P1: groupby().transform()  — broadcast group stats to row level
  P2: rolling() + expanding() — sliding and cumulative windows
  P3: rank() within group    — per-customer payment ranking

All patterns use DVD Rental payment + customer data.
Run: python window_analysis.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-02" / "day-14"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day-16"))

from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger
from etl_protocols import ETLConfig, ETLResult
from orm_queries import write_audit_log

logger = get_pipeline_logger("window_analysis")
engine = get_engine()

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_payment_data() -> pd.DataFrame:
    """Load payment + customer data joined."""
    sql = """
        SELECT
            p.payment_id,
            p.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            p.amount,
            p.payment_date,
            p.rental_id
        FROM payment p
        JOIN customer c ON p.customer_id = c.customer_id
        ORDER BY p.customer_id, p.payment_date
    """
    df = pd.read_sql(sql, engine)
    df["payment_date"] = pd.to_datetime(df["payment_date"])
    logger.info(f"Loaded {len(df):,} payment rows")
    return df


# ── P1: groupby().transform() ─────────────────────────────────────────────────
def p1_broadcast_group_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pattern 1: Add group-level statistics to every row.
    Use case: flag payments that are above/below the customer's average.
    This is a core feature engineering pattern for ML.
    """
    logger.info("P1: groupby().transform() — broadcast group stats")

    result = df.copy()

    # Customer-level aggregates broadcast to each row
    result["customer_total_spend"] = (
        result.groupby("customer_id")["amount"].transform("sum").round(2)
    )
    result["customer_avg_payment"] = (
        result.groupby("customer_id")["amount"].transform("mean").round(4)
    )
    result["customer_payment_count"] = (
        result.groupby("customer_id")["amount"].transform("count")
    )

    # Derived: is this payment above or below customer's average?
    result["above_avg_flag"] = (
        result["amount"] > result["customer_avg_payment"]
    ).astype(int)

    # Derived: payment as % of customer's total spend
    result["pct_of_customer_total"] = (
        result["amount"] / result["customer_total_spend"] * 100
    ).round(2)

    above_pct = result["above_avg_flag"].mean() * 100
    logger.info(f"  {above_pct:.1f}% of payments are above the customer's average")

    # Show sample — one customer's rows
    sample = result[result["customer_id"] == 148][[
        "payment_id", "amount", "customer_avg_payment",
        "above_avg_flag", "pct_of_customer_total"
    ]].head(5)
    logger.info(f"  Sample (customer 148):\n{sample.to_string(index=False)}")

    return result


# ── P2: rolling() + expanding() ──────────────────────────────────────────────
def p2_rolling_windows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pattern 2: Sliding window and cumulative calculations per customer.
    Use case: detect spending trends — is a customer's recent spend increasing?
    """
    logger.info("P2: rolling() + expanding() — sliding and cumulative windows")

    # Sort by customer and date — rolling requires temporal order
    result = (
        df.sort_values(["customer_id", "payment_date"])
        .copy()
        .reset_index(drop=True)
    )

    # Rolling 3-payment moving average per customer
    result["rolling_3_avg"] = (
        result.groupby("customer_id")["amount"]
        .transform(lambda x: x.rolling(window=3, min_periods=1).mean().round(4))
    )

    # Expanding (cumulative) sum per customer — running total spend
    result["cumulative_spend"] = (
        result.groupby("customer_id")["amount"]
        .transform(lambda x: x.expanding().sum().round(2))
    )

    # Expanding max — highest payment seen so far for this customer
    result["running_max_payment"] = (
        result.groupby("customer_id")["amount"]
        .transform(lambda x: x.expanding().max())
    )

    # Show a customer with multiple payments
    sample = result[result["customer_id"] == 148][[
        "payment_date", "amount",
        "rolling_3_avg", "cumulative_spend", "running_max_payment"
    ]].head(8)
    logger.info(f"  Rolling/Expanding sample (customer 148):\n{sample.to_string(index=False)}")

    return result


# ── P3: rank() within group ───────────────────────────────────────────────────
def p3_within_group_rank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pattern 3: Rank each payment within its customer's history.
    Also: rank customers globally by total spend.
    Use case: identify customers' highest-value transactions.
    """
    logger.info("P3: rank() — within-group and global ranking")

    result = df.copy()

    # Rank each payment within the customer (1 = highest payment)
    result["payment_rank_within_customer"] = (
        result.groupby("customer_id")["amount"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    # Global customer rank by total spend
    customer_totals = (
        result.groupby(["customer_id", "customer_name"])["amount"]
        .sum()
        .reset_index()
        .rename(columns={"amount": "total_spend"})
    )
    customer_totals["spend_rank"] = (
        customer_totals["total_spend"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    # Top 5 customers by spend
    top5 = customer_totals.nsmallest(5, "spend_rank")[
        ["spend_rank", "customer_name", "total_spend"]
    ]
    logger.info(f"  Top 5 customers by spend:\n{top5.to_string(index=False)}")

    # Merge rank back to payment-level
    result = result.merge(
        customer_totals[["customer_id", "spend_rank"]],
        on="customer_id", how="left"
    )

    return result


def main() -> None:
    logger.info("=" * 52)
    logger.info("Window Analysis — Day 17")
    logger.info("=" * 52)

    df = load_payment_data()

    df_p1 = p1_broadcast_group_stats(df)
    df_p1.to_csv(OUTPUT_DIR / "p1_broadcast_stats.csv", index=False)

    df_p2 = p2_rolling_windows(df)
    df_p2.to_csv(OUTPUT_DIR / "p2_rolling_windows.csv", index=False)

    df_p3 = p3_within_group_rank(df)
    df_p3.to_csv(OUTPUT_DIR / "p3_rank_analysis.csv", index=False)

    # Write audit log entry
    config = ETLConfig(source_table="payment", target_table="window_analysis_output")
    result = ETLResult(
        pipeline_name="WindowAnalysis",
        source_table="payment",
        target_table="window_analysis_output",
    )
    result.complete(rows_extracted=len(df), rows_loaded=len(df_p3),export_csv=Path("OUTPUT_DIR"), attempts=1)
    write_audit_log(result)

    dispose_engine()
    logger.info("Done. Check output/ for CSV files.")


if __name__ == "__main__":
    main()