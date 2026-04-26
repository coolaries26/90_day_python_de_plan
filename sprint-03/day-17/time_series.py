#!/usr/bin/env python3
"""
time_series.py — Day 17 | Time Series Analysis
===============================================
Patterns to implement:
  P4: pd.Grouper(freq='M')  — monthly revenue aggregation
  P5: shift() + pct_change() — month-over-month growth with lag features

Run: python time_series.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
#from sqlalchemy import sql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02")) # db_utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04")) # logger
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-02" / "day-14")) # etl_protocols
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day-16"))

from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger
from etl_protocols import ETLResult
from orm_queries import write_audit_log

logger = get_pipeline_logger("time_series")
engine = get_engine()

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)



# ── P4: pd.Grouper — monthly aggregation ─────────────────────────────────────
def p4_monthly_revenue() -> pd.DataFrame:
    """
    Pattern 4: Group payments by month using pd.Grouper.

    HINTS:
      - Load payment table: payment_id, customer_id, amount, payment_date
      - pd.to_datetime() on payment_date
      - Set payment_date as index: df.set_index('payment_date')
      - Use groupby(pd.Grouper(freq='M')) to group by month-end
      - Aggregate: amount → sum (total_revenue), payment_id → count (payment_count)
      - Reset index so payment_date becomes a column again
      - Round total_revenue to 2dp
      - Add column: avg_payment = total_revenue / payment_count, round to 4dp

    Expected output (4 rows):
      payment_date  total_revenue  payment_count  avg_payment
      2007-02-28    8351.84        2016           4.1427
      2007-03-31    23886.56       5644           4.2321
      2007-04-30    28559.46       6754           4.2285
      2007-05-31    514.18         182            2.8251

    Self-check: total of total_revenue ≈ 61312.04
    """
    logger.info("P4: pd.Grouper(freq='M') — monthly revenue")
    # YOUR CODE HERE     
    def load_payment_data() -> pd.DataFrame:
        """Load payment + customer data joined."""
    sql = """
            SELECT
                p.payment_id,
                p.customer_id,
                p.amount,
                p.payment_date,
                p.rental_id
            FROM payment p
            ORDER BY p.customer_id, p.payment_date
       """
    df = pd.read_sql(sql, engine)
    df["payment_date"] = pd.to_datetime(df["payment_date"])
    df.set_index("payment_date", inplace=True)
    result = df.groupby(pd.Grouper(freq='M')).agg(
        total_revenue=pd.NamedAgg(column="amount", aggfunc="sum"),
        payment_count=pd.NamedAgg(column="payment_id", aggfunc="count"),
    )
    result.reset_index(inplace=True)
    result["avg_payment"] = result["total_revenue"] / result["payment_count"]
    result["total_revenue"] = result["total_revenue"].round(2)
    result["avg_payment"] = result["avg_payment"].round(4)
    logger.info(f"Loaded {len(df):,} payment rows")
    return result



# ── P5: shift() + pct_change() + lag features ────────────────────────────────
def p5_growth_and_lag(monthly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pattern 5: Add growth and lag features to the monthly revenue DataFrame.

    HINTS — apply to the DataFrame returned by p4_monthly_revenue():

    Step 1: mom_growth_pct
      - monthly_df['total_revenue'].pct_change() * 100
      - round to 2dp
      - First row will be NaN — correct, leave it

    Step 2: prev_month_revenue (LAG feature)
      - monthly_df['total_revenue'].shift(1)
      - This is LAG(total_revenue, 1) — previous month's value

    Step 3: revenue_vs_avg
      - total_revenue minus the mean of total_revenue
      - Shows how far each month is from the average
      - round to 2dp

    Step 4: is_peak_month (boolean)
      - True where total_revenue == total_revenue.max()

    Expected output columns:
      payment_date, total_revenue, payment_count, avg_payment,
      mom_growth_pct, prev_month_revenue, revenue_vs_avg, is_peak_month

    Self-check:
      - July (2007-07-31 or 2007-04-30 depending on date alignment) is_peak_month = True
      - mom_growth_pct for month 2 should be large positive (>100%)
      - prev_month_revenue for month 1 = NaN
    """
    logger.info("P5: shift() + pct_change() — growth and lag features")
    # YOUR CODE HERE
    result = monthly_df.copy()
    result["mom_growth_pct"] = (result["total_revenue"].pct_change() * 100).round(2)
    result["prev_month_revenue"] = result["total_revenue"].shift(1)
    result["revenue_vs_avg"] = (result["total_revenue"] - result["total_revenue"].mean()).round(2)
    result["is_peak_month"] = result["total_revenue"] == result["total_revenue"].max()
    return result



def main() -> None:
    logger.info("=" * 52)
    logger.info("Time Series Analysis — Day 17")
    logger.info("=" * 52)

    # P4
    try:
        monthly = p4_monthly_revenue()
        logger.info(f"\nP4 Monthly Revenue:\n{monthly.to_string(index=False)}")
        monthly.to_csv(OUTPUT_DIR / "p4_monthly_revenue.csv", index=False)
    except NotImplementedError as e:
        logger.warning(f"P4 not implemented: {e}")
        monthly = None

    # P5 — depends on P4
    if monthly is not None:
        try:
            enriched = p5_growth_and_lag(monthly)
            logger.info(f"\nP5 Growth + Lag:\n{enriched.to_string(index=False)}")
            enriched.to_csv(OUTPUT_DIR / "p5_growth_lag.csv", index=False)

            # Write to PostgreSQL
            enriched.to_sql(
                "analytics_monthly_enriched", engine,
                if_exists="replace", index=False, method="multi"
            )
            logger.info("Written to analytics_monthly_enriched table")

            # Audit log
            result = ETLResult(
                pipeline_name="TimeSeriesAnalysis",
                source_table="payment",
                target_table="analytics_monthly_enriched",
            )
            result.complete(
                rows_extracted=len(monthly),
                rows_loaded=len(enriched),
                export_csv=Path("OUTPUT_DIR"),
                attempts=1,
            )
            write_audit_log(result)

        except NotImplementedError as e:
            logger.warning(f"P5 not implemented: {e}")

    dispose_engine()
    logger.info("Done.")


if __name__ == "__main__":
    main()