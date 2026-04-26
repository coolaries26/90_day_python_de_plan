# 📅 DAY 17 — Sprint 03 | Pandas Depth
## Window Functions, Rolling, groupby().transform(), Time Series

---

## 🔁 RETROSPECTIVE — Day 16

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| Alembic init + migration | ✅ Pass | etl_audit_log created correctly |
| ORM models (5 tables) | ✅ Pass | Film, Customer, Rental, Payment, AuditLog |
| active column fix | ✅ Pass | Integer type, == 1 filter |
| Q3 rental count | ✅ Pass | Eleanor Hunt top — investigate 46 vs 45 discrepancy |
| Audit log write + read | ✅ Pass | id=1, all fields populated |
| Git discipline | ✅ Pass | [DAY-016][S03] one clean commit |

### Action Before Starting
```bash
# Verify rental count discrepancy from Day 16 note
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_scalar, close_pool
n = execute_scalar('SELECT COUNT(*) FROM rental WHERE customer_id = 148')
print('Ground truth rental count for Eleanor Hunt:', n)
close_pool()
"
# Add result to dq_findings.md, then proceed

# Branch setup
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-03/day-17-pandas-depth
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-04: Data Wrangling with Pandas                            |
| Story           | ST-17: Pandas Window Functions + Time Series Analysis        |
| Task ID         | TASK-017                                                     |
| Sprint          | Sprint 03 (Days 15–21)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | pandas, window-functions, time-series, groupby, day-17       |
| Acceptance Criteria | 5 Pandas patterns demonstrated on DVD Rental data; results written to DB via audit log; pushed to git |

---

## 📚 BACKGROUND

### The Gap in Your Pandas Knowledge

Your Day 03 Pandas work covered: `read_sql`, `merge`, `groupby`, `cut`.
What you haven't used yet — and what appears constantly in DE/DS interviews:

| Pattern | What It Does | SQL Equivalent |
|---------|-------------|----------------|
| `groupby().transform()` | Compute group stat, broadcast back to each row | Window function (no GROUP BY collapse) |
| `rolling(n)` | Sliding window calculation over N rows | `AVG(x) OVER (ROWS BETWEEN N PRECEDING...)` |
| `expanding()` | Running cumulative from start to current row | `SUM(x) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING)` |
| `pd.Grouper(freq='M')` | Group by time period (month, week, day) | `DATE_TRUNC('month', ...)` |
| `rank()` | Rank within group | `ROW_NUMBER() / RANK() OVER (PARTITION BY ...)` |
| `shift()` | Previous row's value | `LAG()` window function |
| `pct_change()` | Row-over-row % change | `(curr - prev) / prev * 100` |

### Key Difference: `groupby().agg()` vs `groupby().transform()`

```python
df = pd.DataFrame({
    'customer_id': [1, 1, 2, 2],
    'amount':      [10, 20, 30, 40]
})

# groupby().agg() — COLLAPSES to one row per group (like GROUP BY)
df.groupby('customer_id')['amount'].sum()
# customer_id
# 1    30
# 2    70

# groupby().transform() — BROADCASTS result back to every row (like window function)
df['customer_total'] = df.groupby('customer_id')['amount'].transform('sum')
# customer_id  amount  customer_total
# 1            10      30              ← group total broadcast to row level
# 1            20      30
# 2            30      70
# 2            40      70
```

This distinction matters for feature engineering in ML (Sprint 12) where you
need per-row features that are calculated at the group level.

---

## 🎯 OBJECTIVES

1. `groupby().transform()` — broadcast group stats to row level
2. `rolling()` + `expanding()` — sliding and cumulative windows
3. `pd.Grouper(freq='M')` — monthly time series on payment data
4. `rank()` + `shift()` + `pct_change()` — per-group analytics
5. Write final analysis to PostgreSQL + audit log
6. One clean commit `[DAY-017][S03]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                           |
|-------|----------|----------------------------------------------------|
| A     | 10 min   | Branch setup + rental count verify                 |
| B     | 40 min   | `window_analysis.py` — P1-P3 fully provided        |
| C     | 40 min   | `time_series.py` — P4-P5 write yourself            |
| D     | 10 min   | Write results to DB + audit log                    |
| E     | 20 min   | Commit + merge                                     |

---

## 📝 EXERCISES

---

### EXERCISE 1 — window_analysis.py: Patterns P1–P3 (Block B)
**[Fully provided — study each pattern carefully before running]**

Create `sprint-03/day-17/window_analysis.py`:

```python
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
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day-14"))
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
    result.complete(rows_extracted=len(df), rows_loaded=len(df_p3), attempts=1)
    write_audit_log(result)

    dispose_engine()
    logger.info("Done. Check output/ for CSV files.")


if __name__ == "__main__":
    main()
```

**✅ Expected key outputs:**
```
P1: ~50% of payments above customer average (will vary)
P2: cumulative_spend for customer 148 increases monotonically
P3: Eleanor Hunt or Karl Seal at spend_rank=1
```

---

### EXERCISE 2 — time_series.py: Patterns P4–P5 (Block C)
**[Write yourself — hints provided for each pattern]**

Create `sprint-03/day-17/time_series.py`:

```python
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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day-14"))
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
    raise NotImplementedError("Implement p4_monthly_revenue")


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
    raise NotImplementedError("Implement p5_growth_and_lag")


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
                attempts=1,
            )
            write_audit_log(result)

        except NotImplementedError as e:
            logger.warning(f"P5 not implemented: {e}")

    dispose_engine()
    logger.info("Done.")


if __name__ == "__main__":
    main()
```

---

### EXERCISE 3 — Git Push (Block E)

```bash
cd C:\Users\Lenovo\python-de-journey
git status

python scripts/daily_commit.py --day 17 --sprint 3 ^
    --message "Pandas depth: groupby transform, rolling, expanding, rank, shift, pct_change, time series on payment data" ^
    --merge
```

---

## ✅ DAY 17 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | Rental count discrepancy (46 vs 45) investigated + noted in dq_findings  | [✅]   |
| 2 | `window_analysis.py` runs — P1/P2/P3 all print expected output           | [✅]   |
| 3 | P1: `above_avg_flag` column present — ~50% above average                 | [✅]   |
| 4 | P2: `cumulative_spend` increases monotonically per customer              | [✅]   |
| 5 | P3: top customer at spend_rank=1 matches Day 02 Q4 result                | [✅]   |
| 6 | **P4 written by you — 4 rows, total ≈ 61312.04**                         | [✅]   |
| 7 | **P5 written by you — mom_growth_pct, shift, is_peak_month columns**     | [✅]   |
| 8 | `analytics_monthly_enriched` table written to PostgreSQL                 | [✅]   |
| 9 | Audit log entry written for both pipelines                               | [✅]   |
|10 | 4 CSV files in `sprint-03/day-17/output/`                                | [✅]   |
|11 | One clean `[DAY-017][S03]` commit via `daily_commit.py --merge`          | [✅]   |

---

## 🔍 P4 SELF-CHECK

```
Total revenue sum: run this to verify
python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_scalar, close_pool
total = execute_scalar('SELECT ROUND(SUM(amount)::numeric, 2) FROM payment')
print('Ground truth total payment:', total)
close_pool()
"
```

---

## 🔜 PREVIEW: DAY 18

**Topic:** pytest depth — fixtures, parametrize, mock DB correctly  
**What you'll do:** Rewrite `test_etl.py` from Day 13 using proper pytest fixtures,
`@pytest.mark.parametrize`, and correct mocking of `export_csv`. All 3 tests pass.
This is the last gap fill before the sprint test on Day 20.

---

*Day 17 | Sprint 03 | EP-04 | TASK-017*
