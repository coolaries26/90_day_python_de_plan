#!/usr/bin/env python3
"""
data_explorer.py — Day 03 | Pandas Transformations
====================================================
Three transformations on DVD Rental data:
  T1: Customer enrichment (merge customer + rental + payment)
  T2: Film value score (derived column from existing fields)
  T3: [You write] Monthly cohort summary

Results written back to PostgreSQL as analytics tables.

Run: python data_explorer.py
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent / "day-02"))
from db_utils import get_engine, dispose_engine

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

engine = get_engine()


# ── T1: Customer enrichment ────────────────────────────────────────────────────
def t1_customer_summary() -> pd.DataFrame:
    """
    Transform 1: Merge customer + rental + payment into a customer summary.
    This is the foundation of a customer analytics table.

    Steps:
      1. Load three tables via SQL (push aggregation to DB — faster than Python)
      2. Merge on customer_id
      3. Derive new columns: customer_segment, days_since_last_rental
      4. Handle nulls in return_date (open rentals)
    """
    print("\n── T1: Customer Enrichment ──────────────────────────")

    # Load base tables
    customers = pd.read_sql("SELECT customer_id, first_name, last_name, email, active, create_date FROM customer", engine)
    spend = pd.read_sql("""
        SELECT
            customer_id,
            COUNT(rental_id)         AS total_rentals,
            ROUND(SUM(amount)::numeric, 2)  AS total_spend,
            MIN(payment_date)        AS first_payment,
            MAX(payment_date)        AS last_payment
        FROM payment
        GROUP BY customer_id
    """, engine)

    # Merge — equivalent to a LEFT JOIN
    df = customers.merge(spend, on="customer_id", how="left")
    print(f"  After merge: {df.shape}")

    # Derive: full name
    df["full_name"] = df["first_name"] + " " + df["last_name"]

    # Derive: customer segment based on total_spend
    # pd.cut() is like a CASE WHEN with value ranges
    df["segment"] = pd.cut(
        df["total_spend"].fillna(0),
        bins=[0, 50, 100, 150, float("inf")],
        labels=["Bronze", "Silver", "Gold", "Platinum"],
        right=True,
    )

    # Derive: days since last payment (relative to max date in dataset)
    max_date = df["last_payment"].max()
    df["days_since_last_payment"] = (max_date - df["last_payment"]).dt.days

    # Active flag: convert int → bool (DQ fix from profile step)
    df["is_active"] = df["active"].astype(bool)

    # Select final columns
    result = df[[
        "customer_id", "full_name", "email", "is_active",
        "segment", "total_rentals", "total_spend",
        "first_payment", "last_payment", "days_since_last_payment",
    ]].sort_values("total_spend", ascending=False)

    print(f"  Segments: {result['segment'].value_counts().to_dict()}")
    print(f"  Nulls in total_spend: {result['total_spend'].isna().sum()}")
    print(result.head(3).to_string(index=False))
    return result


# ── T2: Film value score ───────────────────────────────────────────────────────
def t2_film_value_score() -> pd.DataFrame:
    """
    Transform 2: Calculate a 'value score' for each film.
    Value = (rental_rate / rental_duration) * log(rental_count + 1)
    Higher score = high rate, short duration, frequently rented.
    This is a feature engineering pattern used in ML pipelines.

    Steps:
      1. Load film + rental counts from DB
      2. Apply numpy-based derived column
      3. Normalise score to 0–100 range (min-max scaling — preview of Sprint 12)
    """
    print("\n── T2: Film Value Score ─────────────────────────────")
    import numpy as np

    films = pd.read_sql("""
        SELECT
            f.film_id, f.title, f.rating, f.rental_rate,
            f.rental_duration, f.length, f.replacement_cost,
            c.name AS category,
            COUNT(r.rental_id) AS rental_count
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c       ON fc.category_id = c.category_id
        JOIN inventory i      ON f.film_id = i.film_id
        LEFT JOIN rental r    ON i.inventory_id = r.inventory_id
        GROUP BY f.film_id, f.title, f.rating, f.rental_rate,
                 f.rental_duration, f.length, f.replacement_cost, c.name
    """, engine)

    print(f"  Films loaded: {len(films)}")

    # Derived column: raw value score
    films["value_score_raw"] = (
        (films["rental_rate"] / films["rental_duration"]) *
        np.log1p(films["rental_count"])   # log1p = log(x+1) handles zero
    )

    # Min-max normalise to 0–100
    v_min = films["value_score_raw"].min()
    v_max = films["value_score_raw"].max()
    films["value_score"] = (
        (films["value_score_raw"] - v_min) / (v_max - v_min) * 100
    ).round(2)

    result = films[[
        "film_id", "title", "category", "rating",
        "rental_rate", "rental_duration", "rental_count",
        "value_score",
    ]].sort_values("value_score", ascending=False)

    print(f"  Top film: {result.iloc[0]['title']} (score: {result.iloc[0]['value_score']})")
    print(f"  Score range: {result['value_score'].min()} – {result['value_score'].max()}")
    return result


# ── T3: WRITE THIS YOURSELF (hints below) ─────────────────────────────────────
def t3_monthly_revenue_cohort() -> pd.DataFrame:
    """
    Transform 3: Monthly revenue summary with month-over-month growth %.

    HINTS:
      - Load from payment table: payment_date, amount
      - Use pd.to_datetime() to ensure payment_date is datetime dtype
      - Use df['payment_date'].dt.to_period('M') to get year-month period
      - Group by the period column, aggregate: sum(amount), count(payment_id)
      - Derive: mom_growth_pct = revenue.pct_change() * 100, round to 2dp
      - pct_change() returns NaN for first row — that's correct, leave it
      - Expected output columns:
            month, total_revenue, payment_count, avg_payment, mom_growth_pct
      - Expected rows: 4 (matches Q3 from Day 02)

    Self-check:
      - July 2005 should be the peak revenue month
      - mom_growth_pct for June → July should be a large positive number (~240%)
    """
    print("\n── T3: Monthly Revenue Cohort ───────────────────────")

    # YOUR CODE HERE
    # Step 1: load payment table
    payments = pd.read_sql("SELECT payment_date, amount FROM payment", engine)
    # Step 2: ensure datetime dtype
    payments["payment_date"] = pd.to_datetime(payments["payment_date"])
    # Step 3: create month period column
    payments["month"] = payments["payment_date"].dt.to_period('M')
    # Step 4: groupby + aggregate
    #monthly_revenue = payments.groupby("month").agg(
    monthly_revenue = payments.groupby(payments["payment_date"].dt.to_period("M")).agg(
        payment_count=("amount", "count"),
        total_revenue=("amount", "sum"),
#        avg_payment=("amount", "mean")
    ).reset_index()
    # Step 5: derive mom_growth_pct
    # Convert Period to string for DB compatibility
    monthly_revenue["month"] = monthly_revenue["payment_date"].astype(str)
    monthly_revenue = monthly_revenue.drop("payment_date", axis=1)
    
    # Calculate MoM growth
    monthly_revenue["mom_growth_pct"] = (
        monthly_revenue["total_revenue"].pct_change() * 100
    ).round(2)

    monthly_revenue["avg_payment"] = (monthly_revenue["total_revenue"] / monthly_revenue["payment_count"]).round(2)
    monthly_revenue["mom_growth_pct"] = monthly_revenue["mom_growth_pct"].round(2)  
    # Step 5: derive mom_growth_pct
    result = monthly_revenue.reset_index()[[
        "month", "total_revenue", "payment_count", "avg_payment", "mom_growth_pct"
    ]]
    print(result.to_string(index=False))

    # Step 6: return clean DataFrame
    return result;
    #raise NotImplementedError("Implement T3 — see hints above")


# ── Write DataFrames back to PostgreSQL ───────────────────────────────────────
def write_to_db(df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> None:
    """
    Write a DataFrame to PostgreSQL.
    if_exists='replace' → drop and recreate table (safe for analytics tables)
    if_exists='append'  → add rows (for incremental loads)

    Note: to_sql() uses SQLAlchemy engine — connection managed by pool.
    """
    df.to_sql(
        name=table_name,
        con=engine,
        schema="public",
        if_exists="replace",
        index=False,
        method="multi",      # batch insert — faster than row-by-row
        chunksize=500,
    )
    # Verify write
    count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {table_name}", engine).iloc[0]["n"]
    print(f"  ✅ Written to DB: {table_name}  ({count:,} rows)")


def write_csv(df: pd.DataFrame, filename: str) -> None:
    path = OUTPUT_DIR / filename
    df.to_csv(path, index=False)
    print(f"  📄 CSV → {path.name}")


def main():
    print("\n🔄 DVD Rental — Pandas Transformations")
    print("=" * 52)

    # T1
    t1 = t1_customer_summary()
    write_to_db(t1, "analytics_customer_summary")
    write_csv(t1, "t1_customer_summary.csv")

    # T2
    t2 = t2_film_value_score()
    write_to_db(t2, "analytics_film_value_score")
    write_csv(t2, "t2_film_value_score.csv")

    # T3 — your implementation
    try:
        t3 = t3_monthly_revenue_cohort()
        write_to_db(t3, "analytics_monthly_cohort")
        write_csv(t3, "t3_monthly_cohort.csv")
    except NotImplementedError as e:
        print(f"  ⏳ {e}")

    # Verify analytics tables exist in DB
    print("\n── Analytics Tables in PostgreSQL ───────────────────")
    tables = pd.read_sql(
        text("""
            SELECT tablename,
                   pg_size_pretty(pg_total_relation_size(quote_ident(tablename)::regclass)) AS size
            FROM pg_tables
            WHERE tablename LIKE 'analytics_%'
            ORDER BY tablename
        """),
        engine
    )
    print(tables.to_string(index=False))

    dispose_engine()
    print("\n✅ Done.\n")


if __name__ == "__main__":
    main()