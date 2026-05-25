#!/usr/bin/env python3
"""
load_raw_data.py — Capstone Day 43
====================================
Loads all 8 Olist CSV files into PostgreSQL raw schema.
Run once to populate raw tables.
Run: python capstone/etl/load_raw_data.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04")) # for logger.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from logger import get_pipeline_logger
from db import get_ecommerce_engine, dispose_ecommerce_engine

logger = get_pipeline_logger("load_raw_data")

RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Mapping: CSV filename → target table name in raw schema
CSV_TABLE_MAP = {
    "olist_orders_dataset.csv":              "orders",
    "olist_order_items_dataset.csv":         "order_items",
    "olist_order_payments_dataset.csv":      "order_payments",
    "olist_order_reviews_dataset.csv":       "order_reviews",
    "olist_customers_dataset.csv":           "customers",
    "olist_sellers_dataset.csv":             "sellers",
    "olist_products_dataset.csv":            "products",
    "product_category_name_translation.csv": "product_category_translation",
}


# ── Q1: Load one CSV — provided ───────────────────────────────────────────
def load_csv_to_db(
    csv_path: Path,
    table_name: str,
    engine,
    schema: str = "raw",
) -> int:
    """Load a single CSV file into PostgreSQL. Returns rows loaded."""
    if not csv_path.exists():
        logger.warning(f"File not found: {csv_path.name} — skipping")
        return 0

    logger.info(f"Loading {csv_path.name}...")
    df = pd.read_csv(csv_path, low_memory=False)
    logger.info(f"  Read: {len(df):,} rows × {len(df.columns)} cols")

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    logger.info(f"  ✅ Loaded → {schema}.{table_name} ({len(df):,} rows)")
    return len(df)


# ── Q2: Load all tables — WRITE THIS YOURSELF ─────────────────────────────
def load_all_tables() -> dict[str, int]:
    """
    Q2 — YOUR TASK:
    Load all 8 CSV files using load_csv_to_db().
    Return a dict of {table_name: row_count}.

    HINTS:
    engine = get_ecommerce_engine()
    results = {}
    for csv_file, table_name in CSV_TABLE_MAP.items():
        csv_path = RAW_DATA_DIR / csv_file
        count = load_csv_to_db(csv_path, table_name, engine)
        results[table_name] = count

    logger.info(f"\nLoad Summary:")
    total = 0
    for table, count in results.items():
        logger.info(f"  raw.{table:<40} {count:>8,} rows")
        total += count
    logger.info(f"  Total rows loaded: {total:,}")

    dispose_ecommerce_engine()
    return results
    """
    # YOUR CODE HERE
    engine = get_ecommerce_engine()
    results = {}
    for csv_file, table_name in CSV_TABLE_MAP.items():
        csv_path = RAW_DATA_DIR / csv_file
        count = load_csv_to_db(csv_path, table_name, engine)
        results[table_name] = count

    return results


def main() -> None:
    logger.info("=" * 52)
    logger.info("Loading Raw Data — Day 43 Capstone")
    logger.info("=" * 52)
    logger.info(f"Data directory: {RAW_DATA_DIR}")

    results = load_all_tables()
    logger.info(f"\n✅ All tables loaded. Total: {sum(results.values()):,} rows")


if __name__ == "__main__":
    main()