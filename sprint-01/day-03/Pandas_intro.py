#!/usr/bin/env python3
"""
pandas_intro.py — Day 03 | Pandas Fundamentals
================================================
Load DVD Rental tables into DataFrames and profile them.
Demonstrates the core read → inspect → understand workflow.

Run: python pandas_intro.py
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "day-02"))
from db_utils import get_engine, dispose_engine

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ── 1. Load tables ─────────────────────────────────────────────────────────────
def load_tables() -> dict[str, pd.DataFrame]:
    """
    Load 4 core DVD Rental tables into DataFrames.
    pd.read_sql() handles connection + result → DataFrame in one call.
    The engine is passed in — pd.read_sql closes the connection after fetching.
    """
    engine = get_engine()
    tables = {
        "film":     "SELECT * FROM film",
        "customer": "SELECT * FROM customer",
        "rental":   "SELECT * FROM rental",
        "payment":  "SELECT * FROM payment",
        #extra tables for bonus profiling
        "actor": "SELECT * FROM actor",
        "address": "SELECT * FROM address",
        "analytics_customer_summary": "SELECT * FROM analytics_customer_summary",
        "analytics_film_value_score": "SELECT * FROM analytics_film_value_score",
        "analytics_monthly_cohort": "SELECT * FROM analytics_monthly_cohort",
        "category": "SELECT * FROM category",
        "city": "SELECT * FROM city",
        "country": "SELECT * FROM country",
        "customer": "SELECT * FROM customer",
        "film": "SELECT * FROM film",
        "film_actor": "SELECT * FROM film_actor",
        "film_category": "SELECT * FROM film_category",
        "inventory": "SELECT * FROM inventory",
        "language": "SELECT * FROM language",
        "payment": "SELECT * FROM payment",
        "rental": "SELECT * FROM rental",
        "staff": "SELECT * FROM staff",
        "store": "SELECT * FROM store",
    }
    dfs: dict[str, pd.DataFrame] = {}
    for name, sql in tables.items():
        dfs[name] = pd.read_sql(sql, engine)
        print(f"  ✅ {name:<12} loaded  →  {dfs[name].shape[0]:>6,} rows × {dfs[name].shape[1]} cols")
    return dfs


# ── 2. Profile each DataFrame ─────────────────────────────────────────────────
def profile_dataframe(name: str, df: pd.DataFrame) -> dict:
    """
    Run .info() and .describe() and capture key stats.
    Returns a dict summary for the report.
    """
    print(f"\n{'═'*54}")
    print(f"  TABLE: {name.upper()}")
    print(f"{'═'*54}")

    # .info() — dtypes, non-null counts, memory usage
    print("\n  [Schema — df.info()]")
    df.info(buf=sys.stdout, memory_usage="deep")

    # .describe() — for numeric columns
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if numeric_cols:
        print(f"\n  [Numeric Stats — df.describe()]")
        desc = df[numeric_cols].describe().round(4)
        print(desc.to_string())

    # Null check — critical for pipeline validation
    null_counts = df.isna().sum()
    null_cols = null_counts[null_counts > 0]
    print(f"\n  [Null Check]")
    if null_cols.empty:
        print("  No nulls found — clean table")
    else:
        for col, cnt in null_cols.items():
            pct = cnt / len(df) * 100
            print(f"  ⚠️  {col}: {cnt} nulls ({pct:.1f}%)")

    # Duplicate check (line 75) with fix ──
    try:
        dupes = df.duplicated().sum()
    except TypeError:
        # Handle columns with unhashable types (e.g., lists)
        hashable_cols = [col for col in df.columns 
                     if df[col].dtype == 'object' and not df[col].apply(lambda x: isinstance(x, list)).any()]
        if hashable_cols:
            dupes = df[hashable_cols].duplicated().sum()
        else:
            dupes = 0
        print(f"  ⚠️  Skipped unhashable columns (lists/dicts); checked {len(hashable_cols)} hashable cols")

    return {
        "table":        name,
        "rows":         len(df),
        "columns":      len(df.columns),
        "null_columns": len(null_cols),
        "duplicates":   int(dupes),
        "memory_mb":    round(df.memory_usage(deep=True).sum() / 1_048_576, 3),
        "dtypes":       df.dtypes.value_counts().to_dict(),
    }


# ── 3. Write profile summary ──────────────────────────────────────────────────
def write_profile_report(summaries: list[dict]) -> None:
    report_path = OUTPUT_DIR / "profile_report.md"
    with open(report_path, "w") as f:
        f.write("# DataFrame Profile Report — Day 03\n\n")
        f.write("| Table | Rows | Columns | Null Cols | Duplicates | Memory (MB) |\n")
        f.write("|-------|------|---------|-----------|------------|-------------|\n")
        for s in summaries:
            f.write(
                f"| {s['table']} | {s['rows']:,} | {s['columns']} "
                f"| {s['null_columns']} | {s['duplicates']} | {s['memory_mb']} |\n"
            )
    print(f"\n  📄 Profile report → {report_path.name}")


def main():
    print("\n🐼 Pandas Intro — Load & Profile DVD Rental Tables")
    print("=" * 54)

    print("\n[1] Loading tables from PostgreSQL...")
    dfs = load_tables()

    print("\n[2] Profiling each DataFrame...")
    summaries = []
    for name, df in dfs.items():
        summary = profile_dataframe(name, df)
        summaries.append(summary)

    print("\n[3] Writing profile report...")
    write_profile_report(summaries)

    dispose_engine()
    print("\n✅ Done. Review output/profile_report.md\n")


if __name__ == "__main__":
    main()
