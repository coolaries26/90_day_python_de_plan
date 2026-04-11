# db_explorer.py — DVD Rental Schema Introspector
# ================================================
# Programmatically documents the dvdrental schema:
#   - Table list with row counts
#   - Column details (name, type, nullable, default)
#   - Primary and foreign keys
#   - Table sizes on disk
# 
# Run: python db_explorer.py
# Output: sprint-01/day-02/output/schema_report.md

import csv
import os
import sys
from pathlib import Path
from datetime import datetime

# Make db_utils importable from this directory
sys.path.insert(0, str(Path(__file__).parent))
from db_utils import execute_query, execute_scalar, close_pool, dispose_engine

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def get_tables() -> list[dict]:
    """Return all public tables with row counts and disk size."""
    sql = """
        SELECT
            t.tablename                                          AS table_name,
            pg_size_pretty(pg_total_relation_size(
                quote_ident(t.tablename)::regclass))             AS total_size,
            pg_size_pretty(pg_relation_size(
                quote_ident(t.tablename)::regclass))             AS table_size,
            (SELECT COUNT(*) FROM information_schema.columns c
             WHERE c.table_name = t.tablename
               AND c.table_schema = 'public')                    AS column_count,
            obj_description(
                quote_ident(t.tablename)::regclass, 'pg_class') AS description
        FROM pg_tables t
        WHERE t.schemaname = 'public'
        ORDER BY pg_total_relation_size(
            quote_ident(t.tablename)::regclass) DESC;
    """
    rows = execute_query(sql, as_dict=True)

    # Add exact row counts (pg_class.reltuples is an estimate)
    for row in rows:
        row["row_count"] = execute_scalar(
            f"SELECT COUNT(*) FROM {row['table_name']}"   # table name from pg_tables is safe
        )
    return rows


def get_columns(table_name: str) -> list[dict]:
    """Return column metadata for a given table."""
    sql = """
        SELECT
            c.ordinal_position                      AS pos,
            c.column_name,
            c.data_type,
            c.character_maximum_length              AS max_len,
            c.is_nullable,
            c.column_default,
            CASE WHEN pk.column_name IS NOT NULL
                 THEN 'YES' ELSE 'NO' END           AS is_primary_key
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT ku.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku
              ON tc.constraint_name = ku.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_name = %s
              AND tc.table_schema = 'public'
        ) pk ON c.column_name = pk.column_name
        WHERE c.table_name = %s
          AND c.table_schema = 'public'
        ORDER BY c.ordinal_position;
    """
    return execute_query(sql, params=(table_name, table_name), as_dict=True)


def get_foreign_keys() -> list[dict]:
    """Return all foreign key relationships across the schema."""
    sql = """
        SELECT
            tc.table_name        AS from_table,
            kcu.column_name      AS from_column,
            ccu.table_name       AS to_table,
            ccu.column_name      AS to_column,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema    = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema    = 'public'
        ORDER BY tc.table_name, kcu.column_name;
    """
    return execute_query(sql, as_dict=True)


def write_schema_report(tables: list[dict], fk_list: list[dict]) -> Path:
    """Write a markdown schema report."""
    report_path = OUTPUT_DIR / "schema_report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# DVD Rental Schema Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Table summary
        f.write("## Tables\n\n")
        f.write("| Table | Rows | Columns | Size |\n")
        f.write("|-------|------|---------|------|\n")
        for t in tables:
            f.write(
                f"| {t['table_name']} | {t['row_count']:,} "
                f"| {t['column_count']} | {t['total_size']} |\n"
            )

        # Column details per table
        f.write("\n## Column Details\n\n")
        for t in tables:
            cols = get_columns(t["table_name"])
            f.write(f"### {t['table_name']}\n\n")
            f.write("| # | Column | Type | Nullable | PK |\n")
            f.write("|---|--------|------|----------|----|\n")
            for c in cols:
                f.write(
                    f"| {c['pos']} | {c['column_name']} | {c['data_type']} "
                    f"| {c['is_nullable']} | {c['is_primary_key']} |\n"
                )
            f.write("\n")

        # Foreign keys
        f.write("## Foreign Keys\n\n")
        f.write("| From Table | From Column | → | To Table | To Column |\n")
        f.write("|------------|-------------|---|----------|-----------|\n")
        for fk in fk_list:
            f.write(
                f"| {fk['from_table']} | {fk['from_column']} | → "
                f"| {fk['to_table']} | {fk['to_column']} |\n"
            )

    return report_path


def write_tables_csv(tables: list[dict]) -> Path:
    """Write table summary to CSV for verification."""
    csv_path = OUTPUT_DIR / "tables_summary.csv"
    if tables:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=tables[0].keys())
            writer.writeheader()
            writer.writerows(tables)
    return csv_path


def main():
    print("\n📐 DVD Rental Schema Explorer")
    print("=" * 40)

    print("\n[1] Fetching table inventory...")
    tables = get_tables()
    print(f"    Found {len(tables)} tables")
    for t in tables:
        print(f"    {t['table_name']:<20} {t['row_count']:>7,} rows  {t['total_size']}")

    print("\n[2] Fetching foreign keys...")
    fk_list = get_foreign_keys()
    print(f"    Found {len(fk_list)} foreign key relationships")

    print("\n[3] Writing schema report...")
    report = write_schema_report(tables, fk_list)
    print(f"    → {report}")

    print("\n[4] Writing tables CSV...")
    csv_out = write_tables_csv(tables)
    print(f"    → {csv_out}")

    print("\n✅ Done. Review output/ folder.\n")

    # Clean up connections
    close_pool()
    dispose_engine()


if __name__ == "__main__":
    main()
