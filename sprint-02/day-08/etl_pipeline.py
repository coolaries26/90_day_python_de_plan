#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))

from db_utils import get_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("etl_day08")


def main():
    logger.info("🚀 Starting First ETL Pipeline — Day 08")
    engine = get_engine()

    logger.info("Running customer lifetime analytics query...")
    
    df = pd.read_sql("""
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name AS full_name,
            c.email,
            COUNT(r.rental_id)                    AS total_rentals,
            ROUND(SUM(p.amount)::numeric, 2)      AS total_spend,
            ROUND(AVG(p.amount)::numeric, 2)      AS avg_spend_per_rental,
            MAX(r.rental_date)                    AS last_rental_date,
            MIN(r.rental_date)                    AS first_rental_date
        FROM customer c
        LEFT JOIN rental r ON c.customer_id = r.customer_id
        LEFT JOIN payment p ON r.rental_id = p.rental_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email
        ORDER BY total_spend DESC
    """, engine)

    logger.info("Transformed customer analytics → {} rows", len(df))

    df.to_sql("analytics_customer_lifetime", engine, if_exists="replace", index=False)
    logger.info("✅ Loaded table: analytics_customer_lifetime")

    output_path = "sprint-02/day-08/output/customer_lifetime_value.csv"
    df.to_csv(output_path, index=False)
    logger.info("📄 Exported CSV → {}", output_path)

    logger.info("🎉 ETL Pipeline completed successfully!")


if __name__ == "__main__":
    main()