#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))   #db_utils
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))   #logger

from db_utils import get_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("etl_framework")


class ETLPipeline:
    """Reusable ETL pipeline class for the rest of the program."""

    def __init__(self, pipeline_name: str):
        self.name = pipeline_name
        self.engine = get_engine()
        logger.info("Initialized ETL Pipeline: %s", pipeline_name)

    def extract(self, sql: str) -> pd.DataFrame:
        logger.info("Extracting data with query...")
        df = pd.read_sql(sql, self.engine)
        logger.info("Extracted %d rows", len(df))
        return df

    def load(self, df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
        logger.info("Loading data into table: %s", table_name)
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        logger.info("✅ Loaded %d rows into %s", len(df), table_name)

    def export_csv(self, df: pd.DataFrame, filename: str):
        Path("sprint-02/day-09/output").mkdir(exist_ok=True)
        path = f"sprint-02/day-09/output/{filename}"
        df.to_csv(path, index=False)
        logger.info("📄 Exported CSV → %s", path)


def main():
    pipeline = ETLPipeline("customer_lifetime")

    sql = """
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name AS full_name,
            COUNT(r.rental_id)                    AS total_rentals,
            ROUND(SUM(p.amount)::numeric, 2)      AS total_spend,
            ROUND(AVG(p.amount)::numeric, 2)      AS avg_spend_per_rental
        FROM customer c
        LEFT JOIN rental r ON c.customer_id = r.customer_id
        LEFT JOIN payment p ON r.rental_id = p.rental_id
        GROUP BY c.customer_id, c.first_name, c.last_name
        ORDER BY total_spend DESC
    """

    df = pipeline.extract(sql)
    pipeline.load(df, "analytics_customer_lifetime")
    pipeline.export_csv(df, "customer_lifetime_day09.csv")

    logger.info("🎉 Modular ETL completed successfully!")


if __name__ == "__main__":
    main()