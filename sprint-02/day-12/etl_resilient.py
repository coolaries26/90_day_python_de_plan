#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
import time

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))  # For db_utils
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))  # For logger
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-11" / "config"))  # For config

from db_utils import get_engine
from logger import get_pipeline_logger
from etl_config import settings   # from Day 11

logger = get_pipeline_logger("etl_resilient")


class ResilientETLPipeline:
    """Production-ready ETL with retry and error handling."""

    def __init__(self, max_retries: int = 3):
        self.engine = get_engine()
        self.max_retries = max_retries
        logger.info("Initialized Resilient ETL Pipeline | max_retries= {} ", max_retries)

    def run(self):
        """Main entry point with retry logic."""
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info("Attempt {} out of {}: Running customer lifetime ETL...", attempt, self.max_retries)
                
                df = self.extract()
                self.load(df)
                self.export_csv(df)
                
                logger.info("🎉 Resilient ETL completed successfully!")
                return df

            except Exception as e:
                logger.error("Attempt {} failed: {}", attempt, str(e), exc_info=True)
                
                if attempt == self.max_retries:
                    logger.critical("❌ All retry attempts failed. ETL pipeline aborted.")
                    raise
                else:
                    wait = attempt * 2
                    logger.warning("Retrying in {} seconds...", wait)
                    time.sleep(wait)

    def extract(self) -> pd.DataFrame:
        sql = """
            SELECT 
                c.customer_id,
                c.first_name || ' ' || c.last_name AS full_name,
                COUNT(r.rental_id)                    AS total_rentals,
                ROUND(SUM(p.amount)::numeric, 2)      AS total_spend,
                ROUND(AVG(p.amount)::numeric, 2)      AS avg_spend_per_rental,
                MAX(r.rental_date)                    AS last_rental,
                NOW()                                 AS load_timestamp
            FROM customer c
            LEFT JOIN rental r ON c.customer_id = r.customer_id
            LEFT JOIN payment p ON r.rental_id = p.rental_id
            GROUP BY c.customer_id, c.first_name, c.last_name
            ORDER BY total_spend DESC
        """
        return pd.read_sql(sql, self.engine)

    def load(self, df: pd.DataFrame):
        df.to_sql(settings.TARGET_TABLE, self.engine, if_exists="replace", index=False)
        logger.info("✅ Loaded {} rows into {}", len(df), settings.TARGET_TABLE)

    def export_csv(self, df: pd.DataFrame):
        Path("sprint-02/day-12/output").mkdir(exist_ok=True)
        path = f"sprint-02/day-12/output/{settings.OUTPUT_CSV}"
        df.to_csv(path, index=False)
        logger.info("📄 Exported CSV → {}", path)


if __name__ == "__main__":
    pipeline = ResilientETLPipeline(max_retries=3)
    pipeline.run()