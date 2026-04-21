#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))   # For db_utils
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))   # For logger


from db_utils import get_engine
from logger import get_pipeline_logger
from config.etl_config import settings

logger = get_pipeline_logger("etl_config")


class ConfigETLPipeline:
    def __init__(self):
        self.engine = get_engine()
        logger.info("Initialized Config-Driven ETL | Pipeline: {}", settings.PIPELINE_NAME)

    def run(self):
        logger.info("Running config-driven customer lifetime ETL...")

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

        df = pd.read_sql(sql, self.engine)
        logger.info("Extracted {} rows", len(df))

        # Load using config
        df.to_sql(settings.TARGET_TABLE, self.engine, if_exists="replace", index=False)
        logger.info("✅ Loaded table: {}", settings.TARGET_TABLE)

        # Export using config
        Path("sprint-02/day-11/output").mkdir(exist_ok=True)
        df.to_csv(f"sprint-02/day-11/output/{settings.OUTPUT_CSV}", index=False)
        logger.info("📄 Exported CSV → {}", settings.OUTPUT_CSV)

        logger.info("🎉 Config-Driven ETL completed successfully!")


if __name__ == "__main__":
    pipeline = ConfigETLPipeline()
    pipeline.run()
    