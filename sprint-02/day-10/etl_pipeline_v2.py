#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Path fixes
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))  # db_utils
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))  # logger

from db_utils import get_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("etl_v2")


class ETLPipelineV2:
    def __init__(self, pipeline_name: str):
        self.name = pipeline_name
        self.engine = get_engine()
        self.load_timestamp = datetime.now()
        logger.info("Initialized ETLPipelineV2: %s", pipeline_name)

    def run_customer_lifetime(self):
        logger.info("Running parameterised customer lifetime ETL...")

        sql = """
            SELECT 
                c.customer_id,
                c.first_name || ' ' || c.last_name AS full_name,
                COUNT(r.rental_id)                    AS total_rentals,
                ROUND(SUM(p.amount)::numeric, 2)      AS total_spend,
                ROUND(AVG(p.amount)::numeric, 2)      AS avg_spend_per_rental,
                MAX(r.rental_date)                    AS last_rental,
                %(load_timestamp)s                    AS load_timestamp
            FROM customer c
            LEFT JOIN rental r ON c.customer_id = r.customer_id
            LEFT JOIN payment p ON r.rental_id = p.rental_id
            GROUP BY c.customer_id, c.first_name, c.last_name
            ORDER BY total_spend DESC
        """

        df = pd.read_sql(sql, self.engine, params={"load_timestamp": self.load_timestamp})

        logger.info("Extracted %d customer records", len(df))

        df.to_sql("analytics_customer_lifetime_v2", self.engine, if_exists="replace", index=False)
        logger.info("✅ Loaded analytics_customer_lifetime_v2")

        Path("sprint-02/day-10/output").mkdir(exist_ok=True)
        df.to_csv("sprint-02/day-10/output/customer_lifetime_v2.csv", index=False)
        logger.info("📄 Exported v2 CSV")

        return df


if __name__ == "__main__":
    pipeline = ETLPipelineV2("customer_lifetime_v2")
    pipeline.run_customer_lifetime()
    logger.info("🎉 Parameterised ETL v2 completed successfully!")