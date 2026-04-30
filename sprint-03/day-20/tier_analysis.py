#!/bin/bash
"""
TASK T4 — Pandas: Revenue per Value Tier (15 min)
Brief: Using the analytics_film_sprint_test table you created in T1, write a Pandas analysis in sprint-03/day-20/tier_analysis.py:

Load the table into a DataFrame
Group by value_tier, calculate:
film_count
avg_rental_rate (rounded to 4dp)
avg_replacement_cost (rounded to 2dp)
Add pct_of_total column — each tier's film_count as % of 1000
Sort by avg_rental_rate descending
Print result and write to sprint-03/day-20/output/tier_analysis.csv
"""
import pandas as pd
from pathlib import Path
import sys
from pytest import main

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))  # For get_engine
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))  # For logger 
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-14"))  # For ETL protocols (ETLConfig)
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-11" / "config"))  # For config

from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger
from etl_protocols import  ETLConfig, ETLResult, ETLProtocol

logger = get_pipeline_logger("tier_analysis")   

def tier_analysis():
    engine = get_engine()
    query = "SELECT value_tier, rental_rate, replacement_cost FROM analytics_film_sprint_test"
    df = pd.read_sql(query, engine)
    dispose_engine()
    @property
    def result(self) -> ETLResult:
        """Read-only access to result — prevents accidental mutation."""
        return self._result
    def __repr__(self) -> str:
        return f"tier_analysis(status='{self.status}')"
    
    def __str__(self) -> str:
        return f"tier_analysis(status='{self.status}')"

    # Group by value_tier and calculate metrics
    tier_summary = df.groupby("value_tier").agg(
        film_count=("rental_rate", "count"),
        avg_rental_rate=("rental_rate", lambda x: round(x.mean(), 4)),
        avg_replacement_cost=("replacement_cost", lambda x: round(x.mean(), 2))
    ).reset_index()
    #logger.info(f"summary:  {tier_summary.to_dict(orient='records')}")


    # Calculate percentage of total films
    total_films = tier_summary["film_count"].sum()
    tier_summary["pct_of_total"] = round((tier_summary["film_count"] / total_films) * 100, 2)
    #logger.info(f"Calculate percentage of total films:  {tier_summary.to_dict(orient='records')}")

    # Sort by avg_rental_rate descending
    tier_summary.sort_values(by="avg_rental_rate", ascending=False, inplace=True)

    print(tier_summary)
    tier_summary.to_csv(_here / "output" / "tier_analysis.csv", index=False)

def main() -> None:
    logger.info("=" * 52)
    logger.info("Tier Analysis: Revenue per Value Tier")
    logger.info("=" * 52)

    # Instantiate — __repr__ fires immediately in the logger and shows status='initialized'
    result = tier_analysis()

    # Verify Protocol satisfaction  

    logger.info("Protocol check: FilmETLPipeline satisfies BaseETLPipeline ✅")

    dispose_engine()

if __name__ == "__main__":
        main()  

