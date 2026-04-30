#!/usr/bin/env python3
"""
Brief: Without looking at oop_etl.py, create a new pipeline class FilmETLPipeline that inherits from BaseETLPipeline and processes the film table.

Requirements:

Inherits BaseETLPipeline from sprint-03/day-14/oop_etl.py
ETLConfig(source_table="film", target_table="analytics_film_sprint_test")
extract() → loads all films with category via JOIN
transform(df) → adds value_tier column:
rental_rate <= 0.99 → "Budget"
rental_rate <= 2.99 → "Standard"
rental_rate > 2.99 → "Premium"
load(df) → writes to analytics_film_sprint_test table
__repr__ inherited — must show status='success' after running the pipeline
"""
from abc import ABC, abstractmethod
import os
import sys
import time
import pandas as pd
import psycopg2

from pathlib import Path
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))  # For db_utils
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))  # For logger
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-14"))  # For BaseETLPipeline and ETLConfig
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-11" / "config"))  # For config

from db_utils import get_engine, dispose_engine
from etl_config import settings
from etl_protocols import  ETLConfig, ETLResult, ETLProtocol
from oop_etl import BaseETLPipeline
from logger import get_pipeline_logger

logger = get_pipeline_logger("film_etl_pipeline")
    

class FilmETLPipeline(ABC):
    def __init__(self, config: ETLConfig) -> None:
        self.config   = config
        self.engine   = get_engine()
        self._result  = ETLResult(
            pipeline_name=self.__class__.__name__,
            source_table=config.source_table,
            target_table=config.target_table,
        )
        self._attempt = 0
        logger.info(f"Pipeline initialised | {self!r}")


    def __repr__(self) -> str:
        return f"FilmETLPipeline(status='{self.status}')"
    
    def __str__(self) -> str:
        return f"FilmETLPipeline(status='{self.status}')"
  
    @property
    def status(self) -> str:
        """Current pipeline status derived from result object."""
        return str(self._result.status)
    @property
    def result(self) -> ETLResult:
        """Read-only access to result — prevents accidental mutation."""
        return self._result

    def pre_run(self) -> None:
        """Called before first attempt. Override for setup logic."""
        pass

    def post_run(self, result: ETLResult) -> None:
        """Called after run completes (success or failure). Override for cleanup."""
        pass

    def extract(self) -> pd.DataFrame:
        query = """
        SELECT f.*, c.name AS category
        FROM film f
        left JOIN film_category fc ON f.film_id = fc.film_id
        left JOIN category c ON fc.category_id = c.category_id
        """
        return pd.read_sql(query, self.engine)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        def categorize_rental_rate(rate: float) -> str:
            if rate <= 0.99:
                return "Budget"
            elif rate <= 2.99:
                return "Standard"
            else:
                return "Premium"

        df['value_tier'] = df['rental_rate'].apply(categorize_rental_rate)
        return df

    def load(self, df: pd.DataFrame) -> int:
            df.to_sql(self.config.target_table, self.engine, if_exists="replace", index=False)
            logger.info("✅ Loaded {} rows into {}", len(df), self.config.target_table)
            return len(df)
    def export_csv(self, df: pd.DataFrame) -> Path:
        # Always anchor paths to the file's own location — never relative strings
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(parents=True, exist_ok=True)   # parents=True handles missing parents
        path = output_dir / self.config.output_csv
        df.to_csv(path, index=False)
        logger.info(f"✅ Exported CSV -> {len(df)} rows at {path}")
        return Path(path)
    
    # ── Template method — shared retry logic ──────────────────────────────────
    def run(self) -> ETLResult:
        """
        Main entry point. Calls extract → transform → load with retry.
        Subclasses should NOT override this — override extract/transform/load.
        """
        self.pre_run()
        logger.info(f"Pipeline START | {self!r}")

        for attempt in range(1, self.config.max_retries + 1):
            self._attempt = attempt
            try:
                logger.info(f"Attempt {attempt}/{self.config.max_retries}")

                df_raw        = self.extract()
                df_transformed = self.transform(df_raw)
                csv_export = self.export_csv(df_transformed)
                rows_loaded   = self.load(df_transformed)

                self._result.complete(
                    rows_extracted=len(df_raw),
                    rows_loaded=rows_loaded,
                    attempts=attempt,
                    export_csv=csv_export
                )
                logger.info(f"Pipeline SUCCESS | {self!r} | "
                            f"rows={rows_loaded} "
                            f"elapsed={self._result.elapsed_seconds:.2f}s")
                break

            except Exception as exc:
                logger.error(f"Attempt {attempt} failed | error={exc}")
                if attempt == self.config.max_retries:
                    self._result.fail(str(exc), attempt)
                    logger.critical(f"Pipeline FAILED | {self!r} | error={exc}")
                    raise
                wait = attempt * self.config.retry_wait_s
                logger.warning(f"Retrying in {wait}s...")
                time.sleep(wait)

        self.post_run(self._result)
        return self._result


def main() -> None:
    logger.info("=" * 52)
    logger.info("OOP ETL Demo — Day 14")
    logger.info("=" * 52)

    # Config via dataclass — clean, typed, validated
    config = ETLConfig(
        source_table="film",
        target_table="analytics_film_sprint_test",
        max_retries=2,
        output_dir=Path(__file__).parent / "output",
    )
    logger.info(f"Config: {config}")

    # Instantiate — __repr__ fires immediately in the logger and shows status='initialized'
    pipeline = FilmETLPipeline(config=config)
    logger.info(f"Pipeline repr: {pipeline!r}")
    logger.info(f"Pipeline str:  {pipeline}")

    # Run
    pipeline = FilmETLPipeline(config=config)
    result = pipeline.run()
    logger.info(f"Result:  {result}")
    logger.info(f"Success: {result.success}")
    logger.info(f"Rows:    {result.rows_loaded}")
    logger.info(f"CSV:     {result.export_csv}")
    logger.info(f"Elapsed: {result.elapsed_seconds:.2f}s")

    # Verify Protocol satisfaction  

    assert isinstance(pipeline, ETLProtocol), "Pipeline does not satisfy BaseETLPipeline protocol"
    assert pipeline.status == "success", "Pipeline did not complete successfully"
    logger.info("Protocol check: FilmETLPipeline satisfies BaseETLPipeline ✅")

    dispose_engine()

if __name__ == "__main__":
    main()