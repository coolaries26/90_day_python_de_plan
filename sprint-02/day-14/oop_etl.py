#!/usr/bin/env python3
"""
oop_etl.py — Day 14 | OOP-Refactored ETL Pipeline
===================================================
Demonstrates:
  - Abstract base class with shared retry logic
  - __repr__ and __str__ for logging visibility
  - @property for computed state
  - Protocol satisfaction without inheritance
  - ETLConfig dataclass as configuration

Run: python oop_etl.py
"""

from __future__ import annotations

import sys
import time
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
from sqlalchemy import sql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02")) # For db_utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04")) # For logger
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-02" / "day-11" / "config")) # For config
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger
from etl_config import settings   # from Day 11, for config values like OUTPUT_CSV
from etl_protocols import ETLConfig, ETLResult, ETLProtocol

logger = get_pipeline_logger("oop_etl")


# ── Q1: BaseETLPipeline — fully provided ──────────────────────────────────────
class BaseETLPipeline(ABC):
    """
    Abstract base class for all ETL pipelines.
    Provides:
      - Retry logic (shared — subclasses don't re-implement this)
      - __repr__ for logging visibility
      - @property for status tracking
      - run() template method that calls extract/transform/load

    Subclasses MUST implement: extract(), transform(), load()
    Subclasses MAY override:   pre_run(), post_run()
    """

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
        """
        Machine-readable representation — used by loggers and debuggers.
        Every subclass inherits this automatically.
        """
        return (
            f"{self.__class__.__name__}("
            f"source='{self.config.source_table}', "
            f"target='{self.config.target_table}', "
            f"max_retries={self.config.max_retries}, "
            f"status='{self.status}')"
        )

    def __str__(self) -> str:
        """Human-readable — used by print() and str()."""
        return (
            f"{self.__class__.__name__}: "
            f"{self.config.source_table} -> {self.config.target_table} "
            f"[{self.status}]"
        )

    @property
    def status(self) -> str:
        """Current pipeline status derived from result object."""
        return self._result.status

    @property
    def result(self) -> ETLResult:
        """Read-only access to result — prevents accidental mutation."""
        return self._result

    # ── Abstract methods — subclasses MUST implement these ────────────────────
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Pull data from source table."""
        ...

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business logic transformations."""
        ...

    @abstractmethod
    def load(self, df: pd.DataFrame) -> int:
        """Write to target. Return rows loaded."""
        ...
    @abstractmethod
    def export_csv(self, df: pd.DataFrame) -> Path:
        """Export transformed DataFrame to CSV. Return CSV file path."""
        ...
    # ── Optional hooks — subclasses MAY override ──────────────────────────────
    def pre_run(self) -> None:
        """Called before first attempt. Override for setup logic."""
        pass

    def post_run(self, result: ETLResult) -> None:
        """Called after run completes (success or failure). Override for cleanup."""
        pass

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
                rows_loaded   = self.load(df_transformed)
                export_csv    = self.export_csv(df_transformed)

                self._result.complete(
                    rows_extracted=len(df_raw),
                    rows_loaded=rows_loaded,
                    export_csv=export_csv,
                    attempts=attempt,
                )
                logger.info(f"Pipeline SUCCESS | {self!r} | "
                            f"rows={rows_loaded} "
                            f"CSV={export_csv} "
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


# ── Q2: CustomerETLPipeline — WRITE THIS YOURSELF ────────────────────────────
class CustomerETLPipeline(BaseETLPipeline):
    """
    Q2 — YOUR TASK:
    Concrete ETL pipeline for customer lifetime value analysis.
    Inherits retry logic and __repr__ from BaseETLPipeline.
    Must implement: extract(), transform(), load()

    HINTS for extract():
      - Use the same SQL from etl_resilient.py (customer + rental + payment join)
      - Return pd.read_sql(sql, self.engine)

    HINTS for transform(df):
      - Add column: value_segment using pd.cut() on total_spend
        bins=[0, 50, 100, 150, float('inf')]
        labels=['Bronze', 'Silver', 'Gold', 'Platinum']
      - Add column: load_date = pd.Timestamp.now().date()
      - Return the enriched df

    HINTS for load(df):
      - Use df.to_sql(self.config.target_table, self.engine,
                      if_exists='replace', index=False, method='multi')
      - Also export CSV to self.config.output_csv  ← uses the @property, no path bugs
      - Return len(df) as the row count

    Self-check:
      - logger.info(f"Loaded {rows} rows into {self.config.target_table}")
      - The __repr__ should show status='success' after run() completes
      - print(pipeline) should show human-readable string
    """
    # YOUR CODE HERE — implement extract(), transform(), load()
    # class ResilientETLPipeline:
    """Production-ready ETL with retry and error handling."""

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

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        bins = [0, 50, 100, 150, float('inf')]
        labels = ['Bronze', 'Silver', 'Gold', 'Platinum']
        df['value_segment'] = pd.cut(df['total_spend'], bins=bins, labels=labels)
        df['load_date'] = pd.Timestamp.now().date()
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
        return path


# ── Runner ────────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("=" * 52)
    logger.info("OOP ETL Demo — Day 14")
    logger.info("=" * 52)

    # Config via dataclass — clean, typed, validated
    config = ETLConfig(
        source_table="customer",
        target_table="analytics_customer_oop",
        max_retries=2,
        output_dir=Path(__file__).parent / "output",
    )
    logger.info(f"Config: {config}")

    # Instantiate — __repr__ fires immediately in the log
    pipeline = CustomerETLPipeline(config=config)
    logger.info(f"Pipeline repr: {pipeline!r}")
    logger.info(f"Pipeline str:  {pipeline}")

    # Run
    result = pipeline.run()

    # Result summary
    logger.info(f"Result:  {result}")
    logger.info(f"Success: {result.success}")
    logger.info(f"Rows:    {result.rows_loaded}")
    logger.info(f"CSV:     {result.export_csv}")
    logger.info(f"Elapsed: {result.elapsed_seconds:.2f}s")

    # Verify Protocol satisfaction
    assert isinstance(pipeline, ETLProtocol), \
        "Pipeline does not satisfy ETLProtocol — check method signatures"
    logger.info("Protocol check: CustomerETLPipeline satisfies ETLProtocol ✅")

    dispose_engine()


if __name__ == "__main__":
    main()