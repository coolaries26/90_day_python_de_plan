#!/usr/bin/env python3
"""
etl_protocols.py — OOP Contracts for ETL Pipelines
====================================================
Defines:
  ETLConfig     — dataclass for pipeline configuration
  ETLResult     — dataclass for pipeline run results
  ETLProtocol   — Protocol (interface) all pipelines must implement
  PipelineRegistry — registry pattern for pipeline lookup by name
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Protocol, runtime_checkable
import pandas as pd


# ── ETLConfig: typed configuration object ─────────────────────────────────────
@dataclass
class ETLConfig:
    """
    Configuration for an ETL pipeline run.
    Using @dataclass means __init__, __repr__, __eq__ are auto-generated.

    Example:
        cfg = ETLConfig(source_table="film", target_table="analytics_film")
        print(cfg)
        # ETLConfig(source_table='film', target_table='analytics_film',
        #           max_retries=3, batch_size=500, ...)
    """
    source_table:  str
    target_table:  str
    max_retries:   int   = 3
    batch_size:    int   = 500
    retry_wait_s:  int   = 2
    output_dir:    Path  = field(default_factory=lambda: Path(__file__).parent / "output")
    created_at:    datetime = field(default_factory=datetime.now, repr=False)

    def __post_init__(self) -> None:
        """Validate after __init__ — called automatically by @dataclass."""
        if self.max_retries < 1:
            raise ValueError(f"max_retries must be >= 1, got {self.max_retries}")
        if self.batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {self.batch_size}")
        # Ensure output_dir is always a Path object (user might pass a string)
        self.output_dir = Path(self.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @property
    def output_csv(self) -> Path:
        """Computed property — CSV path derived from target_table name."""
        return self.output_dir / f"{self.target_table}.csv"

    @classmethod
    def from_env(cls, source: str, target: str) -> ETLConfig:
        """
        Alternative constructor — build config from environment variables.
        Demonstrates @classmethod pattern for multiple construction paths.
        """
        return cls(
            source_table=source,
            target_table=target,
            max_retries=int(os.getenv("ETL_MAX_RETRIES", 3)),
            batch_size=int(os.getenv("ETL_BATCH_SIZE", 500)),
            retry_wait_s=int(os.getenv("ETL_RETRY_WAIT", 2)),
        )


# ── ETLResult: typed result object ────────────────────────────────────────────
@dataclass
class ETLResult:
    """
    Captures the outcome of a pipeline run.
    Returned by every pipeline's run() method.
    """
    pipeline_name:  str
    source_table:   str
    target_table:   str
    rows_extracted: int = 0
    rows_loaded:    int = 0
    attempts_used:  int = 0
    status:         str = "pending"      # pending | success | failed
    error_message:  str | None = None
    started_at:     datetime = field(default_factory=datetime.now, repr=False)
    finished_at:    datetime | None = None

    def complete(self, rows_extracted: int, rows_loaded: int, export_csv: Path,
                 attempts: int) -> None:
        """Mark run as successful."""
        self.rows_extracted = rows_extracted
        self.rows_loaded    = rows_loaded
        self.export_csv      = f"{export_csv}"  # store as string for easier serialization  
        self.attempts_used  = attempts
        self.status         = "success"
        self.finished_at    = datetime.now()

    def fail(self, error: str, attempts: int) -> None:
        """Mark run as failed."""
        self.status        = "failed"
        self.error_message = error
        self.attempts_used = attempts
        self.finished_at   = datetime.now()

    @property
    def elapsed_seconds(self) -> float | None:
        """How long the pipeline ran."""
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @property
    def success(self) -> bool:
        return self.status == "success"


# ── ETLProtocol: the contract all pipelines must satisfy ──────────────────────
@runtime_checkable   # enables isinstance(obj, ETLProtocol) checks
class ETLProtocol(Protocol):
    """
    Protocol (interface) that every ETL pipeline must implement.

    Any class with these three methods automatically satisfies ETLProtocol
    — no inheritance required. This is Python's 'structural subtyping'.

    Usage:
        def run_any_pipeline(pipeline: ETLProtocol) -> ETLResult:
            df = pipeline.extract()
            df = pipeline.transform(df)
            pipeline.load(df)
    """

    def extract(self) -> pd.DataFrame:
        """Pull data from source. Must return a DataFrame."""
        ...

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business logic. Must return transformed DataFrame."""
        ...

    def load(self, df: pd.DataFrame) -> int:
        """Write to target. Must return row count loaded."""
        ...


# ── PipelineRegistry: look up pipeline class by name ─────────────────────────
class PipelineRegistry:
    """
    Registry pattern — maps string names to pipeline classes.
    Used by CLI tools (etl_cli.py) to instantiate pipelines by name.

    Usage:
        registry = PipelineRegistry()
        registry.register("customer", CustomerETLPipeline)
        PipelineClass = registry.get("customer")
        pipeline = PipelineClass(config)
    """

    def __init__(self) -> None:
        self._registry: dict[str, type] = {}

    def register(self, name: str, pipeline_class: type) -> None:
        if not isinstance(pipeline_class, type):
            raise TypeError(f"{pipeline_class} is not a class")
        self._registry[name] = pipeline_class
        print(f"  Registered pipeline: '{name}' -> {pipeline_class.__name__}")

    def get(self, name: str) -> type:
        if name not in self._registry:
            available = list(self._registry.keys())
            raise KeyError(
                f"Pipeline '{name}' not found. Available: {available}"
            )
        return self._registry[name]

    def list_pipelines(self) -> list[str]:
        return list(self._registry.keys())

    def __repr__(self) -> str:
        return f"PipelineRegistry(pipelines={self.list_pipelines()})"


# ── Demonstration ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n── ETLConfig demo ───────────────────────────────")
    cfg = ETLConfig(source_table="film", target_table="analytics_film")
    print(cfg)
    print(f"  output_csv: {cfg.output_csv}")
    print(f"  max_retries: {cfg.max_retries}")

    print("\n── ETLConfig.from_env demo ──────────────────────")
    cfg2 = ETLConfig.from_env("customer", "analytics_customer")
    print(cfg2)

    print("\n── ETLConfig validation demo ────────────────────")
    try:
        bad_cfg = ETLConfig(source_table="film", target_table="out", max_retries=0)
    except ValueError as e:
        print(f"  Validation caught: {e}")

    print("\n── ETLResult demo ───────────────────────────────")
    result = ETLResult(
        pipeline_name="CustomerETL",
        source_table="customer",
        target_table="analytics_customer",
    )
    result.complete(rows_extracted=599, rows_loaded=599, attempts=1)
    print(result)
    print(f"  Elapsed: {result.elapsed_seconds:.3f}s")
    print(f"  Success: {result.success}")

    print("\n── PipelineRegistry demo ────────────────────────")
    registry = PipelineRegistry()
    print(registry)