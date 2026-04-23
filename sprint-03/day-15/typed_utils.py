#!/usr/bin/env python3
"""
typed_utils.py — Day 15 | Advanced Type Hints
==============================================
Demonstrates:
  TypeVar     — generic functions that work on any type
  Generic     — generic classes (typed containers)
  Callable    — functions as parameters
  Literal     — restrict values to specific strings/ints
  TypedDict   — typed dictionaries (like a DB row schema)
  overload    — multiple signatures for one function
  Final       — constants that cannot be reassigned

Run: python typed_utils.py
Then: mypy typed_utils.py --strict --ignore-missing-imports
"""

from __future__ import annotations

from typing import (
    Callable, Final, Generic, Literal,
    TypedDict, TypeVar, overload,
)
from dataclasses import dataclass
import pandas as pd


# ── TypeVar: generic functions ────────────────────────────────────────────────
T = TypeVar("T")
U = TypeVar("U")


def first(items: list[T]) -> T | None:
    """
    Return first item of any list — works for list[int], list[str], list[DataFrame].
    TypeVar T is inferred from the argument — no casting needed.
    """
    return items[0] if items else None


def apply_transform(
    df: pd.DataFrame,
    transform_fn: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """
    Apply any transformation function to a DataFrame.
    Callable[[pd.DataFrame], pd.DataFrame] = a function that takes and returns DataFrame.
    This is how pipeline stages are passed as arguments.
    """
    return transform_fn(df)


# ── Literal: restrict to specific values ──────────────────────────────────────
ETLStatus   = Literal["pending", "running", "success", "failed"]
LoadMode    = Literal["replace", "append", "upsert"]
LogLevel    = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def set_pipeline_status(status: ETLStatus) -> str:
    """
    mypy will reject: set_pipeline_status("unknown")
    Only the four Literal values are valid — caught at type-check time.
    """
    return f"Status set to: {status}"


def get_load_mode(incremental: bool) -> LoadMode:
    """Return mode based on flag — return type is constrained to LoadMode literals."""
    return "append" if incremental else "replace"


# ── TypedDict: typed dictionary — like a DB row schema ───────────────────────
class FilmRow(TypedDict):
    """
    TypedDict defines the expected shape of a dict — like a schema contract.
    Equivalent to: CREATE TABLE film (film_id INTEGER, title VARCHAR, ...)
    mypy will catch if you access film['titl'] (typo) or assign wrong type.
    """
    film_id:      int
    title:        str
    rental_rate:  float
    rating:       str


class ETLSummaryRow(TypedDict):
    """Summary row written to audit log table after each pipeline run."""
    pipeline_name:  str
    source_table:   str
    target_table:   str
    rows_loaded:    int
    status:         ETLStatus
    elapsed_s:      float


def make_summary(
    name: str,
    source: str,
    target: str,
    rows: int,
    status: ETLStatus,
    elapsed: float,
) -> ETLSummaryRow:
    """Build a typed summary row — mypy verifies all fields present and typed."""
    return ETLSummaryRow(
        pipeline_name=name,
        source_table=source,
        target_table=target,
        rows_loaded=rows,
        status=status,
        elapsed_s=elapsed,
    )


# ── Generic class: typed container ────────────────────────────────────────────
class PipelineResult(Generic[T]):
    """
    Generic result wrapper — PipelineResult[pd.DataFrame] or PipelineResult[int].
    Like a typed envelope: the type of the value inside is part of the type itself.
    """

    def __init__(self, value: T, status: ETLStatus, message: str = "") -> None:
        self._value:   T         = value
        self.status:   ETLStatus = status
        self.message:  str       = message

    @property
    def value(self) -> T:
        return self._value

    @property
    def ok(self) -> bool:
        return self.status == "success"

    def map(self, fn: Callable[[T], U]) -> PipelineResult[U]:
        """
        Transform the inner value — returns a new PipelineResult with new type.
        Pattern from functional programming — useful in pipeline chaining.
        """
        return PipelineResult(fn(self._value), self.status, self.message)

    def __repr__(self) -> str:
        return f"PipelineResult(status={self.status!r}, ok={self.ok})"


# ── Final: constants ──────────────────────────────────────────────────────────
MAX_POOL_SIZE:    Final[int] = 10
DEFAULT_SCHEMA:   Final[str] = "public"
AUDIT_TABLE:      Final[str] = "etl_audit_log"


# ── overload: multiple signatures for one function ───────────────────────────
@overload
def coerce_id(value: str) -> int: ...
@overload
def coerce_id(value: int) -> int: ...

def coerce_id(value: str | int) -> int:
    """
    Accept str or int, always return int.
    @overload lets callers know exactly what they get back based on input type.
    Without overload, return type would just be int — less precise.
    """
    return int(value)


# ── Demo ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print("\n── TypeVar demo ─────────────────────────────────")
    print(first([10, 20, 30]))          # int
    print(first(["a", "b", "c"]))       # str
    print(first([]))                     # None

    print("\n── Callable demo ────────────────────────────────")
    df = pd.DataFrame({"amount": [1.5, 2.5, 3.0]})
    result = apply_transform(df, lambda d: d.assign(doubled=d["amount"] * 2))
    print(result)

    print("\n── Literal demo ─────────────────────────────────")
    print(set_pipeline_status("success"))
    print(get_load_mode(incremental=True))

    print("\n── TypedDict demo ───────────────────────────────")
    summary = make_summary("CustomerETL", "customer", "analytics_customer",
                           599, "success", 0.71)
    print(summary)

    print("\n── Generic PipelineResult demo ──────────────────")
    r: PipelineResult[pd.DataFrame] = PipelineResult(df, "success", "loaded")
    print(r)
    print(f"  ok: {r.ok}")
    r2 = r.map(lambda d: len(d))   # PipelineResult[int]
    print(f"  mapped to row count: {r2.value}")

    print("\n── Final constants ──────────────────────────────")
    print(f"  MAX_POOL_SIZE:  {MAX_POOL_SIZE}")
    print(f"  DEFAULT_SCHEMA: {DEFAULT_SCHEMA}")
    print(f"  AUDIT_TABLE:    {AUDIT_TABLE}")

    print("\n── overload demo ────────────────────────────────")
    print(coerce_id("42"))
    print(coerce_id(7))


if __name__ == "__main__":
    main()