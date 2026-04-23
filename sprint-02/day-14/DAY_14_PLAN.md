# 📅 DAY 14 — Gap Fill Sprint | OOP Deep-Dive
## Dataclasses, Protocols, __repr__, Applied to Your ETL Code

---

## 🔁 RETROSPECTIVE — Days 08–13 Assessment

### What Was Diagnosed
| File | Issue | Fix Today |
|------|-------|-----------|
| `etl_resilient.py` | Hardcoded relative path in `export_csv()` | Fix with `Path(__file__).parent` |
| `etl_resilient.py` | `run()` missing return type annotation | Add `-> pd.DataFrame` |
| `test_etl.py` | `export_csv` not mocked — bleeds into retry tests | Patch it in tests |
| `test_etl.py` | No fixtures — setup/teardown duplicated | Refactor with `@pytest.fixture` |
| All ETL files | No `__repr__`, no dataclasses for config | Add today |

### Branch Setup
```bash
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-02/day-14-oop-deepdive
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Python Core for DBAs                                  |
| Story           | ST-14: OOP Patterns Applied to ETL Code                      |
| Task ID         | TASK-014                                                     |
| Sprint          | Gap Fill (Days 14–20)                                        |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | oop, dataclasses, protocols, type-hints, etl, day-14         |
| Acceptance Criteria | ETL config as dataclass; Pipeline has __repr__; Protocols defined; path bug fixed; run() typed; pushed to git |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                        |
|---------------|----------------------------------------------|
| Branch        | `sprint-02/day-14-oop-deepdive`              |
| Base Branch   | `develop`                                    |
| Commit Prefix | `[DAY-014]`                                  |
| Folder        | `sprint-02/day-14/`                          |
| Files         | `oop_etl.py`, `etl_protocols.py`, updated `etl_resilient.py` |

---

## 📚 BACKGROUND

Your `ResilientETLPipeline` from Day 12 is a class — but it uses OOP as a container,
not as a design tool. Real OOP means:

```
1. Dataclasses    → replace config dicts/settings with typed, validated objects
2. __repr__       → every class tells you its state when printed/logged
3. Protocols      → define contracts (interfaces) so pipelines are swappable
4. Inheritance    → share retry/logging logic across pipeline types
5. @property      → computed attributes that look like fields
```

### Why This Matters for Data Engineering

```python
# Without OOP — what you have now:
pipeline = ResilientETLPipeline(max_retries=3)
print(pipeline)
# → <etl_resilient.ResilientETLPipeline object at 0x000001A0BD7518D0>
# Useless in logs. You can't tell what pipeline this is.

# With OOP — what you'll have after today:
pipeline = ResilientETLPipeline(config=ETLConfig(source="film", target="analytics_film"))
print(pipeline)
# → ResilientETLPipeline(source='film', target='analytics_film', max_retries=3, status='ready')
# Immediately useful. Shows up clean in every log line.
```

### The Four OOP Concepts in One Picture

```
ETLProtocol (Protocol)          ← defines the contract: extract/transform/load
       ↑ implements
BaseETLPipeline                  ← shared: retry logic, logging, __repr__
       ↑ inherits
ResilientETLPipeline             ← specific: customer lifetime ETL
       │ uses
ETLConfig (dataclass)            ← configuration object with validation
```

---

## 🎯 OBJECTIVES

1. Convert `etl_config.py` settings to a proper `@dataclass` with validation
2. Define an `ETLProtocol` using `typing.Protocol`
3. Refactor `ResilientETLPipeline` with `__repr__`, `__str__`, `@property`
4. Add `BaseETLPipeline` with shared retry logic
5. Fix the hardcoded path bug in `export_csv()`
6. Add `run() -> pd.DataFrame` return type
7. Push clean — one commit

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|---------------------------------------------------|
| A     | 15 min   | Branch setup + path bug fix in etl_resilient.py   |
| B     | 35 min   | `etl_protocols.py` — Protocol + dataclass         |
| C     | 40 min   | Refactor `oop_etl.py` — full OOP pipeline         |
| D     | 10 min   | Verify __repr__ output in logs                    |
| E     | 20 min   | Commit + merge                                    |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Fix etl_resilient.py Path Bug (Block A)
**[Full steps — simple fix, do it first]**

Open `sprint-02/day-12/etl_resilient.py` and fix `export_csv()`:

```python
# FIND:
def export_csv(self, df: pd.DataFrame):
    Path("sprint-02/day-12/output").mkdir(exist_ok=True)
    path = f"sprint-02/day-12/output/{settings.OUTPUT_CSV}"

# REPLACE WITH:
def export_csv(self, df: pd.DataFrame) -> None:
    # Always anchor paths to the file's own location — never relative strings
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)   # parents=True handles missing parents
    path = output_dir / settings.OUTPUT_CSV
    df.to_csv(path, index=False)
    logger.info(f"📄 Exported CSV -> {path}")
```

Also fix `run()` return type:
```python
# FIND:
def run(self):

# REPLACE:
def run(self) -> pd.DataFrame:
```

**Verify fix:**
```bash
cd C:\Users\Lenovo\python-de-journey
python sprint-02/day-12/etl_resilient.py
# Should run without FileNotFoundError
# output/ folder should appear inside sprint-02/day-12/
```

---

### EXERCISE 2 — etl_protocols.py: Dataclass + Protocol (Block B)
**[Full steps — new concepts, fully explained]**

**Background:** A `Protocol` in Python is like a Java Interface — it defines what methods
a class MUST have, without forcing inheritance. Any class that implements those methods
automatically satisfies the Protocol. This enables you to swap pipeline implementations
without changing the code that calls them.

A `@dataclass` is a class where Python auto-generates `__init__`, `__repr__`, and `__eq__`
from the field annotations. Perfect for configuration objects.

Create `sprint-02/day-14/etl_protocols.py`:

```python
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

    def complete(self, rows_extracted: int, rows_loaded: int,
                 attempts: int) -> None:
        """Mark run as successful."""
        self.rows_extracted = rows_extracted
        self.rows_loaded    = rows_loaded
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
```

**✅ Checkpoint:**
```bash
python sprint-02/day-14/etl_protocols.py
# Should print ETLConfig, ETLResult, and registry demos with no errors
# ETLConfig repr should show all fields cleanly
```

---

### EXERCISE 3 — oop_etl.py: Refactored Pipeline with Full OOP (Block C)
**[Q1 BaseETLPipeline fully provided. Q2 CustomerETLPipeline — write yourself with hints]**

Create `sprint-02/day-14/oop_etl.py`:

```python
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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger
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

                self._result.complete(
                    rows_extracted=len(df_raw),
                    rows_loaded=rows_loaded,
                    attempts=attempt,
                )
                logger.info(f"Pipeline SUCCESS | {self!r} | "
                            f"rows={rows_loaded} elapsed={self._result.elapsed_seconds:.2f}s")
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
    raise NotImplementedError("Implement CustomerETLPipeline methods")


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
    logger.info(f"Elapsed: {result.elapsed_seconds:.2f}s")

    # Verify Protocol satisfaction
    assert isinstance(pipeline, ETLProtocol), \
        "Pipeline does not satisfy ETLProtocol — check method signatures"
    logger.info("Protocol check: CustomerETLPipeline satisfies ETLProtocol ✅")

    dispose_engine()


if __name__ == "__main__":
    main()
```

**✅ Self-check — CustomerETLPipeline is correct when:**
```
Pipeline initialised | CustomerETLPipeline(source='customer', target='analytics_customer_oop', max_retries=2, status='pending')
Pipeline START       | CustomerETLPipeline(..., status='pending')
Attempt 1/2
Loaded 599 rows into analytics_customer_oop
Pipeline SUCCESS     | CustomerETLPipeline(..., status='success') | rows=599 elapsed=X.XXs
Protocol check: CustomerETLPipeline satisfies ETLProtocol ✅
```

---

### EXERCISE 4 — Git Push (Block E)

```bash
cd C:\Users\Lenovo\python-de-journey

# Check status first — only commit what's needed
git status

python scripts/daily_commit.py --day 14 --sprint 2 ^
    --message "OOP deep-dive: ETLConfig dataclass, ETLProtocol, BaseETLPipeline, CustomerETLPipeline, path bug fixed" ^
    --merge
```

---

## ✅ DAY 14 COMPLETION CHECKLIST

| # | Task                                                               | Done? |
|---|--------------------------------------------------------------------|-------|
| 1 | `etl_resilient.py` path bug fixed — `Path(__file__).parent`       | [ ]   |
| 2 | `run() -> pd.DataFrame` return type added                          | [ ]   |
| 3 | `etl_protocols.py` created — `ETLConfig`, `ETLResult`, `ETLProtocol` | [ ]   |
| 4 | `ETLConfig.__post_init__` validates max_retries and batch_size     | [ ]   |
| 5 | `ETLConfig.from_env()` classmethod works                           | [ ]   |
| 6 | `ETLResult.complete()` and `.fail()` update status correctly       | [ ]   |
| 7 | `ETLResult.elapsed_seconds` @property returns float               | [ ]   |
| 8 | `BaseETLPipeline.__repr__` shows class, source, target, status     | [ ]   |
| 9 | **`CustomerETLPipeline` written by you — 3 methods implemented**   | [ ]   |
|10 | `isinstance(pipeline, ETLProtocol)` returns True                   | [ ]   |
|11 | `python etl_protocols.py` demo runs clean                          | [ ]   |
|12 | `python oop_etl.py` shows `status='success'` in repr              | [ ]   |
|13 | One clean commit via `daily_commit.py --merge`                     | [ ]   |

---

## 🔜 PREVIEW: DAY 15

**Topic:** Type hints + mypy — annotate your existing ETL files  
**What you'll do:** Run mypy on `etl_resilient.py` and `oop_etl.py`, fix all type errors,
add missing annotations, understand `Optional`, `Union`, `TypeVar`, and `Generic`.
By end of day, `mypy --strict` passes on both files.

---

*Day 14 | Gap Fill Sprint | EP-02 | TASK-014*
