# 📅 DAY 15 — Gap Fill Sprint | Type Hints + mypy
## Annotate Your ETL Code — mypy --strict Passes by End of Day

---

## 🔁 RETROSPECTIVE — Day 14

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| ETLConfig dataclass | ✅ Pass | Validation, from_env, @property all working |
| ETLResult lifecycle | ✅ Pass | pending → success, elapsed_seconds correct |
| BaseETLPipeline | ✅ Pass | Template method, __repr__, retry loop clean |
| CustomerETLPipeline | ✅ Pass | 599 rows, CSV at correct absolute path |
| Protocol check | ✅ Pass | isinstance() returns True |
| PipelineRegistry demo | ⚠️ Minor | Add .register() calls to demo before pushing |

**Fix registry demo, then proceed.**

### Branch Setup
```bash
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-02/day-15-type-hints
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Python Core for DBAs                                  |
| Story           | ST-15: Type Hints + mypy Static Analysis                     |
| Task ID         | TASK-015                                                     |
| Sprint          | Gap Fill (Days 14–20)                                        |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | type-hints, mypy, static-analysis, etl, day-15               |
| Acceptance Criteria | mypy passes on oop_etl.py + etl_protocols.py; all key type constructs used; py.typed marker added |

---

## 📚 BACKGROUND

### Why Type Hints Matter for a Data Engineer

```python
# WITHOUT type hints — you find this bug at 3am when the pipeline crashes:
def load(self, df):
    df.to_sql(self.table, self.engine)   # what is df? what is table?

# WITH type hints — you find this bug in your IDE before you even run it:
def load(self, df: pd.DataFrame) -> int:
    df.to_sql(self.table, self.engine)
    return len(df)
# mypy: error: Argument "con" has incompatible type — caught before runtime
```

Type hints are not documentation. They are **machine-checkable contracts**.
`mypy` is the tool that enforces them — it reads your code without running it
and finds type mismatches, missing returns, and wrong argument types.

### Type Hint Vocabulary — DBA Analogy

| Python Type Hint         | SQL Equivalent          | Example                          |
|--------------------------|-------------------------|----------------------------------|
| `int`                    | INTEGER NOT NULL        | `rows: int`                      |
| `str`                    | VARCHAR NOT NULL        | `table: str`                     |
| `str \| None`            | VARCHAR NULL            | `error: str \| None`             |
| `Optional[str]`          | VARCHAR NULL            | `Optional[str]` = `str \| None`  |
| `list[str]`              | ARRAY of VARCHAR        | `tables: list[str]`              |
| `dict[str, int]`         | Key-value map           | `counts: dict[str, int]`         |
| `pd.DataFrame`           | Result set              | `df: pd.DataFrame`               |
| `type[T]`                | Schema/DDL              | `cls: type[BaseETLPipeline]`     |
| `TypeVar`                | Generic column type     | `T = TypeVar('T')`               |
| `Callable[[int], str]`   | Function as parameter   | `transform: Callable[[pd.DataFrame], pd.DataFrame]` |

### mypy Strictness Levels

```bash
mypy file.py                    # basic — catches obvious errors
mypy --strict file.py           # strict — requires ALL annotations
mypy --ignore-missing-imports   # skip untyped third-party libraries
```

Today's target: `mypy --strict --ignore-missing-imports` passes on your Day 14 files.

---

## 🎯 OBJECTIVES

1. Install mypy + pandas-stubs
2. Run mypy baseline on existing files — understand the error report
3. Fix all mypy errors in `etl_protocols.py`
4. Fix all mypy errors in `oop_etl.py`
5. Add a `mypy.ini` config file to the project
6. Write `typed_utils.py` — demonstrates TypeVar, Generic, Callable, Literal
7. Push clean

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                        |
|-------|----------|-------------------------------------------------|
| A     | 15 min   | Install mypy + baseline run on Day 14 files     |
| B     | 35 min   | Fix etl_protocols.py + oop_etl.py mypy errors   |
| C     | 35 min   | `typed_utils.py` — advanced type constructs     |
| D     | 15 min   | mypy.ini config + final clean run               |
| E     | 20 min   | Commit + merge                                  |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Install mypy + Baseline Run (Block A)
**[Full steps]**

```bash
.venv\Scripts\activate
pip install mypy==1.7.1 pandas-stubs==2.1.4.231227

# Add to requirements.txt
echo mypy==1.7.1 >> requirements.txt
echo pandas-stubs==2.1.4.231227 >> requirements.txt
```

**Run baseline — see what needs fixing:**
```bash
cd C:\Users\Lenovo\python-de-journey

# Run on Day 14 files — expect errors, that's the point
mypy sprint-02/day-14/etl_protocols.py --ignore-missing-imports
mypy sprint-02/day-14/oop_etl.py --ignore-missing-imports

# Run strict — more errors, that's also the point
mypy sprint-02/day-14/etl_protocols.py --strict --ignore-missing-imports
```

**Document every error line in `sprint-02/day-15/mypy_baseline.md`:**
```markdown
# mypy Baseline Report — Day 15

## etl_protocols.py
Line X: error: ...
Line Y: error: ...

## oop_etl.py
Line X: error: ...
```

This is your starting point. Fix each one systematically.

---

### EXERCISE 2 — Fix etl_protocols.py + oop_etl.py (Block B)
**[Most common fixes provided — you apply them]**

**Common mypy errors you will encounter and how to fix them:**

**Error 1: `Function is missing a return type annotation`**
```python
# Before:
def pre_run(self):
    pass

# After:
def pre_run(self) -> None:
    pass
```

**Error 2: `Function is missing a type annotation for one or more arguments`**
```python
# Before:
def __init__(self, config):

# After:
def __init__(self, config: ETLConfig) -> None:
```

**Error 3: `"ETLResult" has no attribute "elapsed_seconds"` (from @property)**
```python
# mypy needs the return type on @property:
# Before:
@property
def elapsed_seconds(self):

# After:
@property
def elapsed_seconds(self) -> float | None:
```

**Error 4: `Incompatible return value type (got "None", expected "DataFrame")`**
```python
# abstract method needs return type:
@abstractmethod
def extract(self) -> pd.DataFrame: ...

@abstractmethod
def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...

@abstractmethod
def load(self, df: pd.DataFrame) -> int: ...
```

**Error 5: `Need type annotation for variable` (in __init__)**
```python
# Before:
self._registry = {}

# After:
self._registry: dict[str, type] = {}
```

**Error 6: `Dict entry has incompatible type` in dataclass fields**
```python
# field(default_factory=...) needs explicit type on the field:
# Before:
created_at = field(default_factory=datetime.now)

# After:
created_at: datetime = field(default_factory=datetime.now, repr=False)
```

**Run mypy after each fix — aim for zero errors:**
```bash
mypy sprint-02/day-14/etl_protocols.py --strict --ignore-missing-imports
# Target: Success: no issues found in 1 source file
```

---

### EXERCISE 3 — typed_utils.py: Advanced Type Constructs (Block C)
**[Full steps — these appear in production DE code constantly]**

Create `sprint-02/day-15/typed_utils.py`:

```python
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
```

**✅ Checkpoint:**
```bash
python sprint-02/day-15/typed_utils.py    # runs clean
mypy sprint-02/day-15/typed_utils.py --strict --ignore-missing-imports
# Target: Success: no issues found in 1 source file
```

---

### EXERCISE 4 — mypy.ini Configuration (Block D)
**[Full steps — this config file applies to the whole project]**

Create `mypy.ini` at project root:

```ini
# mypy.ini — project-wide mypy configuration
# Applies when running: mypy <file> from project root

[mypy]
python_version         = 3.12
strict                 = True
ignore_missing_imports = True
warn_return_any        = True
warn_unused_configs    = True
show_error_codes       = True
pretty                 = True

# Third-party libraries without stubs — suppress errors for these
[mypy-psycopg2.*]
ignore_missing_imports = True

[mypy-loguru.*]
ignore_missing_imports = True

[mypy-git.*]
ignore_missing_imports = True

[mypy-sqlalchemy.*]
ignore_missing_imports = True
```

**Final mypy run — target zero errors on all three files:**
```bash
cd C:\Users\Lenovo\python-de-journey

mypy sprint-02/day-14/etl_protocols.py
mypy sprint-02/day-14/oop_etl.py
mypy sprint-02/day-15/typed_utils.py

# All three should show:
# Success: no issues found in 1 source file
```

---

### EXERCISE 5 — Git Push (Block E)

```bash
cd C:\Users\Lenovo\python-de-journey
git status   # review before committing

python scripts/daily_commit.py --day 15 --sprint 2 ^
    --message "Type hints + mypy: etl_protocols and oop_etl fully annotated, typed_utils.py, mypy.ini added" ^
    --merge
```

---

## ✅ DAY 15 COMPLETION CHECKLIST

| # | Task                                                                  | Done? |
|---|-----------------------------------------------------------------------|-------|
| 1 | mypy + pandas-stubs installed                                         | [ ]   |
| 2 | `mypy_baseline.md` documents starting errors                          | [ ]   |
| 3 | `etl_protocols.py` — `mypy --strict` passes                          | [ ]   |
| 4 | `oop_etl.py` — `mypy --strict` passes                                | [ ]   |
| 5 | `typed_utils.py` created — all 7 constructs demonstrated              | [ ]   |
| 6 | `typed_utils.py` — `mypy --strict` passes                            | [ ]   |
| 7 | `mypy.ini` created at project root                                    | [ ]   |
| 8 | `apply_transform` uses `Callable[[pd.DataFrame], pd.DataFrame]`       | [ ]   |
| 9 | `PipelineResult[T]` generic class `.map()` returns `PipelineResult[U]`| [ ]   |
|10 | `ETLSummaryRow` TypedDict matches audit log schema                    | [ ]   |
|11 | One clean commit via `daily_commit.py --merge`                        | [ ]   |

---

## 🔍 SELF-CHECK — mypy is fully passing when:

```bash
mypy sprint-02/day-14/etl_protocols.py sprint-02/day-14/oop_etl.py sprint-02/day-15/typed_utils.py
# Output:
# Success: no issues found in 3 source files
```

---

## ⚠️ WATCH OUT FOR

**`pd.DataFrame` in strict mode:**
pandas-stubs covers most of the API but some methods return `Any`.
If mypy complains about a pandas method, add `# type: ignore[return-value]`
on that specific line only — never silence the whole file.

**`datetime` fields in dataclass:**
```python
# This causes mypy to complain about incompatible default:
started_at: datetime = datetime.now()   # ← evaluated once at class definition

# Correct pattern:
started_at: datetime = field(default_factory=datetime.now)
```

**`str | None` vs `Optional[str]`:**
Both are identical in Python 3.10+. Use `str | None` — it's cleaner.
`Optional[str]` is legacy syntax, still valid but avoid in new code.

---

## 🔜 PREVIEW: DAY 16

**Topic:** SQLAlchemy ORM + Alembic migrations  
**What you'll do:** Define ORM models for `film`, `customer`, `rental` as Python classes.
Run Alembic to create a migration that adds an `analytics_audit_log` table.
Use the ORM models in a query instead of raw SQL strings.
This is the last gap before the test sprint on Day 20.

---

*Day 15 | Gap Fill Sprint | EP-02 | TASK-015*
