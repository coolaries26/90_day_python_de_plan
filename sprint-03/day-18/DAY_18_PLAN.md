# 📅 DAY 18 — Sprint 03 | pytest Depth
## Fixtures, parametrize, Correct DB Mocking — Fix test_etl.py to 3/3

---

## 🔁 RETROSPECTIVE — Day 17

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| P1 broadcast transform | ✅ Pass | 49.4% above avg — correct |
| P2 rolling + expanding | ✅ Pass | Monotonic cumulative spend |
| P3 rank | ✅ Pass | Eleanor Hunt rank=1, $211.55 |
| P4 monthly grouper | ✅ Pass | 61312.04 total — ground truth match |
| P5 growth + lag | ✅ Pass | All 4 derived columns correct |
| Audit log | ✅ Pass | Both pipelines logged |
| Rental count 46 vs 45 | ℹ️ Note | Add to dq_findings.md, low priority |

### Branch Setup
```bash
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-03/day-18-pytest-depth
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Python Core for DBAs                                  |
| Story           | ST-18: pytest Depth — Fixtures, Parametrize, Mocking         |
| Task ID         | TASK-018                                                     |
| Sprint          | Sprint 03 (Days 15–21)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | pytest, fixtures, mocking, testing, day-18                   |
| Acceptance Criteria | test_etl.py 3/3 passing; conftest.py with shared fixtures; parametrize used; DB never touched in unit tests |

---

## 📚 BACKGROUND

### Why Your Day 13 Tests Failed — The Real Lesson

```
test_successful_etl    FAILED   FileNotFoundError: sprint-02\day-12\output
test_retry_on_failure  FAILED   StopIteration
test_max_retries_exceeded PASSED
```

Both failures had the same root cause: **incomplete mocking**.

You mocked `get_engine` and `pd.read_sql` but left `export_csv` unmocked.
When the mock `pd.read_sql` returned a `MagicMock`, the pipeline proceeded to
`export_csv()` which tried to create a real folder at a relative path — and failed.

The retry test then exhausted `side_effect` because the retry loop kept calling
`export_csv` (which failed) after `read_sql` succeeded, consuming more retries than
you expected.

### The Three Rules of Unit Testing ETL Code

```
RULE 1: Mock everything that touches the outside world
        (DB connections, file system, network, time.sleep)

RULE 2: Test one thing per test function
        (retry logic = one test, CSV export = separate test)

RULE 3: Never let tests share mutable state
        (use fixtures to create fresh objects per test)
```

### pytest Vocabulary

| Concept | What It Does | When to Use |
|---------|-------------|-------------|
| `@pytest.fixture` | Shared setup/teardown | DB config, temp dirs, mock objects used by multiple tests |
| `@pytest.mark.parametrize` | Run same test with multiple inputs | Test multiple ETL configs, multiple retry counts |
| `unittest.mock.patch` | Replace a real object with a fake | DB engine, file system, network calls |
| `MagicMock` | Fake object that accepts any call | Stand-in for engine, DataFrame, JIRA client |
| `pytest.raises` | Assert an exception is raised | Testing error paths |
| `tmp_path` | pytest built-in fixture — temp directory | File output tests — no hardcoded paths |
| `monkeypatch` | Temporarily change env vars / attributes | Test different config values |
| `caplog` | Capture log output in tests | Verify log messages were written |

---

## 🎯 OBJECTIVES

1. Understand why Day 13 tests failed — root cause analysis
2. Build `conftest.py` with shared fixtures
3. Fix `test_etl.py` → 3/3 passing with complete mocking
4. Write new tests using `@pytest.mark.parametrize`
5. Use `tmp_path` fixture for file system tests
6. Use `caplog` to assert log messages
7. Push clean — one commit `[DAY-018][S03]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|---------------------------------------------------|
| A     | 15 min   | Branch setup + root cause analysis write-up       |
| B     | 35 min   | `conftest.py` — shared fixtures                   |
| C     | 40 min   | Fix `test_etl.py` → 3/3 passing                   |
| D     | 10 min   | New parametrize + caplog tests                    |
| E     | 20 min   | Commit + merge                                    |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Root Cause Write-Up (Block A)
**[Required — 5 minutes, write in your notes]**

Create `sprint-03/day-18/root_cause_analysis.md`:

```markdown
# Test Failure Root Cause Analysis — Day 18

## test_successful_etl — FileNotFoundError

### What failed:
export_csv() called Path("sprint-02/day-12/output").mkdir()
Running from sprint-02/day-13/ → relative path resolved incorrectly

### Root causes:
1. export_csv() not mocked — real filesystem called from unit test
2. Hardcoded relative path in etl_resilient.py (fixed Day 14)

### Fix:
- Mock export_csv with @patch.object(ResilientETLPipeline, "export_csv")
- OR use tmp_path fixture and patch output_dir

## test_retry_on_failure — StopIteration

### What failed:
mock_read_sql.side_effect = [Exception("DB error"), MagicMock()]
Attempt 1: read_sql raises Exception ✅
Attempt 2: read_sql returns MagicMock ✅ — but export_csv fails
Attempt 3: retry fires → read_sql has no more items → StopIteration

### Root causes:
1. export_csv not mocked — caused unexpected extra retry
2. side_effect list had only 2 items but 3 attempts were made

### Fix:
- Mock export_csv → only 2 read_sql calls happen
- side_effect = [Exception("DB error"), MagicMock()] is then correct

## Lesson:
In unit tests for ETL pipelines:
ALWAYS mock: get_engine, pd.read_sql, df.to_sql, export_csv, time.sleep
NEVER mock: the retry logic itself (that's what you're testing)
```

---

### EXERCISE 2 — conftest.py: Shared Fixtures (Block B)
**[Fully provided — study the fixture patterns carefully]**

Create `sprint-03/day-18/conftest.py`:

```python
"""
conftest.py — Shared pytest fixtures for Day 18 tests
======================================================
pytest automatically discovers this file and makes all
fixtures defined here available to every test in the folder.

No imports needed in test files — pytest injects fixtures by name.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

# Bootstrap paths
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-12"))
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-14"))

from etl_protocols import ETLConfig


# ── Fixture 1: sample DataFrame ───────────────────────────────────────────────
@pytest.fixture
def sample_df() -> pd.DataFrame:
    """
    A small realistic DataFrame representing customer ETL output.
    Used in any test that needs a DataFrame without hitting the DB.
    Scope: function (fresh copy per test — prevents mutation between tests)
    """
    return pd.DataFrame({
        "customer_id":   [1, 2, 3, 148, 526],
        "full_name":     ["Alice A", "Bob B", "Carol C", "Eleanor Hunt", "Karl Seal"],
        "total_rentals": [10, 8, 15, 45, 42],
        "total_spend":   [45.50, 32.00, 67.25, 211.55, 208.58],
        "segment":       ["Bronze", "Bronze", "Silver", "Platinum", "Platinum"],
    })


# ── Fixture 2: ETLConfig with tmp_path ───────────────────────────────────────
@pytest.fixture
def etl_config(tmp_path: Path) -> ETLConfig:
    """
    ETLConfig pointing to a temporary directory for output.
    tmp_path is a pytest built-in fixture — creates a unique temp dir
    per test and cleans it up automatically after the test.

    This eliminates all hardcoded path issues in tests.
    """
    return ETLConfig(
        source_table="customer",
        target_table="analytics_test_output",
        max_retries=2,
        retry_wait_s=0,     # ← no sleep in tests — speeds up retry tests
        output_dir=tmp_path / "output",
    )


# ── Fixture 3: mock engine ────────────────────────────────────────────────────
@pytest.fixture
def mock_engine() -> MagicMock:
    """
    A MagicMock standing in for the SQLAlchemy engine.
    Prevents any real DB connection in unit tests.
    """
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


# ── Fixture 4: mock pipeline (patches engine + read_sql + to_sql + export) ───
@pytest.fixture
def mock_pipeline(etl_config: ETLConfig, sample_df: pd.DataFrame):
    """
    A ResilientETLPipeline with all external calls mocked.
    Use this when testing pipeline behaviour, not specific methods.

    Yields the pipeline object — patches are active during the test
    and automatically removed after.
    """
    from etl_resilient import ResilientETLPipeline

    with patch("etl_resilient.get_engine") as mock_get_engine, \
         patch("etl_resilient.pd.read_sql", return_value=sample_df), \
         patch.object(ResilientETLPipeline, "export_csv"):
        mock_get_engine.return_value = MagicMock()
        pipeline = ResilientETLPipeline(max_retries=etl_config.max_retries)
        yield pipeline


# ── Fixture 5: caplog at INFO level ──────────────────────────────────────────
@pytest.fixture
def info_log(caplog):
    """
    Capture log output at INFO level during a test.
    Usage in test:
        def test_something(info_log):
            run_something()
            assert "expected message" in info_log.text
    """
    import logging
    with caplog.at_level(logging.INFO):
        yield caplog
```

---

### EXERCISE 3 — Fix test_etl.py → 3/3 Passing (Block C)
**[test_max_retries fully provided. Fix test_successful_etl yourself. Fix test_retry_on_failure yourself — hints given]**

Create `sprint-03/day-18/test_etl_fixed.py`:

```python
"""
test_etl_fixed.py — Day 18 | Fixed ETL Tests
=============================================
Fixes test_successful_etl and test_retry_on_failure from Day 13
using proper fixtures and complete mocking.

Run: pytest test_etl_fixed.py -v
Target: 3/3 passing
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pandas as pd
import pytest

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-12"))

from etl_resilient import ResilientETLPipeline


# ── Test 1: Fully provided — study this pattern ───────────────────────────────
def test_max_retries_exceeded():
    """
    Test 3: After max_retries, pipeline raises exception.
    This test was already passing on Day 13 — kept here as reference.

    Pattern: patch as context managers → explicit control over scope
    """
    with patch("etl_resilient.get_engine"), \
         patch("etl_resilient.pd.read_sql",
               side_effect=Exception("Persistent DB error")), \
         patch.object(ResilientETLPipeline, "export_csv"), \
         patch("etl_resilient.time.sleep"):   # ← no real sleeping in tests

        pipeline = ResilientETLPipeline(max_retries=2)

        with pytest.raises(Exception, match="Persistent DB error"):
            pipeline.run()


# ── Test 2: Fix this yourself ─────────────────────────────────────────────────
def test_successful_etl(sample_df: pd.DataFrame, etl_config):
    """
    Test 1: Happy path — pipeline runs, loads data, exports CSV.

    HINTS:
      - Patch: "etl_resilient.get_engine" (no return value needed)
      - Patch: "etl_resilient.pd.read_sql" return_value=sample_df
      - Patch.object: ResilientETLPipeline, "load"    ← mock the DB write
      - Patch.object: ResilientETLPipeline, "export_csv" ← mock file write

    WHY patch load AND export_csv separately?
      - load() calls df.to_sql() → needs real engine → mock it
      - export_csv() writes to disk → use tmp_path via etl_config fixture
        OR mock it entirely to keep test focused on retry logic

    ASSERTIONS to make:
      - pipeline.run() does not raise
      - mock_read_sql was called exactly once (no retries on success)
      - mock_load was called exactly once
      - result is the sample_df (or check row count)

    HINTS for assertions:
      mock_read_sql.assert_called_once()
      mock_load.assert_called_once()
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement test_successful_etl")


# ── Test 3: Fix this yourself ─────────────────────────────────────────────────
def test_retry_on_failure(sample_df: pd.DataFrame):
    """
    Test 2: Retry logic — fails first attempt, succeeds on second.

    HINTS:
      - Patch "etl_resilient.get_engine"
      - Patch "etl_resilient.pd.read_sql" with side_effect:
            [Exception("DB connection failed"), sample_df]
            ↑ fail first call, succeed second call
      - Patch.object ResilientETLPipeline, "load"
      - Patch.object ResilientETLPipeline, "export_csv"
      - Patch "etl_resilient.time.sleep"  ← prevents 2s wait in test

    ASSERTIONS:
      - pipeline.run() does not raise
      - mock_read_sql.call_count == 2  (called twice — one fail, one success)
      - mock_load.call_count == 1      (load only happens after successful extract)

    IMPORTANT: max_retries=3 so there is room for the retry to succeed.
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement test_retry_on_failure")


# ── Parametrize example — fully provided ─────────────────────────────────────
@pytest.mark.parametrize("max_retries,expected_calls", [
    (1, 1),   # 1 retry allowed → read_sql called once then raises
    (2, 2),   # 2 retries → called twice then raises
    (3, 3),   # 3 retries → called three times then raises
])
def test_retry_count_matches_max_retries(max_retries: int, expected_calls: int):
    """
    Parametrize: verify retry count equals max_retries exactly.
    This single parametrized test replaces 3 separate test functions.
    """
    with patch("etl_resilient.get_engine"), \
         patch("etl_resilient.pd.read_sql",
               side_effect=Exception("always fails")), \
         patch.object(ResilientETLPipeline, "export_csv"), \
         patch("etl_resilient.time.sleep") as mock_sleep:

        pipeline = ResilientETLPipeline(max_retries=max_retries)

        with pytest.raises(Exception):
            pipeline.run()

        assert pd.read_sql.call_count == expected_calls   # type: ignore[attr-defined]
        # time.sleep called (max_retries - 1) times — no sleep after last failure
        assert mock_sleep.call_count == max_retries - 1
```

**Run after implementing both fixes:**
```bash
cd sprint-03/day-18
pytest test_etl_fixed.py -v

# Target:
# test_etl_fixed.py::test_max_retries_exceeded        PASSED
# test_etl_fixed.py::test_successful_etl              PASSED
# test_etl_fixed.py::test_retry_on_failure            PASSED
# test_etl_fixed.py::test_retry_count_matches_max_retries[1-1]  PASSED
# test_etl_fixed.py::test_retry_count_matches_max_retries[2-2]  PASSED
# test_etl_fixed.py::test_retry_count_matches_max_retries[3-3]  PASSED
# 6 passed in X.XXs
```

---

### EXERCISE 4 — caplog Test (Block D)
**[Write yourself — simple, one hint]**

Add this test to `test_etl_fixed.py`:

```python
def test_pipeline_logs_success(sample_df: pd.DataFrame, caplog):
    """
    Verify pipeline logs success message at INFO level.

    HINT:
      - Use same mocking pattern as test_successful_etl
      - import logging; use caplog.at_level(logging.INFO)
      - After pipeline.run(), check:
            assert "SUCCESS" in caplog.text
            OR assert "successfully" in caplog.text.lower()
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement test_pipeline_logs_success")
```

---

### EXERCISE 5 — Git Push (Block E)

```bash
cd C:\Users\Lenovo\python-de-journey

# Run full test suite first — must be 6/6 (or 7/7 with caplog test)
cd sprint-03/day-18
pytest test_etl_fixed.py -v --tb=short
cd ..\..

python scripts/daily_commit.py --day 18 --sprint 3 ^
    --message "pytest depth: conftest fixtures, 3/3 ETL tests passing, parametrize, caplog" ^
    --merge
```

---

## ✅ DAY 18 COMPLETION CHECKLIST

| # | Task                                                                  | Done? |
|---|-----------------------------------------------------------------------|-------|
| 1 | `root_cause_analysis.md` written — both failures explained            | [ ]   |
| 2 | `conftest.py` created — 5 fixtures                                    | [ ]   |
| 3 | `etl_config` fixture uses `tmp_path` — no hardcoded paths             | [ ]   |
| 4 | `mock_pipeline` fixture patches all 3 external calls                  | [ ]   |
| 5 | `test_max_retries_exceeded` — passes (reference)                      | [ ]   |
| 6 | **`test_successful_etl` — written by you, passes**                    | [ ]   |
| 7 | **`test_retry_on_failure` — written by you, passes**                  | [ ]   |
| 8 | `test_retry_count_matches_max_retries` — 3 parametrize cases pass     | [ ]   |
| 9 | **`test_pipeline_logs_success` — written by you, passes**             | [ ]   |
|10 | `pytest test_etl_fixed.py -v` → 6/6 or 7/7 passing, 0 failing        | [ ]   |
|11 | `time.sleep` mocked in all retry tests — no real delays               | [ ]   |
|12 | One clean `[DAY-018][S03]` commit via `daily_commit.py --merge`       | [ ]   |

---

## 🔍 SELF-CHECK — test_successful_etl is correct when:

```
test_etl_fixed.py::test_successful_etl PASSED
```
And inside the test:
- `mock_read_sql.assert_called_once()` passes — no unnecessary retries
- `mock_load.assert_called_once()` passes — data was loaded exactly once

---

## ⚠️ WATCH OUT FOR

**`patch` decorator order vs argument order:**
```python
# When stacking @patch decorators, arguments are in REVERSE order:
@patch("module.C")   # → arg mock_c (innermost = first arg)
@patch("module.B")   # → arg mock_b
@patch("module.A")   # → arg mock_a (outermost = last arg)
def test(mock_a, mock_b, mock_c): ...
# ↑ confusing — use context managers (with patch(...)) instead
```

**`pd.read_sql` global patch vs method patch:**
```python
# Patches the function in the module where it's USED, not where it's defined:
patch("etl_resilient.pd.read_sql")   # ✅ correct
patch("pandas.read_sql")             # ❌ won't intercept calls in etl_resilient
```

**`retry_wait_s=0` in ETLConfig for tests:**
Without this, `time.sleep(2)` runs for real in retry tests — 6 seconds of waiting.
Always set `retry_wait_s=0` in test configs OR mock `time.sleep`.

---

## 🔜 PREVIEW: DAY 19

**Topic:** Fix original `test_etl.py` from Day 13 in-place + pytest coverage report  
**What you'll do:** Apply today's patterns to the original file, run `pytest --cov`
to generate a coverage report, and reach ≥80% coverage on `etl_resilient.py`.

---

*Day 18 | Sprint 03 | EP-02 | TASK-018*