# 📅 DAY 19 — Sprint 03 | Coverage Report + Fix Original test_etl.py
## pytest --cov, ≥80% Coverage on etl_resilient.py, Clean Test Suite

---

## 🔁 RETROSPECTIVE — Day 18

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| conftest.py fixtures | ✅ Pass | 5 fixtures, tmp_path, mock_engine |
| test_successful_etl | ✅ Pass | Correct mocking, assert_called_once |
| test_retry_on_failure | ✅ Pass | side_effect list, call_count == 2 |
| test_retry_count_matches_max_retries | ✅ Pass | 3 parametrize cases |
| test_pipeline_logs_success | ✅ Pass | loguru bridge in conftest.py |
| Runtime | ✅ Pass | 0.63s — fast, correct mocking |

### Branch Setup
```bash
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-03/day-19-coverage
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Python Core for DBAs                                  |
| Story           | ST-19: Coverage Reports + Fix Original Tests                 |
| Task ID         | TASK-019                                                     |
| Sprint          | Sprint 03 (Days 15–21)                                       |
| Story Points    | 2                                                            |
| Priority        | HIGH                                                         |
| Labels          | pytest, coverage, testing, day-19                            |
| Acceptance Criteria | pytest --cov passes ≥80% on etl_resilient.py; original test_etl.py fixed to 3/3; coverage HTML report generated |

---

## 📚 BACKGROUND

### What Coverage Measures — and What It Doesn't

```
Coverage = percentage of your source code lines
           that are executed during tests.

80% coverage means 80% of lines in etl_resilient.py
were touched by at least one test.

What coverage DOES tell you:
  - Which code paths are NEVER tested
  - Dead code that can be removed
  - Branches (if/else) that have no test

What coverage DOES NOT tell you:
  - Whether your tests are correct
  - Whether edge cases are handled
  - Whether the assertions are meaningful

Rule: 80% coverage with good assertions > 100% coverage with assert True
```

### Reading a Coverage Report

```
Name                    Stmts   Miss  Cover   Missing
----------------------------------------------------
etl_resilient.py           45      9    80%   34, 56-61, 78
                                              ↑
                                    These line numbers are NOT covered
                                    Open the file and look at those lines
                                    Write tests that exercise them
```

### pytest-cov Commands

```bash
# Basic coverage — terminal only
pytest --cov=etl_resilient test_etl_fixed.py

# With missing line numbers
pytest --cov=etl_resilient --cov-report=term-missing test_etl_fixed.py

# HTML report — visual, click through source
pytest --cov=etl_resilient --cov-report=html test_etl_fixed.py
# Opens: htmlcov/index.html

# Fail if coverage drops below threshold
pytest --cov=etl_resilient --cov-fail-under=80 test_etl_fixed.py
```

---

## 🎯 OBJECTIVES

1. Run `pytest --cov` on Day 18 tests — get baseline coverage number
2. Identify uncovered lines — write targeted tests to cover them
3. Reach ≥80% coverage on `etl_resilient.py`
4. Fix original `sprint-02/day-13/test_etl.py` → 3/3 passing
5. Generate HTML coverage report
6. Push clean — one commit `[DAY-019][S03]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                           |
|-------|----------|----------------------------------------------------|
| A     | 15 min   | Branch setup + baseline coverage run               |
| B     | 30 min   | Write targeted tests for uncovered lines           |
| C     | 35 min   | Fix original test_etl.py → 3/3 passing             |
| D     | 20 min   | HTML report + coverage badge in README             |
| E     | 20 min   | Commit + merge                                     |

---

## 📝 EXERCISES

---

### EXERCISE 1 — Baseline Coverage Run (Block A)
**[Full steps]**

```bash
cd C:\Users\Lenovo\python-de-journey\sprint-03\day-18

# Run with missing lines shown
pytest test_etl_fixed.py -v ^
    --cov=..\..\sprint-02\day-12\etl_resilient ^
    --cov-report=term-missing ^
    --cov-report=html:coverage_html

# Note: --cov path uses the module location, not the test location
```

**Read the output carefully:**
```
Name               Stmts   Miss  Cover   Missing
------------------------------------------------
etl_resilient.py      XX      X    XX%   <line numbers>
```

Write the uncovered line numbers in `sprint-03/day-19/coverage_notes.md`:
```markdown
# Coverage Notes — Day 19

## Baseline
- Coverage: XX%
- Uncovered lines: XX, XX-XX

## What those lines do (look them up in etl_resilient.py):
- Line XX: ...
- Lines XX-XX: ...

## Tests written to cover them:
- test_XX: covers line XX
```

---

### EXERCISE 2 — Targeted Coverage Tests (Block B)
**[Write yourself — hints for each common uncovered path]**

Create `sprint-03/day-19/test_coverage_gaps.py`:

```python
"""
test_coverage_gaps.py — Day 19 | Coverage Gap Tests
====================================================
Tests written specifically to cover lines identified as
missing in the coverage baseline run.

Common uncovered paths in ETL pipelines:
  - CRITICAL log path (all retries exhausted)
  - export_csv success path
  - load() success path
  - __init__ parameter variations

Run: pytest test_coverage_gaps.py -v --cov=../../sprint-02/day-12/etl_resilient
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pandas as pd
import pytest

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent.parent / "sprint-01" / "day-04"))
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-12"))
sys.path.insert(0, str(_here.parent.parent / "sprint-02" / "day-14"))

from etl_resilient import ResilientETLPipeline


# ── Test: export_csv actual path (uses tmp_path) ──────────────────────────────
def test_export_csv_creates_file(tmp_path: Path, sample_df: pd.DataFrame):
    """
    Test the actual export_csv method — not mocked.
    Uses tmp_path so no hardcoded paths.

    HINTS:
      - Patch get_engine, pd.read_sql, load (mock DB calls)
      - Do NOT patch export_csv — let it run for real
      - Patch the output path: monkeypatch or patch Path inside export_csv
      - Easier: patch settings.OUTPUT_CSV to return a filename
        and patch the mkdir call to use tmp_path

    Simplest approach:
      - Create pipeline
      - Call pipeline.export_csv(sample_df) directly
      - Patch just the path inside export_csv to use tmp_path:

        with patch("etl_resilient.Path") as mock_path:
            mock_dir = MagicMock()
            mock_path.return_value.parent = tmp_path
            pipeline.export_csv(sample_df)

      OR patch __file__ reference:
        with patch.object(Path, "__new__", return_value=tmp_path / "output"):
            ...

    Even simpler — directly test what export_csv does:
      pipeline = ResilientETLPipeline(max_retries=1)
      out_dir = tmp_path / "output"
      out_dir.mkdir()
      # Write directly using pandas — test the output exists
      sample_df.to_csv(out_dir / "test.csv", index=False)
      assert (out_dir / "test.csv").exists()
      assert len(pd.read_csv(out_dir / "test.csv")) == len(sample_df)
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement test_export_csv_creates_file")


# ── Test: load() success path ─────────────────────────────────────────────────
def test_load_calls_to_sql(sample_df: pd.DataFrame):
    """
    Test that load() calls df.to_sql with correct parameters.

    HINTS:
      - Patch get_engine
      - Create pipeline
      - Call pipeline.load(sample_df) directly
      - Assert to_sql was called:
            sample_df.to_sql.assert_called_once()
            OR check call args include the target table name

    Note: sample_df is a real DataFrame — to_sql will try to connect.
    Use a MagicMock DataFrame instead:
      mock_df = MagicMock(spec=pd.DataFrame)
      pipeline.load(mock_df)
      mock_df.to_sql.assert_called_once()
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement test_load_calls_to_sql")


# ── Test: __init__ with different max_retries ─────────────────────────────────
@pytest.mark.parametrize("max_retries", [1, 2, 5])
def test_pipeline_init_max_retries(max_retries: int):
    """
    Verify __init__ stores max_retries correctly.
    Simple but covers the __init__ lines.
    """
    with patch("etl_resilient.get_engine"):
        pipeline = ResilientETLPipeline(max_retries=max_retries)
        assert pipeline.max_retries == max_retries


# ── Test: CRITICAL log on final failure ──────────────────────────────────────
def test_critical_log_on_exhausted_retries(caplog):
    """
    Verify CRITICAL message is logged when all retries fail.
    Covers the logger.critical() line inside the retry loop.

    HINTS:
      - Same pattern as test_max_retries_exceeded
      - Add caplog bridge (already in conftest.py via autouse fixture)
      - After pytest.raises block:
            assert any("aborted" in m.lower() or "failed" in m.lower()
                       for m in [r.message for r in caplog.records])
    """
    # YOUR CODE HERE
    raise NotImplementedError("Implement test_critical_log_on_exhausted_retries")
```

**Run combined coverage after implementing all four:**
```bash
cd C:\Users\Lenovo\python-de-journey\sprint-03\day-18

pytest test_etl_fixed.py test_coverage_gaps.py -v ^
    --cov=..\..\sprint-02\day-12\etl_resilient ^
    --cov-report=term-missing ^
    --cov-fail-under=80

# Target: ≥80% coverage, 0 failures
```

---

### EXERCISE 3 — Fix Original test_etl.py (Block C)
**[Full steps — apply Day 18 patterns to the original file]**

Open `sprint-02/day-13/test_etl.py` and apply these three changes:

**Change 1 — Add `export_csv` mock to `test_successful_etl`:**
```python
# ADD this patch to the existing decorators:
@patch.object(ResilientETLPipeline, "export_csv")
@patch("etl_resilient.pd.read_sql")
@patch("etl_resilient.get_engine")
def test_successful_etl(mock_get_engine, mock_read_sql, mock_export_csv):
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine
    mock_df = MagicMock()
    mock_read_sql.return_value = mock_df

    pipeline = ResilientETLPipeline(max_retries=2)
    result = pipeline.run()

    assert result is mock_df
    mock_read_sql.assert_called_once()
    mock_export_csv.assert_called_once()   # ← verify export was called
```

**Change 2 — Fix `test_retry_on_failure` — add export mock + sleep mock:**
```python
@patch("etl_resilient.time.sleep")           # no real sleeping
@patch.object(ResilientETLPipeline, "export_csv")  # no file writes
@patch("etl_resilient.pd.read_sql")
@patch("etl_resilient.get_engine")
def test_retry_on_failure(mock_get_engine, mock_read_sql,
                          mock_export_csv, mock_sleep):
    mock_get_engine.return_value = MagicMock()
    mock_read_sql.side_effect = [Exception("DB connection failed"), MagicMock()]

    pipeline = ResilientETLPipeline(max_retries=3)
    result = pipeline.run()

    assert result is not None
    assert mock_read_sql.call_count == 2
    assert mock_export_csv.call_count == 1   # export only on success
    mock_sleep.assert_called_once()          # slept once between attempts
```

**Change 3 — Add sleep mock to `test_max_retries_exceeded`:**
```python
@patch("etl_resilient.time.sleep")
@patch("etl_resilient.pd.read_sql")
@patch("etl_resilient.get_engine")
def test_max_retries_exceeded(mock_get_engine, mock_read_sql, mock_sleep):
    mock_get_engine.return_value = MagicMock()
    mock_read_sql.side_effect = Exception("Persistent DB error")

    pipeline = ResilientETLPipeline(max_retries=2)

    with pytest.raises(Exception):
        pipeline.run()

    assert mock_read_sql.call_count == 2
    assert mock_sleep.call_count == 1   # slept once between attempt 1 and 2
```

**Verify original file now passes:**
```bash
cd C:\Users\Lenovo\python-de-journey\sprint-02\day-13
pytest test_etl.py -v

# Target:
# test_etl.py::test_successful_etl       PASSED
# test_etl.py::test_retry_on_failure     PASSED
# test_etl.py::test_max_retries_exceeded PASSED
# 3 passed in X.XXs
```

---

### EXERCISE 4 — HTML Coverage Report + README Badge (Block D)
**[Full steps]**

```bash
cd C:\Users\Lenovo\python-de-journey\sprint-03\day-18

# Generate HTML report
pytest test_etl_fixed.py ..\day-19\test_coverage_gaps.py ^
    --cov=..\..\sprint-02\day-12\etl_resilient ^
    --cov-report=html:..\day-19\coverage_html ^
    --cov-report=term-missing

# Open the HTML report
start ..\day-19\coverage_html\index.html
```

**Add coverage badge to README.md:**
```markdown
## Test Coverage

| Module | Coverage |
|--------|----------|
| etl_resilient.py | ![coverage](https://img.shields.io/badge/coverage-XX%25-green) |
| test_etl_fixed.py | 7/7 passing |

*Run `pytest --cov --cov-report=term-missing` to update*
```

Replace `XX` with your actual coverage number.

---

### EXERCISE 5 — Git Push (Block E)

```bash
cd C:\Users\Lenovo\python-de-journey

python scripts/daily_commit.py --day 19 --sprint 3 ^
    --message "Coverage: >=80pct on etl_resilient, original test_etl.py 3/3 fixed, HTML report generated" ^
    --merge
```

---

## ✅ DAY 19 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | Baseline coverage run — number documented in coverage_notes.md           | [ ]   |
| 2 | Uncovered lines identified and explained                                 | [ ]   |
| 3 | `test_export_csv_creates_file` — written and passing                     | [ ]   |
| 4 | `test_load_calls_to_sql` — written and passing                           | [ ]   |
| 5 | `test_pipeline_init_max_retries` — 3 parametrize cases passing           | [ ]   |
| 6 | `test_critical_log_on_exhausted_retries` — written and passing           | [ ]   |
| 7 | Combined coverage ≥80% — `--cov-fail-under=80` passes                   | [ ]   |
| 8 | Original `sprint-02/day-13/test_etl.py` → 3/3 passing                   | [ ]   |
| 9 | HTML coverage report generated in `day-19/coverage_html/`               | [ ]   |
|10 | README.md updated with coverage table                                    | [ ]   |
|11 | One clean `[DAY-019][S03]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 SELF-CHECK — All tests passing

```bash
# Run everything together:
cd sprint-03/day-18
pytest test_etl_fixed.py ..\day-19\test_coverage_gaps.py -v ^
    --cov=..\..\sprint-02\day-12\etl_resilient ^
    --cov-report=term-missing ^
    --cov-fail-under=80

# AND original:
cd ..\..\sprint-02\day-13
pytest test_etl.py -v

# Combined target:
# sprint-03/day-18: 7 passed
# sprint-03/day-19: 4 passed
# sprint-02/day-13: 3 passed
# Coverage: ≥80%
```

---

## 🔜 PREVIEW: DAY 20 — Sprint 03 Test

**Gap Sprint Final Test** — full code review of everything built Days 14–19.  
Self-assessment rubric + sprint close (`--to-main` + `sprint-03-complete` tag).  
Depending on results, Sprint 04 (Airflow) begins Day 21.

---

*Day 19 | Sprint 03 | EP-02 | TASK-019*
