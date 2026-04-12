# 🔁 Day 04 Retrospective
## Sprint 01 | Production-Grade Logging for Data Pipelines

> Fill this out at the START of Day 05 (before beginning Day 05 work).
> Honest reflection drives faster improvement.

---

## ✅ What Went Well

- [x] Day 03 rounding fix reviewed and verified
- [x] Day 03 LEFT JOIN fix validated — 1000 films confirmed loaded
- [x] `sprint-01/day-04/` branch created from `develop`
- [x] `logger.py` created with Python logging configuration
- [x] Logging levels (DEBUG/INFO/WARNING/ERROR) implemented
- [x] FileHandler configured with log rotation (5MB, 7 files)
- [x] StreamHandler for console output configured
- [x] `pipeline_log_demo.py` demonstrates logging in action
- [x] `pipeline_log_demo.py` runs successfully (exit code 0)
- [x] `logs/` directory created and logs written per run
- [x] Log files follow naming convention: `pipeline_YYYYMMDD.log`
- [x] Structured JSON log format includes timestamp, level, logger name, message
- [x] Log rotation tested — verified log files created correctly
- [ ] `db_utils.py` refactored — `print()` statements replaced with logging
- [ ] `data_explorer.py` refactored — `print()` statements replaced with logging
- [ ] All acceptance criteria met and verified
- [ ] Git commits made with `[DAY-004]` prefix
- [ ] Code pushed to `sprint-01/day-04-logging`

_Write notes here:_
Successfully created logger.py with comprehensive logging configuration including file and console handlers, log rotation, and structured JSON formatting. Pipeline_log_demo.py runs successfully and demonstrates all logging levels (DEBUG, INFO, WARNING) with proper structured output. Log files are being written to logs/ directory with timestamps. However, refactoring of db_utils.py and data_explorer.py to replace print() statements with logging calls is still in progress.

---

## ❌ What Didn't Go Well / Blockers

| Issue | Root Cause | Resolution |
|-------|------------|------------|
| db_utils.py refactoring incomplete | Partial comments added but print() statements not replaced with logging calls | Complete the refactoring by replacing all print() statements with appropriate logger calls |
| data_explorer.py refactoring incomplete | Still contains 19 print() statements across T1, T2, T3 transforms | Replace all print() statements with structured logging calls using logger.info(), logger.debug(), etc. |
| No git commits made yet | Day 04 work in progress, acceptance criteria not fully met | Complete refactoring, test thoroughly, then commit with [DAY-004] prefix |

_Note: Add any issues encountered during implementation._

---

## 📚 What I Learned Today

1. Python logging architecture: Understanding Logger, Handler, Formatter, and the flow from source code through handlers to outputs.
2. Log level filtering: How DEBUG, INFO, WARNING, ERROR, CRITICAL levels control what gets logged and why separation of concerns matters.
3. File handlers vs. stream handlers: Console logging for real-time feedback; file logging for auditability and debugging after-the-fact.
4. Log rotation: Why rotating logs at fixed sizes (5MB) and keeping a fixed number (7 files) prevents disk space issues in long-running pipelines.
5. Structured logging: Adding context (logger name, timestamp, level) to every log line makes debugging production issues much faster.
6. Refactoring from print(): Recognizing the shift from ad-hoc debugging output to systematic audit trails.
7. Log naming conventions: Using YYYYMMDD_HHMMSS timestamps makes it easy to correlate logs with specific pipeline runs.

---

## ⏱️ Time Tracking

| Block  | Estimated | Actual | Variance |
|--------|-----------|--------|----------|
| A (Branch setup + Day 03 fix verification) | 10 min | 10 min | 0 min |
| B (`logger.py` — design logging config) | 20 min | 25 min | +5 min |
| C (`pipeline_log_demo.py` — demo script) | 20 min | 15 min | -5 min |
| D (Refactor `db_utils.py` + `data_explorer.py`) | 30 min | ~20 min (partial) | -10 min (incomplete) |
| E (Test log rotation + verify output format) | 15 min | 10 min | -5 min |
| F (Log + git push) | 10 min | - | - |
| **Total** | **~1 hr 45 min** | **~1 hr 20 min** | **~-25 min** |

_Fill in Actual and Variance after completing Day 04._

---

_Paste git log --oneline -5 here:_

```
(No Day 04 commits made yet - work in progress)
Last commit: [DAY-003][FIX] T2 LEFT JOIN film_category to retain all 1000 films; DQ findings updated
```

---

*Retrospective: Day 04 | Sprint 01*
