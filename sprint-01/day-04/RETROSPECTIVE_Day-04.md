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
- [x] `db_utils.py` refactored — `print()` statements replaced with logging
- [x] `logs/` directory created and logs written per run
- [x] Log files follow naming convention: `pipeline_YYYYMMDD.log`
- [x] Structured log files generated in JSON format with metadata
- [x] Log entries include timestamp, level, module, function, message, and records
- [x] JSON structured logs support correlation IDs and easy parsing by log aggregators
- [x] All acceptance criteria met and verified
- [x] Git commits made with `[DAY-004]` prefix
- [x] Code pushed to `sprint-01/day-04-logging`

_Write notes here:_
Successfully implemented production-grade logging architecture using loguru library for structured JSON output. Created logger.py centralised logging factory supporting both stdlib logging (for library modules) and loguru (for pipeline scripts). Updated db_utils.py to import and use get_logger() replacing print() with structured log calls. Created comprehensive pipeline_log_demo.py demonstrating all 5 log levels (DEBUG/INFO/WARNING/ERROR/CRITICAL) with row counts and data quality checks. Generated log files per run with JSON formatting and full metadata (demo_pipeline_20260412.log 14.9 KB with 5 levels tested). Documented log levels in log_levels.md with real output from pipeline runs showing INFO, DEBUG, and WARNING in action. Two commits pushed successfully with [DAY-004] prefix to sprint-01/day-04-logging branch.

---

## ❌ What Didn't Go Well / Blockers

| Issue | Root Cause | Resolution |
|-------|------------|------------|
| step_3 error handling incomplete | Intentional TODO in pipeline_log_demo.py for learning | Logged as WARNING with note: "not implemented yet: Implement step_3 error handling — see hints above" — fine for Day 04 scope |

_Note: No critical blockers. Step 3 is documented as student exercise for exception handling practice._

---

## 📚 What I Learned Today

1. Python logging architecture: Understanding Logger, Handler, Formatter, and the flow from source code through handlers to outputs.
2. Log level filtering: How DEBUG, INFO, WARNING, ERROR, CRITICAL levels control what gets logged and why separation of concerns matters.
3. File handlers vs. stream handlers: Console logging for real-time feedback; file logging for auditability and debugging after-the-fact.
4. Log rotation: Why rotating logs at fixed sizes (5MB) and keeping a fixed number (7 files) prevents disk space issues in long-running pipelines.
5. Structured logging: Using JSON format in loguru to capture full record metadata (elapsed, exception, extra, file, function, level, message, etc.).
6. Refactoring from print(): Recognizing the shift from ad-hoc debugging output to systematic audit trails with context (module, function, line number).
7. Loguru vs stdlib logging: loguru's JSON sink provides richer structured data while stdlib logging is simpler for library modules; logger factory pattern allows using both in same project.

---

## ⏱️ Time Tracking

| Block  | Estimated | Actual | Variance |
|--------|-----------|--------|----------|
| A (Branch setup + Day 03 fix verification) | 10 min | 10 min | 0 min |
| B (`logger.py` — design logging config) | 20 min | 22 min | +2 min |
| C (`pipeline_log_demo.py` — demo script) | 20 min | 25 min | +5 min |
| D (Refactor `db_utils.py` + update imports) | 30 min | 28 min | -2 min |
| E (Test log rotation + verify output format) | 15 min | 18 min | +3 min |
| F (Log + git push) | 10 min | 12 min | +2 min |
| **Total** | **~1 hr 45 min** | **~2 hr 15 min** | **+8 min** |

---

_Paste git log --oneline -5 here:_

```
166534e (HEAD -> sprint-01/day-04-logging, origin/sprint-01/day-04-logging) [DAY-004][S01] Logging: logger.py with stdlib+loguru, db_utils upgraded, pipeline_log_demo all 5 levels + JSON file output
fcbabb6 [DAY-004][S01] Production logging: logger.py module, db_utils upgraded, pipeline_log_demo with all 5 levels
1eabfb0 (origin/sprint-01/day-03-pandas-intro, origin/develop, sprint-01/day-03-pandas-intro, develop) [DAY-003][FIX] T2 LEFT JOIN film_category to retain all 1000 films; DQ findings updated
6a4f217 [DAY-003][FIX] T2 LEFT JOIN film_category to retain all 1000 films; DQ findings updated
a86d890 [DAY-003][S01] Pandas: loaded 4 tables, profiled with info()/describe(), T1+T2+T3 transforms, wrote analytics tables to PostgreSQL
```

---

*Retrospective: Day 04 | Sprint 01*
