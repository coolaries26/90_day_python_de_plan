**✅ DAY 11 COMPLETE — Well done!**

You successfully built a **config-driven ETL pipeline** using Pydantic. This is a major step toward production-grade code.

---

**I have noted your feedback carefully.**

From **Day 12 onwards**, I will change the style as you requested:

- I will give **hints and requirements first**
- I will **not** provide the full code immediately
- You will try writing the code yourself
- After you finish (or get stuck), I will share the reference solution for comparison

This will help you gain more confidence and deeper understanding.

---

### **DAY 12 PLAN** (with the new hints-first approach)

**Save this as:**  
`sprint-02/day-12/DAY_12_PLAN.md`

```markdown
# 📅 DAY 12 — Sprint 02 | Error Handling & Pipeline Resilience
## Making ETL Pipelines Production-Ready

---

## 🔁 RETROSPECTIVE — Day 11

**Good work!**  
You now have a config-driven ETL pipeline.  
Today we focus on making it robust and resilient.

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Core Data Pipeline                                    |
| Story           | ST-12: Error Handling & Pipeline Resilience                  |
| Task ID         | TASK-012                                                     |
| Sprint          | Sprint 02 (Days 8–14)                                        |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | etl, error-handling, resilience, day-12                      |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-02/day-12-resilient-etl`           |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-012]`                                |
| Folder        | `sprint-02/day-12/`                        |

**Create branch:**
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-02/day-12-resilient-etl
```

---

## 🎯 OBJECTIVES (2 hrs)

Build a more resilient ETL pipeline that can:
- Handle database connection failures
- Handle query errors gracefully
- Retry failed operations
- Log errors properly without crashing the whole pipeline

---

## 📝 EXERCISES (Hints-First Style)

### EXERCISE 1 — Resilient ETL Pipeline

**Task:**  
Create a new file `sprint-02/day-12/etl_resilient.py`

**Hints:**
- Use `try/except` around the main operations
- Catch specific exceptions: `psycopg2.Error`, `SQLAlchemyError`, `pandas` errors
- Add retry logic (use a simple loop with `time.sleep`)
- Log errors at appropriate levels (`ERROR` or `WARNING`)
- Ensure the pipeline continues even if one part fails
- Use the existing `ETLPipeline` class from Day 11 as a base if you want

**Requirements:**
- The pipeline should still extract customer analytics
- Log clear messages when errors occur
- Retry up to 3 times on transient errors
- Save partial results if possible

**Try writing this yourself first.**  
When you are done (or stuck), reply with **“Show reference for Exercise 1”** and I will show you my version for comparison.

### EXERCISE 2 — Data Quality with Error Handling

**Task:**  
Enhance `data_quality.py` (or create `data_quality_resilient.py`) to handle missing tables or query failures gracefully.

**Hints:**
- Wrap each check in try/except
- Log which check failed without stopping the others
- Return a summary of passed/failed checks

---

## ✅ DAY 12 COMPLETION CHECKLIST

| # | Task                                              | Done? |
|---|---------------------------------------------------|-------|
| 1 | Branch created                                    | [ ✅ ]   |
| 2 | Resilient ETL pipeline written                    | [ ✅ ]   |
| 3 | Data quality with error handling                  | [ ✅ ]   |
| 4 | Pipeline runs successfully                        | [ ✅ ]   |
| 5 | One clean commit + merge                          | [ ✅ ]   |

---

**When you are ready to start**, reply with **“Start Day 12”** and I will give you more detailed hints for Exercise 1 if needed.

You are now in control of how much guidance you want. Let me know how you’d like to proceed.
