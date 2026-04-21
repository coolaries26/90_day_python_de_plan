```markdown
# 📅 DAY 13 — Sprint 02 | CLI-Driven ETL Pipelines + Basic Testing
## Making Pipelines Executable from Command Line + pytest

---
## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-02: Core Data Pipeline                                    |
| Story           | ST-13: CLI-Driven ETL + Basic Unit Testing                   |
| Task ID         | TASK-013                                                     |
| Sprint          | Sprint 02 (Days 8–14)                                        |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | etl, cli, pytest, testing, day-13                            |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-02/day-13-cli-etl`                 |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-013]`                                |
| Folder        | `sprint-02/day-13/`                        |

**Create branch (Git steps at start):**
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-02/day-13-cli-etl
```

---

## 🎯 OBJECTIVES (2 hrs)

Today you will:
1. Turn your ETL pipeline into a **CLI tool** (using `click`)
2. Add basic **unit tests** with `pytest`
3. Learn how to test database-dependent code

---

## 📝 EXERCISES (Hints-First Style)

### EXERCISE 1 — CLI-Driven ETL Pipeline

**Task:**  
Create `sprint-02/day-13/etl_cli.py`

**Hints (more explanatory):**
- Use the `click` library to create command-line options
- The command should accept `--pipeline-name`, `--target-table`, `--max-retries`
- Reuse your `ResilientETLPipeline` class from Day 12
- Make the main function use `@click.command()` and `@click.option()`
- Add a default behavior when no arguments are given

Try writing this yourself first.  
When you finish (or get stuck), reply with **“Show reference for Exercise 1”**.

### EXERCISE 2 — Basic Unit Testing with pytest

**Task:**  
Create `sprint-02/day-13/test_etl.py`

**Hints (more explanatory):**
- Use `pytest` to test the `ResilientETLPipeline` class
- Mock the database connection (so tests don't hit real PostgreSQL)
- Write at least 3 tests: one for successful run, one for retry logic, one for error handling
- Use `pytest.raises()` for expected exceptions

Try writing this yourself first.  
When you finish (or get stuck), reply with **“Show reference for Exercise 2”**.

---

## ✅ DAY 13 COMPLETION CHECKLIST

| # | Task                                              | Done? |
|---|---------------------------------------------------|-------|
| 1 | Branch created                                    | [ ]   |
| 2 | CLI ETL pipeline written                          | [ ]   |
| 3 | Basic pytest tests written                        | [ ]   |
| 4 | One clean commit + merge                          | [ ]   |

---

**Git steps at the end (after you finish the exercises):**

```powershell
python scripts/daily_commit.py \
  --day 13 \
  --sprint 2 \
  --message "Day 13: CLI-driven ETL pipeline + basic pytest unit tests" \
  --merge
```

---

**Ready when you are.**

Reply with **“Start Day 13”** and I will give you more detailed hints for Exercise 1 if you want, or you can start coding right away.

You are doing really well — the code you write from now on will be increasingly your own.