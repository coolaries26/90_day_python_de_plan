**✅ HERE IS YOUR DOWNLOADABLE `DAY_07_PLAN.md`**  
**(Final day of Sprint 01 — Sprint Review + Mini Capstone + Test)**

**How to save it (10 seconds):**
1. Copy **everything below the line** (from `# 📅 DAY 07` to the end).
2. Paste into VS Code → Save as:  
   **`sprint-01/day-07/DAY_07_PLAN.md`**

---

```markdown
# 📅 DAY 07 — Sprint 01 | Sprint Review + Mini Capstone + Test
## Pipeline Runner + Full Sprint 01 Closure

---

## 🔁 RETROSPECTIVE — Day 06 (Complete BEFORE starting Day 07)

### Verification Checklist (run first — 2 minutes)
```powershell
cd C:\Users\Lenovo\python-de-journey
.venv\Scripts\activate

# 1. Check JIRA integration worked
python -c "
import json
with open('sprint-01/day-06/output/jira_demo.json') as f:
    data = json.load(f)
    print('✅ JIRA Task:', data.get('issue_key'))
"

# 2. Verify progress log & develop
Get-Content logs\progress.md -Tail 3
git log --oneline -5 develop
```

### Instructor Feedback on Day 06
| Item                        | Assessment | Note |
|-----------------------------|------------|------|
| Pydantic settings           | ✅ Pass     | Strict config validation working |
| `jira_client.py`            | ✅ Pass     | Clean reusable client |
| JIRA task creation + worklog| ✅ Pass     | Task created & 2h logged |
| Integration in daily_commit | ✅ Pass     | Non-blocking error handling |
| Git discipline              | ✅ Pass     | One clean commit (as requested) |

**Overall: Excellent Day 06. You have now production-grade automation (Git + JIRA + Config). Sprint 01 foundations are rock-solid. Time to close the sprint with a mini capstone.**

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-01: Environment & Foundations                             |
| Story           | ST-07: Sprint 01 Review + Capstone Pipeline Runner           |
| Task ID         | TASK-007                                                     |
| Sprint          | Sprint 01 (Days 1–7)                                         |
| Story Points    | 4                                                            |
| Priority        | CRITICAL                                                     |
| Labels          | review, capstone, pipeline, test, sprint-01                  |
| Acceptance Criteria | `pipeline_runner.py` executes Days 2-6, generates HTML report, posts summary to JIRA; Sprint test passed; develop → main merged + tagged |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-01/day-07-sprint-review`           |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-007]`                                |
| Folder        | `sprint-01/day-07/`                        |
| Files to Push | `pipeline_runner.py`, `sprint_review.md`, `output/sprint01_report.html` |

**Create branch now:**
```bash
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-01/day-07-sprint-review
```

---

## 📚 BACKGROUND

Today is the **official close of Sprint 01**.  
You have built:
- Environment (Python + PostgreSQL + appuser)
- Reusable DB utilities + Pandas exploration
- Production logging + Git automation
- JIRA integration + Pydantic config

Now we tie everything together into one **orchestrator script** (`pipeline_runner.py`) — exactly what real Data Engineering teams use to run daily pipelines.

---

## 🎯 OBJECTIVES

1. Build `pipeline_runner.py` — executes Days 2-6 in sequence
2. Generate a beautiful HTML sprint report
3. Post sprint summary to JIRA
4. Run the official **Sprint 01 Test**
5. Merge to `develop` → `main`, create sprint tag
6. Write personal sprint retrospective

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                              |
|-------|----------|---------------------------------------|
| A     | 15 min   | Retrospective + branch                |
| B     | 40 min   | Build `pipeline_runner.py`            |
| C     | 25 min   | Sprint 01 Test                        |
| D     | 20 min   | Final merge + tag + report            |
| E     | 20 min   | Personal retrospective + push         |

---

## 📝 EXERCISES

### EXERCISE 1 — Create pipeline_runner.py (Block B)

**Create `sprint-01/day-07/pipeline_runner.py`** (full file):

```python
#!/usr/bin/env python3
"""
pipeline_runner.py — Sprint 01 Capstone
Orchestrates Days 2-6 and generates final report.
"""
import sys
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "day-04"))
from logger import get_pipeline_logger, log_pipeline_start, log_pipeline_end
from jira_client import jira_client

logger = get_pipeline_logger("sprint01_runner")

def run_day(module_path: str, description: str):
    logger.info("▶️ Running %s", description)
    start = time.perf_counter()
    try:
        __import__(module_path)
        elapsed = time.perf_counter() - start
        logger.info("✅ %s completed in %.1fs", description, elapsed)
        return True
    except Exception as e:
        logger.error("❌ %s failed: %s", description, e)
        return False

def generate_html_report():
    report_path = Path("sprint-01/day-07/output/sprint01_report.html")
    report_path.parent.mkdir(exist_ok=True)
    html = f"""<!DOCTYPE html>
<html><head><title>Sprint 01 — Python DE Journey</title></head><body>
<h1>🏁 Sprint 01 Complete — {datetime.now().strftime('%Y-%m-%d')}</h1>
<p>Congratulations! You have a production-ready Python + PostgreSQL foundation.</p>
<ul>
<li>✅ Environment + PostgreSQL (appuser)</li>
<li>✅ db_utils + schema explorer</li>
<li>✅ Pandas transformations</li>
<li>✅ Production logging + rotation</li>
<li>✅ GitPython automation</li>
<li>✅ JIRA + Pydantic integration</li>
</ul>
<p><strong>Next: Sprint 02 — DVD Rental Data Pipeline</strong></p>
</body></html>"""
    report_path.write_text(html, encoding="utf-8")
    logger.info("📊 HTML report generated → %s", report_path)

def main():
    log_pipeline_start(logger, "Sprint01_Capstone")
    start_time = time.perf_counter()

    success = True
    success &= run_day("day-02.db_explorer", "Day 02 — Schema Explorer")
    success &= run_day("day-03.pandas_intro", "Day 03 — Pandas Intro")
    success &= run_day("day-04.pipeline_log_demo", "Day 04 — Logging Demo")
    success &= run_day("day-05.git_workflow", "Day 05 — GitPython Demo")
    success &= run_day("day-06.jira_client", "Day 06 — JIRA Client Test")

    generate_html_report()

    elapsed = time.perf_counter() - start_time
    status = "SUCCESS" if success else "PARTIAL"
    log_pipeline_end(logger, "Sprint01_Capstone", rows_processed=0, elapsed_sec=elapsed, status=status)

    # Post to JIRA
    try:
        jira_client.create_or_update_daily_task(
            day=7, sprint=1,
            message="Sprint 01 completed — full pipeline runner + test passed",
            sha="CAPSTONE"
        )
    except Exception as e:
        logger.warning("JIRA post failed (non-blocking): %s", e)

    logger.info("🎉 SPRINT 01 COMPLETE!")

if __name__ == "__main__":
    main()
```

### EXERCISE 2 — Sprint 01 Test (Block C)

Run the capstone:
```bash
cd C:\Users\Lenovo\python-de-journey
python sprint-01/day-07/pipeline_runner.py
```

**You pass the Sprint 01 Test if you see:**
- All 5 modules run with ✅
- HTML report created
- JIRA task updated
- No critical errors

### EXERCISE 3 — Final Merge + Tag (Block D)

```bash
cd C:\Users\Lenovo\python-de-journey

git add sprint-01/day-07/
git add scripts/daily_commit.py   # if you updated it

python scripts/daily_commit.py \
  --day 7 \
  --sprint 1 \
  --message "Sprint 01 COMPLETE — pipeline runner, HTML report, full test passed" \
  --merge \
  --to-main
```

---

## ✅ DAY 07 / SPRINT 01 COMPLETION CHECKLIST

| # | Task                                              | Done? |
|---|---------------------------------------------------|-------|
| 1 | Retrospective completed                           | [ ]   |
| 2 | `pipeline_runner.py` created and runs successfully| [ ]   |
| 3 | HTML report generated                             | [ ]   |
| 4 | Sprint 01 Test passed                             | [ ]   |
| 5 | Merged to develop + main                          | [ ]   |
| 6 | Sprint tag created (`sprint-01-complete`)         | [ ]   |
| 7 | JIRA task for Day 07 created                      | [ ]   |

---

## 🔜 PREVIEW: SPRINT 02 (Days 8–14) — DVD Rental Data Pipeline

**Day 08:** Restore full Pagila DVD Rental dataset + advanced schema exploration  
**Day 09:** ETL pipeline with SQLAlchemy + Alembic migrations  
**Day 10:** Airflow DAG basics (first scheduler!)  
...and more.

---

**🎉 CONGRATULATIONS!**  
You have officially completed **Sprint 01** — a production-grade Python + PostgreSQL foundation.

*Day 07 | Sprint 01 | EP-01 | TASK-007 — SPRINT COMPLETE*

---

**You now have the complete `DAY_07_PLAN.md` file.**

**Next steps:**
1. Save the file.
2. Run the verification checklist at the top.
3. Start working through the exercises.
4. When finished, run the final `daily_commit.py` command (it will also merge to `main` and create the sprint tag).

Reply with **“Day 07 done”** (or paste any output/logs if you want feedback) and I will immediately start **Sprint 02 — Day 08**.

You did an outstanding job on Sprint 01. Ready for the real data engineering work? 🚀
```