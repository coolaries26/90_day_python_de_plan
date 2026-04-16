```markdown
# 📅 DAY 07 — Sprint 01 | Sprint Review + Mini Capstone + Test
## Pipeline Runner + Full Sprint 01 Closure

---

## 🔁 RETROSPECTIVE — Day 06 (Already done)

**Feedback:** JIRA works but proof file missing + 3 duplicate commits. Git hygiene needs improvement.

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
| Acceptance Criteria | pipeline_runner.py runs Days 2-6, HTML report generated, JIRA summary posted, Sprint test passed |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-01/day-07-sprint-review`           |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-007]`                                |
| Folder        | `sprint-01/day-07/`                        |
| Files to Push | `pipeline_runner.py`, `output/sprint01_report.html` |

**Create branch:**
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-01/day-07-sprint-review
```

---

## 🎯 OBJECTIVES (2 hrs)

1. Build `pipeline_runner.py` (orchestrator for Days 2–6)
2. Run official **Sprint 01 Test**
3. Generate HTML report
4. Final merge to `develop` → `main` + sprint tag
5. Personal retrospective

---

## 📝 EXERCISES

### EXERCISE 1 — pipeline_runner.py

**Create `sprint-01/day-07/pipeline_runner.py`** (full file):

```python
#!/usr/bin/env python3
import sys
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "day-04"))
from logger import get_pipeline_logger, log_pipeline_start, log_pipeline_end
from sprint-01.day-06.jira_client import jira_client

logger = get_pipeline_logger("sprint01_runner")

def run_day(module_path: str, description: str):
    logger.info("▶️ Running %s", description)
    start = time.perf_counter()
    try:
        __import__(module_path)
        logger.info("✅ %s completed in %.1fs", description, time.perf_counter() - start)
        return True
    except Exception as e:
        logger.error("❌ %s failed: %s", description, e)
        return False

def generate_html_report():
    report_path = Path("sprint-01/day-07/output/sprint01_report.html")
    report_path.parent.mkdir(exist_ok=True)
    html = f"""<!DOCTYPE html>
<html><head><title>Sprint 01 Complete</title></head><body>
<h1>🏁 Sprint 01 Complete — {datetime.now().strftime('%Y-%m-%d')}</h1>
<p>You now have a production-ready Python + PostgreSQL foundation.</p>
<ul>
<li>✅ Environment + PostgreSQL (appuser)</li>
<li>✅ db_utils + Pandas</li>
<li>✅ Logging + Git automation</li>
<li>✅ JIRA + Pydantic integration</li>
</ul>
<p><strong>Sprint 02 starts tomorrow!</strong></p>
</body></html>"""
    report_path.write_text(html, encoding="utf-8")
    logger.info("📊 HTML report generated → %s", report_path)

def main():
    log_pipeline_start(logger, "Sprint01_Capstone")
    start_time = time.perf_counter()

    success = True
    success &= run_day("day-02.db_explorer", "Day 02 — Schema Explorer")
    success &= run_day("day-03.pandas_intro", "Day 03 — Pandas")
    success &= run_day("day-04.pipeline_log_demo", "Day 04 — Logging")
    success &= run_day("day-05.git_workflow", "Day 05 — GitPython")
    success &= run_day("day-06.jira_client", "Day 06 — JIRA")

    generate_html_report()

    elapsed = time.perf_counter() - start_time
    log_pipeline_end(logger, "Sprint01_Capstone", 0, elapsed, "SUCCESS" if success else "PARTIAL")

    try:
        jira_client.create_or_update_daily_task(7, 1, "Sprint 01 completed — full pipeline runner + test passed", "CAPSTONE")
    except Exception as e:
        logger.warning("JIRA post failed: %s", e)

    logger.info("🎉 SPRINT 01 COMPLETE!")

if __name__ == "__main__":
    main()
```

### EXERCISE 2 — Run Sprint 01 Test

```bash
python sprint-01/day-07/pipeline_runner.py
```

### EXERCISE 3 — Final Commit + Merge + Tag

```bash
git add sprint-01/day-07/
python scripts/daily_commit.py \
  --day 7 \
  --sprint 1 \
  --message "Sprint 01 COMPLETE — pipeline runner + HTML report + test passed" \
  --merge \
  --to-main
```

---

## ✅ SPRINT 01 COMPLETION CHECKLIST

| # | Task                                              | Done? |
|---|---------------------------------------------------|-------|
| 1 | `pipeline_runner.py` runs with all ✅             | [ ]   |
| 2 | HTML report generated                             | [ ]   |
| 3 | Sprint test passed                                | [ ]   |
| 4 | Merged to develop + main                          | [ ]   |
| 5 | Sprint tag created                                | [ ]   |

---

**🎉 Sprint 01 is now complete once you finish the above.**

When you are done, reply with **“Day 07 done”** and I will immediately start **Sprint 02 — Day 08** (real ETL pipelines with the DVD Rental database).

You’ve built a solid foundation. Let’s keep the momentum! 🚀