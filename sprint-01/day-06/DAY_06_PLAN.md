# 📅 DAY 06 — Sprint 01 | JIRA Automation + Config-Driven Pipelines
## Extend daily_commit.py → Real JIRA Integration (python-jira + Pydantic)

---

## 🔁 RETROSPECTIVE — Day 05 (Complete BEFORE starting Day 06)

### Verification Checklist (run first — 2 minutes)
```powershell
cd C:\Users\Lenovo\python-de-journey
.venv\Scripts\activate

# 1. Confirm daily_commit.py works end-to-end
python scripts/daily_commit.py --day 5 --sprint 1 --message "TEST: Day 05 retrospective" --merge

# 2. Check progress log & develop branch
Get-Content logs\progress.md -Tail 5
git log --oneline -5 develop
```

### Instructor Feedback on Day 05 Output
| Item                        | Assessment | Note |
|-----------------------------|------------|------|
| GitPython fundamentals      | ✅ Pass     | `git_workflow.py` clean & informative |
| `daily_commit.py` automation| ✅ Pass     | Full stage → commit → push → merge flow |
| `merge_to_main()` implementation | ✅ Pass | Tag `sprint-01-complete` created correctly |
| One-commit-per-day rule     | ⚠️ Minor issue | You ended up with 5 commits for Day 005 (duplicates from test runs) |
| Branch hygiene              | ✅ Pass     | No direct commits to develop/main |
| `logs/progress.md`          | ✅ Pass     | Structured table with SHA & branch |

**Overall: Strong Day 05. The duplicate commits are a small learning point — we will enforce the one-commit rule strictly from today onward. You can safely proceed to Day 06.**

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-01: Environment & Foundations                             |
| Story           | ST-06: Production JIRA + Config Management                   |
| Task ID         | TASK-006                                                     |
| Sprint          | Sprint 01 (Days 1–7)                                         |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | jira, pydantic, automation, config, day-06                   |
| Acceptance Criteria | `daily_commit.py` now creates/updates JIRA task, logs time, updates status; Pydantic config validated; secrets never in git |

---

## 📁 GIT REPO DETAIL

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Branch        | `sprint-01/day-06-jira-automation`         |
| Base Branch   | `develop`                                  |
| Commit Prefix | `[DAY-006]`                                |
| Folder        | `sprint-01/day-06/`                        |
| Files to Push | `jira_client.py`, updated `daily_commit.py`, `config/settings.py`, `output/jira_demo.json` |

**Create branch now:**
```bash
cd C:\Users\Lenovo\python-de-journey
git checkout develop
git pull origin develop
git checkout -b sprint-01/day-06-jira-automation
```

---

## 📚 BACKGROUND

You now have a rock-solid Git automation script.  
Today we make it **production-grade** by connecting it to JIRA (exactly what real Data Engineering teams do).  
Every pipeline run will auto-create a JIRA task under EP-01, log 2 hours, and update status to “Done”.

We also introduce **Pydantic v2** for strict config validation — the same pattern used in every modern data pipeline.

---

## 🎯 OBJECTIVES

1. Install `jira` + `pydantic` + `python-dotenv`
2. Build `jira_client.py` — reusable JIRA client (used for next 84 days)
3. Create strict Pydantic settings model (`config/settings.py`)
4. Extend `daily_commit.py` to auto-post progress to JIRA
5. Run full Day-06 workflow end-to-end
6. Verify everything in JIRA UI + git

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                              |
|-------|----------|---------------------------------------|
| A     | 15 min   | Retrospective + new branch + installs |
| B     | 30 min   | `config/settings.py` + Pydantic       |
| C     | 35 min   | `jira_client.py`                      |
| D     | 25 min   | Update `daily_commit.py`              |
| E     | 15 min   | Run + verify + git push               |

---

## 📝 EXERCISES

### EXERCISE 1 — Install libraries & create config folder (Block A)

```bash
.venv\Scripts\activate
pip install jira==3.8.0 pydantic==2.10.1 pydantic-settings==2.7.0

# Add to requirements.txt
echo "jira==3.8.0" >> requirements.txt
echo "pydantic==2.10.1" >> requirements.txt
echo "pydantic-settings==2.7.0" >> requirements.txt
```

```bash
mkdir -p sprint-01/day-06/config
mkdir -p sprint-01/day-06/output
```

### EXERCISE 2 — config/settings.py (Block B)

**Create `sprint-01/day-06/config/settings.py`**:

```python
#!/usr/bin/env python3
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr
from pathlib import Path

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    JIRA_URL: str
    JIRA_USERNAME: str
    JIRA_API_TOKEN: SecretStr
    JIRA_PROJECT_KEY: str = "EP01"

    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    @property
    def jira_auth(self):
        return (self.JIRA_USERNAME, self.JIRA_API_TOKEN.get_secret_value())

settings = AppSettings()
```

**Add these lines to your `.env` file** (at project root):
```env
JIRA_URL=https://your-company.atlassian.net
JIRA_USERNAME=your-email@domain.com
JIRA_API_TOKEN=your_actual_api_token_here
JIRA_PROJECT_KEY=EP01
```

> **Get your token here:** https://id.atlassian.com/manage-profile/security/api-tokens

### EXERCISE 3 — jira_client.py (Block C)

**Create `sprint-01/day-06/jira_client.py`** (full file):

```python
#!/usr/bin/env python3
import json
from pathlib import Path
from jira import JIRA
from config.settings import settings
from logger import get_pipeline_logger

logger = get_pipeline_logger("jira_client")

class JiraClient:
    def __init__(self):
        self.jira = JIRA(server=settings.JIRA_URL, basic_auth=settings.jira_auth)
        logger.info("✅ Connected to JIRA | project={}", settings.JIRA_PROJECT_KEY)

    def create_or_update_daily_task(self, day: int, sprint: int, message: str, sha: str) -> str:
        summary = f"[DAY-{day:03d}][S{sprint:02d}] Daily Progress — Python DE Journey"
        description = f"""
h3. Day {day:03d} — Sprint {sprint:02d}
*Message:* {message}
*Commit:* {sha}
*Branch:* sprint-01/day-06-jira-automation
        """

        issues = self.jira.search_issues(f'project={settings.JIRA_PROJECT_KEY} AND summary ~ "DAY-{day:03d}" AND status != Done')
        if issues:
            issue = issues[0]
            self.jira.add_comment(issue.key, f"Updated progress\n{message}")
            logger.info("Updated JIRA task | {}", issue.key)
        else:
            issue = self.jira.create_issue(
                project=settings.JIRA_PROJECT_KEY,
                summary=summary,
                description=description,
                issuetype={"name": "Task"},
                labels=["automation", "day-06"]
            )
            logger.info("Created new JIRA task | {}", issue.key)

        self.jira.add_worklog(issue.key, timeSpent="2h")
        logger.info("Logged 2h work on task {}", issue.key)

        Path("sprint-01/day-06/output").mkdir(exist_ok=True)
        with open("sprint-01/day-06/output/jira_demo.json", "w") as f:
            json.dump({"issue_key": issue.key, "summary": issue.fields.summary}, f, indent=2)

        return issue.key

jira_client = JiraClient()
```

### EXERCISE 4 — Update daily_commit.py (Block D)

Open `scripts/daily_commit.py` and **add this import** after the existing imports:
```python
from jira_client import jira_client
```

Then **add this block** right after `append_progress_log(...)`:
```python
        # === JIRA AUTOMATION ===
        try:
            issue_key = jira_client.create_or_update_daily_task(
                day=args.day, sprint=sprint, message=args.message, sha=sha
            )
            log.info("JIRA automation complete | task={}", issue_key)
        except Exception as exc:
            log.error("JIRA post failed (non-blocking) | {}", exc)
```

### EXERCISE 5 — Run Day 06 & Git Push (Block E)

```bash
cd C:\Users\Lenovo\python-de-journey

git add sprint-01/day-06/
git add scripts/daily_commit.py
git add requirements.txt

python scripts/daily_commit.py \
  --day 6 \
  --sprint 1 \
  --message "JIRA automation + Pydantic config complete" \
  --merge
```

---

## ✅ DAY 06 COMPLETION CHECKLIST

| # | Task                                              | Done? |
|---|---------------------------------------------------|-------|
| 1 | Retrospective completed                           | [ ]   |
| 2 | `config/settings.py` created                      | [ ]   |
| 3 | `jira_client.py` created and connects to JIRA     | [ ]   |
| 4 | `daily_commit.py` now posts to JIRA               | [ ]   |
| 5 | JIRA task created + 2h logged                     | [ ]   |
| 6 | `output/jira_demo.json` proof file exists         | [ ]   |
| 7 | One clean commit + merge to develop               | [ ]   |

---

## 🔜 PREVIEW: DAY 07 (Sprint 01 End)

Full Sprint 01 Review + Mini Capstone Project + Sprint Test.

---

*Day 06 | Sprint 01 | EP-01 | TASK-006*

```

---

**You now have the complete, ready-to-use `DAY_06_PLAN.md` file.**

Just follow the steps inside it (start with the retrospective checklist).  
When you finish Day 06, run the final `daily_commit.py` command shown in Exercise 5 — it will automatically create the JIRA task for you.

Let me know when you're done with Day 06 (or if you hit any issue with the JIRA token/config) and I’ll immediately generate **DAY_07_PLAN.md** (last day of Sprint 01 + full review + test).  

You're making excellent progress! 🚀
```

---
