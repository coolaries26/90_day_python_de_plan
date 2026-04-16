#!/usr/bin/env python3
import sys
from pathlib import Path

# ── ROBUST PATH FIX ───────────────────────────────
_here = Path(__file__).resolve().parent
print("DEBUG: Script running from →", _here)

# Add day-06 (config) and day-04 (logger)
sys.path.insert(0, str(_here))
sys.path.insert(0, str(_here.parent / "day-04"))

print("DEBUG: sys.path[0..1] →", sys.path[:2])

# === ALL IMPORTS ===
from jira import JIRA
from logger import get_pipeline_logger
from config.settings import settings

# Create logger BEFORE the class uses it
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

        issues = self.jira.search_issues(
            f'project={settings.JIRA_PROJECT_KEY} AND summary ~ "DAY-{day:03d}" AND status != Done'
        )

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

        # Save proof
        Path("sprint-01/day-06/output").mkdir(exist_ok=True)
        import json
        with open("sprint-01/day-06/output/jira_demo.json", "w") as f:
            json.dump({"issue_key": issue.key, "summary": issue.fields.summary}, f, indent=2)

        return issue.key


# Create singleton
jira_client = JiraClient()