#!/usr/bin/env python3
import sys
from pathlib import Path
from datetime import datetime   # ← Added this line

# ── ROBUST PATH FIX ───────────────────────────────
_here = Path(__file__).resolve().parent
print("DEBUG: Script running from →", _here)

sys.path.insert(0, str(_here))
sys.path.insert(0, str(_here.parent / "day-04"))

print("DEBUG: sys.path[0..1] →", sys.path[:2])

from jira import JIRA
from logger import get_pipeline_logger
from config.settings import settings

logger = get_pipeline_logger("jira_client")

class JiraClient:
    def __init__(self):
        self.jira = JIRA(server=settings.JIRA_URL, basic_auth=settings.jira_auth)
        logger.info("✅ Connected to JIRA | project={}", settings.JIRA_PROJECT_KEY)


    def create_or_update_daily_task(self, day: int, sprint: int, message: str, sha: str) -> str:
        """Temporary hardcoded project key + debug print"""
        
        # === TEMPORARY HARDCODE (for debugging) ===
        project_key = "COOLA8"          # ← Change only this line if needed
        print(f"DEBUG: Using JIRA project key = '{project_key}'")   # ← This will show us the truth
        
        summary = f"[DAY-{day:03d}][S{sprint:02d}] Daily Progress — Python DE Journey"
        description = f"""
h3. Day {day:03d} — Sprint {sprint:02d}
*Message:* {message}
*Commit:* {sha}
*Branch:* sprint-01/day-07-sprint-review
        """

        issue = self.jira.create_issue(
            project=project_key,                    # ← using hardcoded key
            summary=summary,
            description=description,
            issuetype={"name": "Task"},
            labels=["automation", f"day-{day:03d}"]
        )

        self.jira.add_worklog(issue.key, timeSpent="2h")
        logger.info("✅ Created new JIRA task | {}", issue.key)

        # Save proof
        Path("sprint-01/day-06/output").mkdir(exist_ok=True)
        import json
        with open("sprint-01/day-06/output/jira_demo.json", "w") as f:
            json.dump({"issue_key": issue.key, "summary": issue.fields.summary}, f, indent=2)

        return issue.key
    

# Create singleton
jira_client = JiraClient()