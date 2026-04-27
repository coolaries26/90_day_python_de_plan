#!/usr/bin/env python3
import sys
from pathlib import Path
from datetime import datetime   # ← Added this line

# ── ROBUST PATH FIX ───────────────────────────────
_here = Path(__file__).resolve().parent
print("DEBUG: Script running from →", _here)

sys.path.insert(0, str(_here))
sys.path.insert(0, str(_here.parent / "day-04"))
sys.path.insert(0, str(_here / "config"))

print("DEBUG: sys.path[0..1] →", sys.path[:2])

from jira import JIRA
from logger import get_pipeline_logger
from settings import settings

logger = get_pipeline_logger("jira_client")

def create_or_update_daily_task(
    self,
    day: int,
    sprint: int,
    message: str,
    sha: str,
    epic_name: str = "",
    story_points: int = 3,
    priority: str = "High",
    labels: list[str] | None = None,
) -> str:
    """
    Create or update a JIRA task matching the daily plan's JIRA card format.
    Searches for existing open task for this day first — updates if found.
    """
    from datetime import datetime

    summary = f"[DAY-{day:03d}][S{sprint:02d}] {message[:80]}"
    labels = labels or ["python-de-journey", f"day-{day:03d}", f"sprint-{sprint:02d}"]

    description = (
        f"h3. Day {day:03d} — Sprint {sprint:02d}\n\n"
        f"*Message:* {message}\n"
        f"*Commit SHA:* {sha}\n"
        f"*Date:* {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"*Epic:* {epic_name or 'Python DE Journey'}\n"
        f"*Story Points:* {story_points}\n"
    )

    # Search for existing open task for this day
    jql = (
        f'project = "{settings.JIRA_PROJECT_KEY}" '
        f'AND summary ~ "DAY-{day:03d}" '
        f'AND summary ~ "S{sprint:02d}" '
        f'AND statusCategory != Done '
        f'ORDER BY created DESC'
    )

    try:
        existing = self.jira.search_issues(jql, maxResults=1)
    except Exception as exc:
        logger.warning(f"JIRA search failed | {exc} | creating new task")
        existing = []

    if existing:
        # Update existing task
        issue = existing[0]
        issue.update(summary=summary, description=description)
        self.jira.add_comment(issue.key, f"Progress update: {message}\nSHA: {sha}")
        logger.info(f"Updated JIRA task | {issue.key}")
    else:
        # Build create payload — only include fields the project supports
        payload: dict = {
            "project":     {"key": settings.JIRA_PROJECT_KEY},
            "summary":     summary,
            "description": description,
            "issuetype":   {"name": "Task"},
            "priority":    {"name": priority},
            "labels":      labels,
        }

        # Story points — field name varies by JIRA instance
        # Try story_points field — skip if project doesn't support it
        for sp_field in ["story_points", "customfield_10016",
                         "customfield_10028", "customfield_10014"]:
            try:
                test_payload = {**payload, sp_field: story_points}
                issue = self.jira.create_issue(**test_payload)
                logger.info(f"Created JIRA task | {issue.key} | sp_field={sp_field}")
                break
            except Exception:
                continue
        else:
            # Create without story points if all field names failed
            issue = self.jira.create_issue(**payload)
            logger.info(f"Created JIRA task | {issue.key} | (no story points)")

    # Log 2h work
    try:
        self.jira.add_worklog(issue.key, timeSpent="2h")
        logger.info(f"Logged 2h on {issue.key}")
    except Exception as exc:
        logger.warning(f"Worklog failed (non-blocking) | {exc}")

    # Trsnsition to In Progress if still To Do
    try:
        transitions = self.jira.transitions(issue.key)
        in_progress = next(
            (t for t in transitions
             if "progress" in t["name"].lower() or "start" in t["name"].lower()),
            None
        )
        if in_progress:
            self.jira.transition_issue(issue.key, in_progress["id"])
            logger.info(f"Transitioned {issue.key} → In Progress")
    except Exception as exc:
        logger.warning(f"Transition failed (non-blocking) | {exc}")

    # Save proof file
    import json
    out_dir = Path(__file__).parent / "output"
    out_dir.mkdir(exist_ok=True)
    with open(out_dir / "jira_demo.json", "w", encoding="utf-8") as f:
        json.dump({
            "issue_key":   issue.key,
            "summary":     issue.fields.summary,
            "day":         day,
            "sprint":      sprint,
            "sha":         sha,
        }, f, indent=2)

    return issue.key