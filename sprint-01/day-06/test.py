#!/usr/bin/env python3
"""
Standalone JIRA diagnostic script
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from jira import JIRA

# Explicitly load .env from project root
project_root = Path(__file__).resolve().parent
load_dotenv(project_root / ".env", override=True)

print("=== JIRA Diagnostic ===")
print(f"Project root     : {project_root}")
print(f"JIRA_URL         : {os.getenv('JIRA_URL')}")
print(f"JIRA_USERNAME    : {os.getenv('JIRA_USERNAME')}")
print(f"JIRA_PROJECT_KEY : {os.getenv('JIRA_PROJECT_KEY')}")
print(f"JIRA_API_TOKEN   : {'***' if os.getenv('JIRA_API_TOKEN') else 'MISSING'}")

if not all([os.getenv('JIRA_URL'), os.getenv('JIRA_USERNAME'), os.getenv('JIRA_API_TOKEN'), os.getenv('JIRA_PROJECT_KEY')]):
    print("\n❌ Missing required environment variables in .env")
    exit(1)

try:
    jira = JIRA(
        server=os.getenv('JIRA_URL'),
        basic_auth=(os.getenv('JIRA_USERNAME'), os.getenv('JIRA_API_TOKEN'))
    )
    print(f"\n✅ Successfully connected to JIRA")
    print(f"Project key used : {os.getenv('JIRA_PROJECT_KEY')}")

    # Try to create a test task
    issue = jira.create_issue(
        project=os.getenv('JIRA_PROJECT_KEY'),
        summary=f"Diagnostic test task - {os.getenv('JIRA_PROJECT_KEY')}",
        description="This is a test from the diagnostic script",
        issuetype={"name": "Task"}
    )
    print(f"✅ Test task created successfully: {issue.key}")

except Exception as e:
    print(f"\n❌ Failed: {e}")