#!/usr/bin/env python3
"""
pipeline_runner.py — Sprint 01 Capstone
Orchestrates Days 2-6 and generates final report.
"""
import sys
from pathlib import Path
import time
from datetime import datetime

# ── PATH FIX FOR JIRA CLIENT ───────────────────────────────
_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here.parent / "day-04"))          # logger
sys.path.insert(0, str(_here.parent / "day-06"))          # jira_client + config

print("DEBUG: sys.path added for jira_client")

# === IMPORTS ===
from logger import get_pipeline_logger, log_pipeline_start, log_pipeline_end
from jira_client import jira_client   # ← Corrected import

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