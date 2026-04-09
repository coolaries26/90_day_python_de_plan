# 🐍 Python Data Engineering Journey — 90 Days

**Learner:** [Alok Srivastava]
**Start Date:** [April 07, 2026]
**Target Role:** Python Data Engineer / Data Scientist

## 📚 BACKGROUND

As a DBA, you already understand relational databases, schema design, query optimization,
and production operations. This is your superpower. Python will be the glue language that
transforms your SQL expertise into automated data pipelines, scheduled ETL jobs, and
analytical applications. The first day is PURELY about your workbench — getting every tool
installed, wired together, and version-controlled so that every subsequent day has a clean
and reproducible foundation.

We use **DVD Rental** (Pagila) as our working database throughout the 90 days. It mirrors
real-world e-commerce data (customers, rentals, payments, inventory, staff) and provides
rich ground for ETL, analysis, and ML projects.

**Hardening note:** PostgreSQL will be configured with a dedicated `appuser` account
(non-superuser) that our Python application will ALWAYS use. This mirrors real production
practice and prevents privilege escalation bugs from day one.

---

## Program Structure
- 13 Sprints × 7 Days
- ~2 Hours/Day
- Database: PostgreSQL 15 (DVD Rental / Pagila)
- Stack: Python 3.11, psycopg2, SQLAlchemy, Pandas, Airflow, Streamlit

## Progress Tracker
| Sprint | Days  | Status  |
|--------|-------|---------|
| 01     | 1–7   | 🔄 In Progress |
| 02     | 8–14  | ⏳ Pending |
...

# 📅 DAY 01 — Sprint 01 | Environment Setup
## Python Install + VS Code + Git Init + PostgreSQL Install
## 🎯 OBJECTIVES

1. Install Python 3.11+ and configure virtual environment tooling
2. Install and configure VS Code with Python extensions
3. Initialise Git repo with branching strategy
4. Install PostgreSQL 15 locally
5. Create `appuser` with hardened permissions
6. Set up daily log + git push automation script skeleton
7. Verify all tools are communicating correctly

---

## ⏱️ TIME BUDGET (2 hrs)

| Block    | Duration | Activity                        |
|----------|----------|---------------------------------|
| Block A  | 30 min   | Python + VS Code setup          |
| Block B  | 20 min   | Git repo initialisation         |
| Block C  | 35 min   | PostgreSQL install + appuser    |
| Block D  | 15 min   | Verify setup script             |
| Block E  | 20 min   | Daily log + first git push      |

---

---

## 🗂️ JIRA CARD

| Field           | Value                                                  |
|-----------------|--------------------------------------------------------|
| Epic            | EP-01: Environment & Foundations                       |
| Story           | ST-01: Setup Complete Python Dev Environment           |
| Task ID         | TASK-001                                               |
| Sprint          | Sprint 01 (Days 1–7)                                   |
| Story Points    | 3                                                      |
| Priority        | CRITICAL                                               |
| Assignee        | [Alok Srivastava]                                            |
| Labels          | setup, python, git, postgresql, day-01                 |
| Status          | In Progress                                            |
| Acceptance Criteria | Python 3.11+ running, VS Code configured, Git repo live, PostgreSQL installed and accessible |

---

## 📁 GIT REPO DETAIL

| Field           | Value                                                  |
|-----------------|--------------------------------------------------------|
| Repo Name       | `python-de-journey`                                    |
| Branch          | `sprint-01/day-01-env-setup`                           |
| Base Branch     | `develop`                                              |
| Commit Prefix   | `[DAY-01]`                                             |
| Folder          | `sprint-01/day-01/`                                    |
| Files to Push   | `setup_python.sh/ps1`, `setup_postgresql.sh/ps1`, `requirements.txt`, `verify_setup.py`, `day01_log.md` |

---


## Quick Start
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python verify_setup.py
```

