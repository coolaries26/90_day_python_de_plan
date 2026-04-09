# 🔁 Day 01 Retrospective Template
## Sprint 01 | Environment Setup

> Fill this out at the START of Day 02 (before beginning Day 02 work).
> Honest reflection drives faster improvement.

---

## ✅ What Went Well

- [✅] Python installed and working.
- [✅] VS Code configured with extensions.
- [✅] PostgreSQL running.
- [✅] appuser created with correct privileges.
- [✅] DVD Rental database populated.
- [✅] verify_setup.py passing all checks.
- [✅] First git push successful.

_Write notes here:_



---

## ❌ What Didn't Go Well / Blockers

| Issue | Root Cause | Resolution |
|-------|------------|------------|
|script didn't work at firs|shell script not working on windows | converted .sh to ps1 |            |
| git gh was not found|gh was not installed|install gh using choco install gh in powershell" |

---

## 📚 What I Learned Today

1. setting up gh auth 
2. learning pgadmin 
3. debug using copilot

---

## ⏱️ Time Tracking

| Block  | Estimated | Actual | Variance |
|--------|-----------|--------|----------|
| A (Python + VS Code) | 30 min | 30 min | - |
| B (Git setup)        | 20 min | 1 hr| 40 min |
| C (PostgreSQL)       | 35 min | 2 hr | 90 min|
| D (Verify)           | 15 min | 2 hr| 115min|
| E (Log + Push)       | 20 min | 1 hr| 40min|
| **Total**            | **2 hr** | *6.5 hr*| **4.5 hr** |

---

## 🎯 Carry-Forward to Day 02

- [ ] Any failed verify_setup.py checks to fix?
- [ ] Any git issues unresolved?
- [ ] Password / .env settings to update?

---

## 💡 Feedback Request for Instructor/AI

_Paste your verify_setup.py output here for review:_

```
======================================================
  DAY 01 — Full Stack Environment Verification
  python-de-journey | Sprint 01
======================================================

──────────────────────────────────────────────────────
  1 · Python Version
──────────────────────────────────────────────────────
  ✅ Python 3.12.4
  ✅ 64-bit interpreter

──────────────────────────────────────────────────────
  2 · Required Packages
──────────────────────────────────────────────────────
  ✅ psycopg2-binary  →  2.9.9 (dt dec pq3 ext lo64)
  ✅ SQLAlchemy  →  2.0.23
  ✅ pandas  →  2.1.4
  ✅ numpy  →  1.26.2
  ✅ python-dotenv  →  n/a
  ✅ loguru  →  0.7.2
  ✅ pyyaml  →  6.0.1
  ✅ click  →  8.1.7

──────────────────────────────────────────────────────
  3 · Environment Variables (.env)
──────────────────────────────────────────────────────
  📂 .env loaded from: C:\Users\Lenovo\python-de-journey\sprint-01\day-01\.env

  ✅ $DB_HOST  →  127.0.0.1
  ✅ $DB_PORT  →  5432
  ✅ $DB_NAME  →  dvdrental
  ✅ $DB_USER  →  appuser
  ✅ $DB_PASSWORD  →  ***

──────────────────────────────────────────────────────
  4 · PostgreSQL Connection (psycopg2 + Connection Pool)
──────────────────────────────────────────────────────
  ✅ psycopg2 connection  →  user=appuser db=dvdrental
  ✅ PostgreSQL reachable  →  PostgreSQL 17.0 on x86_64-windows, compiled by msv
  ✅ Public tables visible  →  22 tables
  ✅ appuser is NOT superuser  →  SECURITY PASS

──────────────────────────────────────────────────────
  5 · SQLAlchemy Engine (ORM Layer)
──────────────────────────────────────────────────────
  ✅ SQLAlchemy engine  →  db=dvdrental
  ✅ Introspect tables  →  15 tables found
  ✅ pool_pre_ping enabled  →  stale-connection protection ON
  ✅ pool_recycle=1800  →  connection recycled every 30 min

     📋 First 5 tables: actor, film, store, address, category

──────────────────────────────────────────────────────
  6 · DVD Rental Data Integrity
──────────────────────────────────────────────────────
  ✅ Table: film  →  1,000 rows
  ✅ Table: actor  →  200 rows
  ✅ Table: customer  →  599 rows
  ✅ Table: rental  →  16,044 rows
  ✅ Table: payment  →  14,596 rows
  ✅ Table: inventory  →  4,581 rows
  ✅ Table: store  →  2 rows
  ✅ Table: staff  →  2 rows
  ✅ Table: category  →  16 rows
  ✅ Table: language  →  6 rows

──────────────────────────────────────────────────────
  7 · Git Repository
──────────────────────────────────────────────────────
  ✅ Git repository initialised
  ✅ Current branch  →  sprint-01/day-01-env-setup
  ✅ Remote origin set  →  git@github.com:testuser/python-de-journey.git

======================================================
  RESULT:  36/36 checks passed  (100%)

  🎉 All checks passed — environment is ready!
======================================================
```
**git log -5 output**
```
a3f5297 (HEAD -> main, 90_day_python_de_plan.git/main) [DAY-001][S01] Completed env setup
e853d44 [DAY-001][S01] Completed env setup
abe0b28 [DAY-001][S01] Completed env setup
d1a8793 [DAY-001][S01] Completed env setup
```
$ python scripts/daily_log.py --day 1 --message "Completed env setup"

──────────────────────────────────────────────────
  📅 Day 001 | Sprint 01
  📝 Completed env setup
  📌 ✅ Done
──────────────────────────────────────────────────

  ✅ Day folder ready → sprint-01\day-01
  ✅ Log entry written → logs\progress.md
  ✅ Committed: [DAY-001][S01] Completed env setup
  ✅ Pushed to remote

  📋 Git Status:
     Branch: main
     Recent commits:
       a3f5297 [DAY-001][S01] Completed env setup
       e853d44 [DAY-001][S01] Completed env setup
       abe0b28 [DAY-001][S01] Completed env setup

  🎯 Day 001 logged successfully.

```

---
*Retrospective: Day 01 | Sprint 01*
