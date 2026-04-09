# 🐍 90-Day Python & Data Engineering Learning Program
## DBA → Python Data Engineer / Data Scientist

---

## 📋 PROGRAM METADATA

| Field         | Value                                         |
|---------------|-----------------------------------------------|
| Duration      | 90 Days (13 Sprints)                          |
| Daily Effort  | ~2 Hours                                      |
| Learner Role  | Database Administrator (DBA)                  |
| Target Role   | Python Data Engineer / Data Scientist         |
| DB Engine     | PostgreSQL (local, appuser)                   |
| Sample DB     | DVD Rental (Pagila)                           |
| Version Ctrl  | Git (GitHub/GitLab)                           |
| Task Tracker  | JIRA (auto-logged via Python script)          |

---

## 🧱 EPIC BREAKDOWN

| Epic ID | Epic Name                          | Sprints     | Days      |
|---------|------------------------------------|-------------|-----------|
| EP-01   | Environment & Foundations          | S01         | 1–7       |
| EP-02   | Python Core for DBAs               | S02–S03     | 8–21      |
| EP-03   | DB Connectivity & ORM              | S04         | 22–28     |
| EP-04   | Data Wrangling with Pandas         | S05–S06     | 29–42     |
| EP-05   | ETL Pipeline Engineering           | S07–S08     | 43–56     |
| EP-06   | Workflow Orchestration             | S09         | 57–63     |
| EP-07   | Data Visualization & Reporting     | S10         | 64–70     |
| EP-08   | Advanced Python & OOP              | S11         | 71–77     |
| EP-09   | ML Foundations & Scikit-learn      | S12         | 78–84     |
| EP-10   | Capstone & Job Readiness           | S13         | 85–90     |

---

## 📅 SPRINT PLAN

### SPRINT 01 — Environment Setup (Days 1–7) [EP-01]
**Goal:** Fully operational Python + PostgreSQL + Git dev environment

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 1   | Python install, venv, pip, IDE (VS Code), Git init, PostgreSQL install |
| 2   | PostgreSQL: appuser, DVD Rental DB setup, psql CLI basics |
| 3   | Python libraries install (psycopg2, pandas, sqlalchemy, etc.) + verify |
| 4   | First Python-to-PostgreSQL connection script   |
| 5   | Git workflow: branch, commit, push, pull, PR basics |
| 6   | Python logging module + daily log automation script |
| 7   | **Sprint 01 Test** — end-to-end env check + Git audit |

### SPRINT 02 — Python Core I: Syntax & Control Flow (Days 8–14) [EP-02]
**Goal:** Master Python syntax with DBA-relevant examples

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 8   | Variables, data types, type casting, f-strings |
| 9   | Conditionals, loops (for/while), comprehensions |
| 10  | Functions: args, kwargs, defaults, docstrings  |
| 11  | Exception handling: try/except/finally, custom exceptions |
| 12  | File I/O: read/write CSV, JSON, text files     |
| 13  | Modules & packages: import, __init__, project structure |
| 14  | **Sprint 02 Test** — mini project: DB query result → CSV |

### SPRINT 03 — Python Core II: Collections & OOP Intro (Days 15–21) [EP-02]
**Goal:** Data structures + first OOP concepts applied to DB models

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 15  | Lists, tuples, sets — DB row manipulation     |
| 16  | Dictionaries — result set mapping              |
| 17  | Iterators, generators — large dataset handling |
| 18  | OOP: classes, __init__, methods, properties    |
| 19  | OOP: inheritance, polymorphism                 |
| 20  | Decorators, context managers                   |
| 21  | **Sprint 03 Test** — OOP model for DB schema entity |

### SPRINT 04 — DB Connectivity & ORM (Days 22–28) [EP-03]
**Goal:** Python ↔ PostgreSQL mastery using psycopg2 and SQLAlchemy

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 22  | psycopg2 deep dive: connections, cursors, transactions |
| 23  | Parameterized queries, SQL injection prevention |
| 24  | SQLAlchemy Core: engine, metadata, text()     |
| 25  | SQLAlchemy ORM: models, sessions, queries     |
| 26  | Alembic: schema migrations                    |
| 27  | Connection pooling, query optimization        |
| 28  | **Sprint 04 Test** — ORM CRUD app on DVD Rental DB |

### SPRINT 05 — Pandas I: Data Wrangling (Days 29–35) [EP-04]
**Goal:** Pandas fundamentals with DB data as source

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 29  | DataFrame creation: from DB, CSV, JSON        |
| 30  | Selection, filtering, loc/iloc                |
| 31  | GroupBy, aggregations, pivot tables           |
| 32  | Merge, join, concat — multi-table analysis   |
| 33  | Handling nulls, duplicates, data types        |
| 34  | Apply, map, lambda transformations            |
| 35  | **Sprint 05 Test** — DVD Rental revenue analysis |

### SPRINT 06 — Pandas II: EDA & Data Quality (Days 36–42) [EP-04]
**Goal:** Exploratory analysis and data profiling

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 36  | Descriptive stats, profiling with ydata-profiling |
| 37  | Datetime handling and time series basics      |
| 38  | String operations and regex in Pandas         |
| 39  | Multi-index DataFrames                        |
| 40  | Data quality checks: constraints, assertions  |
| 41  | Writing transformed data back to PostgreSQL   |
| 42  | **Sprint 06 Test** — automated data quality report |

### SPRINT 07 — ETL Engineering I (Days 43–49) [EP-05]
**Goal:** Build structured Extract-Transform-Load pipelines

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 43  | ETL concepts: source, staging, target         |
| 44  | Extract: DB, CSV, JSON, REST API              |
| 45  | Transform: cleaning, mapping, enrichment      |
| 46  | Load: bulk inserts, upsert patterns           |
| 47  | Error handling in pipelines, dead-letter queues |
| 48  | Logging and audit trails in ETL               |
| 49  | **Sprint 07 Test** — full ETL for DVD Rental analytics table |

### SPRINT 08 — ETL Engineering II: Modular Pipelines (Days 50–56) [EP-05]
**Goal:** Production-grade, reusable pipeline architecture

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 50  | Config-driven pipelines (YAML/JSON configs)   |
| 51  | CLI tools with argparse/click                 |
| 52  | Unit testing pipelines: pytest, fixtures      |
| 53  | Parallelism: threading, multiprocessing, concurrent.futures |
| 54  | Incremental/delta loads                       |
| 55  | Data lineage and metadata tracking            |
| 56  | **Sprint 08 Test** — parallel multi-source ETL pipeline |

### SPRINT 09 — Orchestration with Airflow (Days 57–63) [EP-06]
**Goal:** Schedule and monitor data pipelines

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 57  | Airflow install, setup, UI walkthrough        |
| 58  | DAGs, operators, task dependencies            |
| 59  | PostgresOperator, PythonOperator              |
| 60  | XCom, variables, connections                  |
| 61  | Sensors, branching, retries                   |
| 62  | Backfilling, SLAs, alerting                   |
| 63  | **Sprint 09 Test** — orchestrated DVD Rental nightly pipeline |

### SPRINT 10 — Visualization & Reporting (Days 64–70) [EP-07]
**Goal:** Turn pipeline output into actionable charts and dashboards

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 64  | Matplotlib: line, bar, scatter, subplots      |
| 65  | Seaborn: statistical plots, heatmaps          |
| 66  | Plotly: interactive charts                    |
| 67  | Streamlit: build a data dashboard             |
| 68  | Automated report generation (PDF/HTML)        |
| 69  | Dashboard connected to live PostgreSQL        |
| 70  | **Sprint 10 Test** — DVD Rental Streamlit dashboard |

### SPRINT 11 — Advanced Python & OOP Patterns (Days 71–77) [EP-08]
**Goal:** Write engineer-grade Python

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 71  | SOLID principles in Python                    |
| 72  | Design patterns: Factory, Singleton, Strategy |
| 73  | Abstract base classes, protocols, dataclasses |
| 74  | Type hints, mypy, pydantic validation         |
| 75  | Async Python: asyncio, aiopg                  |
| 76  | Packaging: pyproject.toml, setup.cfg, wheels  |
| 77  | **Sprint 11 Test** — typed, documented, packaged ETL module |

### SPRINT 12 — ML Foundations (Days 78–84) [EP-09]
**Goal:** Introductory ML on real DB data

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 78  | NumPy: arrays, linear algebra basics          |
| 79  | Scikit-learn: preprocessing, train/test split |
| 80  | Regression: LinearRegression on DVD Rental    |
| 81  | Classification: RandomForest on customer churn |
| 82  | Clustering: KMeans customer segmentation      |
| 83  | Model persistence: joblib, pickle             |
| 84  | **Sprint 12 Test** — end-to-end ML pipeline from DB to prediction |

### SPRINT 13 — Capstone & Job Readiness (Days 85–90) [EP-10]
**Goal:** Production project + interview prep

| Day | Focus                                         |
|-----|-----------------------------------------------|
| 85  | Capstone design: architecture, Git project    |
| 86  | Capstone build: ETL + Airflow DAG             |
| 87  | Capstone build: dashboard + ML model          |
| 88  | Code review: clean code, docstrings, README   |
| 89  | Portfolio: GitHub README, LinkedIn summary    |
| 90  | Final demo + self-assessment rubric           |

---

## 📦 PRODUCT BACKLOG (Top Items)

| ID     | Type    | Title                                          | Epic  | Priority |
|--------|---------|------------------------------------------------|-------|----------|
| PB-001 | Story   | Setup Python dev environment                   | EP-01 | CRITICAL |
| PB-002 | Story   | Install and configure PostgreSQL locally       | EP-01 | CRITICAL |
| PB-003 | Story   | Load DVD Rental sample database                | EP-01 | CRITICAL |
| PB-004 | Story   | Configure appuser with least privilege         | EP-01 | CRITICAL |
| PB-005 | Story   | Setup Git repo with branch strategy            | EP-01 | CRITICAL |
| PB-006 | Task    | Install Python libraries (requirements.txt)    | EP-01 | HIGH     |
| PB-007 | Task    | Daily git push automation script               | EP-01 | HIGH     |
| PB-008 | Feature | Python ↔ PostgreSQL connectivity layer        | EP-03 | HIGH     |
| PB-009 | Feature | Reusable ETL framework                         | EP-05 | HIGH     |
| PB-010 | Feature | Airflow pipeline for DVD Rental analytics      | EP-06 | HIGH     |
| PB-011 | Feature | Streamlit dashboard on PostgreSQL              | EP-07 | MEDIUM   |
| PB-012 | Feature | Customer churn ML pipeline                     | EP-09 | MEDIUM   |
| PB-013 | Feature | Capstone: end-to-end data product              | EP-10 | HIGH     |
| PB-014 | Task    | JIRA automation via Python Jira SDK            | EP-01 | MEDIUM   |
| PB-015 | Task    | Daily retrospective template + log script      | EP-01 | HIGH     |

---

## 🔁 GIT BRANCHING STRATEGY

```
main
├── develop
│   ├── sprint-01/day-01-env-setup
│   ├── sprint-01/day-02-postgresql-setup
│   ├── sprint-02/day-08-python-basics
│   └── ...
└── tags/sprint-XX-test-result
```

---

## ⚠️ KNOWN GAPS (Self-Check)

| Gap                            | Mitigation                                |
|--------------------------------|-------------------------------------------|
| No Docker coverage             | Added to Sprint 08 bonus tasks            |
| No Spark/Hadoop                | Out of scope; referenced in Day 90 roadmap|
| No dbt                         | Mentioned Day 56 as stretch goal          |
| No cloud (AWS/GCP/Azure)       | Referenced in Day 90 next-steps           |
| Memory leaks in psycopg2       | Connection pool + context managers enforced from Day 4 |

---

## 🛡️ POSTGRESQL HARDENING CHECKLIST

- [ ] appuser has CONNECT + SELECT/INSERT/UPDATE/DELETE only (no SUPERUSER)
- [ ] pg_hba.conf uses md5 (or scram-sha-256) for local connections
- [ ] max_connections tuned (default 100 → set pool_size ≤ 10 per app)
- [ ] idle_in_transaction_session_timeout = 30s
- [ ] statement_timeout = 60s
- [ ] All connections via context managers (no dangling cursors)
- [ ] SQLAlchemy pool_pre_ping=True (detect stale connections)
- [ ] Connection pool: pool_size=5, max_overflow=2, pool_recycle=1800

---

*Generated: Day 0 | Program Start*
*Next: See day_01/ folder for Sprint 01, Day 1 full plan*
