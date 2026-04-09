# 📅 DAY 01 — Sprint 01 | Environment Setup
## Python Install + VS Code + Git Init + PostgreSQL Install

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
| Assignee        | [Your Name]                                            |
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
| Files to Push   | `setup_python.sh`, `setup_postgresql.sh`, `requirements.txt`, `verify_setup.py`, `day01_log.md` |

---

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

## 📝 EXERCISES

---

### EXERCISE 1 — Install Python 3.11+ (Block A)

**Objective:** Install Python, verify version, understand pyenv for version management.

#### Ubuntu/Debian (WSL2 or native Linux):
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3.11-dev python3-pip build-essential libpq-dev

# Verify
python3.11 --version
pip3 --version

# Set python3.11 as default python3 (optional)
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
sudo update-alternatives --config python3
```

#### macOS (Homebrew):
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew install python@3.11
echo 'export PATH="/usr/local/opt/python@3.11/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
python3.11 --version
```

#### Windows (native):
```powershell
# Download Python 3.11 installer from https://python.org
# During install: CHECK "Add Python to PATH" and "Install pip"
# Then in PowerShell:
python --version
pip --version
```

**✅ Checkpoint:** `python3.11 --version` → `Python 3.11.x`

---

### EXERCISE 2 — Install VS Code + Extensions (Block A)

**Objective:** Configure a professional Python IDE.

```bash
# Ubuntu
sudo snap install --classic code

# macOS
brew install --cask visual-studio-code

# Verify
code --version
```

**Install these VS Code extensions (paste in terminal):**
```bash
code --install-extension ms-python.python
code --install-extension ms-python.vscode-pylance
code --install-extension ms-python.black-formatter
code --install-extension ms-toolsai.jupyter
code --install-extension mtxr.sqltools
code --install-extension mtxr.sqltools-driver-pg
code --install-extension eamodio.gitlens
code --install-extension christian-kohler.path-intellisense
```

**Create VS Code workspace settings:**
```bash
mkdir -p ~/python-de-journey/.vscode
cat > ~/python-de-journey/.vscode/settings.json << 'EOF'
{
    "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
    "editor.formatOnSave": true,
    "[python]": {
        "editor.defaultFormatter": "ms-python.black-formatter"
    },
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "files.trimTrailingWhitespace": true,
    "editor.rulers": [88],
    "python.analysis.typeCheckingMode": "basic"
}
EOF
```

**✅ Checkpoint:** Open VS Code, open command palette (Ctrl+Shift+P), type "Python: Select Interpreter" — your venv should appear.

---

### EXERCISE 3 — Git Repo Initialisation (Block B)

**Objective:** Create a structured Git repository that will hold all 90 days of work.

```bash
# 1. Create the project directory
mkdir -p ~/python-de-journey
cd ~/python-de-journey

# 2. Initialise git
git init
git branch -M main

# 3. Create .gitignore
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
.venv/
venv/
env/
*.egg-info/
dist/
build/
.eggs/

# Environment
.env
.env.local
*.env

# IDE
.vscode/settings.json
.idea/
*.iml

# Logs
*.log
logs/

# DB credentials
db_config.ini
secrets/

# OS
.DS_Store
Thumbs.db
EOF

# 4. Create top-level README
cat > README.md << 'EOF'
# 🐍 Python Data Engineering Journey — 90 Days

**Learner:** [Your Name]
**Start Date:** [Today's Date]
**Target Role:** Python Data Engineer / Data Scientist

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

## Quick Start
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python verify_setup.py
```
EOF

# 5. Create folder structure for sprints
mkdir -p sprint-01/{day-01,day-02,day-03,day-04,day-05,day-06,day-07}
mkdir -p logs
mkdir -p scripts
touch logs/.gitkeep

# 6. First commit on main
git add .
git commit -m "[INIT] Project structure, .gitignore, README"

# 7. Create develop branch
git checkout -b develop
git push origin develop  # Only if remote is set (see step below)
```

**Create remote on GitHub/GitLab:**
```bash
# GitHub CLI (if installed):
gh repo create python-de-journey --private --source=. --remote=origin --push

# OR manually:
# 1. Go to github.com → New Repository → python-de-journey (private)
# 2. Then:
git remote add origin https://github.com/YOUR_USERNAME/python-de-journey.git
git push -u origin main
git push -u origin develop
```

**Create Day 01 branch:**
```bash
git checkout develop
git checkout -b sprint-01/day-01-env-setup
```

**✅ Checkpoint:** `git log --oneline` shows initial commit; `git branch` shows `sprint-01/day-01-env-setup` as current.

---

### EXERCISE 4 — Install PostgreSQL 15 (Block C)

**Objective:** Install PostgreSQL locally, harden it for application use.

#### Ubuntu/Debian:
```bash
sudo apt install -y postgresql postgresql-contrib
sudo systemctl start postgresql
sudo systemctl enable postgresql
sudo systemctl status postgresql

# Verify
psql --version
```

#### macOS:
```bash
brew install postgresql@15
brew services start postgresql@15
echo 'export PATH="/usr/local/opt/postgresql@15/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
psql --version
```

#### Windows:
```
Download PostgreSQL 15 installer from https://postgresql.org/download/windows/
Install with: PostgreSQL Server, pgAdmin 4, Command Line Tools
Default port: 5432
```

**✅ Checkpoint:** `psql --version` → `psql (PostgreSQL) 15.x`

---

### EXERCISE 5 — Create appuser + dvdrental Database (Block C)

**Objective:** Set up a hardened `appuser` account and create the working database.

```bash
# Connect as postgres superuser
sudo -u postgres psql
```

**Inside psql, run:**
```sql
-- ============================================================
-- STEP 1: Create application user (non-superuser, no createdb)
-- ============================================================
CREATE USER appuser WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOINHERIT
    CONNECTION LIMIT 20
    PASSWORD 'AppUser@2024!';  -- Change this to a strong password

-- ============================================================
-- STEP 2: Create the dvdrental database
-- ============================================================
CREATE DATABASE dvdrental
    WITH OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TEMPLATE = template0
    CONNECTION LIMIT = 50;

-- ============================================================
-- STEP 3: Grant appuser access to dvdrental
-- ============================================================
GRANT CONNECT ON DATABASE dvdrental TO appuser;

-- Connect to dvdrental to set schema permissions
\c dvdrental

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO appuser;

-- Grant table-level DML permissions (NOT DDL)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO appuser;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO appuser;

-- Ensure future tables also get permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO appuser;

-- ============================================================
-- STEP 4: Harden connection settings
-- ============================================================
ALTER ROLE appuser SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE appuser SET statement_timeout = '60s';
ALTER ROLE appuser SET lock_timeout = '10s';

-- Verify
\du appuser
\q
```

**Configure pg_hba.conf for local connections:**
```bash
# Find pg_hba.conf location
sudo -u postgres psql -c "SHOW hba_file;"

# Edit it (path varies, typically):
sudo nano /etc/postgresql/15/main/pg_hba.conf

# Add/verify this line exists for local connections:
# TYPE  DATABASE    USER      ADDRESS     METHOD
# host  dvdrental   appuser   127.0.0.1/32  scram-sha-256

# Reload PostgreSQL
sudo systemctl reload postgresql
```

**Configure postgresql.conf hardening:**
```bash
sudo nano /etc/postgresql/15/main/postgresql.conf

# Ensure/set these values:
# max_connections = 100
# shared_buffers = 256MB
# effective_cache_size = 512MB
# idle_in_transaction_session_timeout = 30000   # 30 sec
# statement_timeout = 60000                     # 60 sec
# log_min_duration_statement = 1000             # log slow queries > 1s
# log_connections = on
# log_disconnections = on

sudo systemctl restart postgresql
```

**Test appuser connection:**
```bash
psql -h 127.0.0.1 -U appuser -d dvdrental -W
# Enter password: AppUser@2024!
# \conninfo  → should show appuser connected to dvdrental
# \q
```

**✅ Checkpoint:** `appuser` can connect to `dvdrental` on `127.0.0.1`, cannot create databases.

---

### EXERCISE 6 — Create .env File + Python verify_setup.py (Block D)

**Objective:** Centralise credentials, verify the full stack is wired together.

**Create .env (NEVER commit this to git):**
```bash
cat > ~/python-de-journey/.env << 'EOF'
# PostgreSQL
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=dvdrental
DB_USER=appuser
DB_PASSWORD=AppUser@2024!
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=2
DB_POOL_RECYCLE=1800

# App
APP_ENV=development
LOG_LEVEL=INFO
EOF

echo ".env" >> .gitignore
```

**Create requirements.txt:**
```bash
cat > ~/python-de-journey/requirements.txt << 'EOF'
# Database
psycopg2-binary==2.9.9
SQLAlchemy==2.0.23
alembic==1.13.0

# Data Engineering
pandas==2.1.4
numpy==1.26.2
pyarrow==14.0.2

# Configuration & Utilities
python-dotenv==1.0.0
pyyaml==6.0.1
click==8.1.7

# Code Quality
black==23.12.0
pylint==3.0.3
pytest==7.4.3
pytest-cov==4.1.0

# Logging
loguru==0.7.2

# Data Profiling (Sprint 06)
# ydata-profiling==4.6.3

# Visualization (Sprint 10)
# matplotlib==3.8.2
# seaborn==0.13.0
# plotly==5.18.0
# streamlit==1.29.0

# ML (Sprint 12)
# scikit-learn==1.3.2
# joblib==1.3.2
EOF
```

**Install dependencies in virtual environment:**
```bash
cd ~/python-de-journey
python3.11 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

pip install --upgrade pip
pip install -r requirements.txt
```

**Create verify_setup.py:**
```python
#!/usr/bin/env python3
"""
verify_setup.py — Day 01 Environment Verification
Confirms Python, psycopg2, SQLAlchemy, and DB connection are all functional.
"""

import sys
import os
import importlib
from dotenv import load_dotenv

load_dotenv()

REQUIRED_PACKAGES = [
    "psycopg2",
    "sqlalchemy",
    "pandas",
    "numpy",
    "dotenv",
    "loguru",
    "yaml",
    "click",
]

def check_python_version():
    version = sys.version_info
    assert version.major == 3 and version.minor >= 11, (
        f"❌ Python 3.11+ required, found {version.major}.{version.minor}"
    )
    print(f"  ✅ Python {version.major}.{version.minor}.{version.micro}")

def check_packages():
    for pkg in REQUIRED_PACKAGES:
        try:
            importlib.import_module(pkg)
            print(f"  ✅ {pkg}")
        except ImportError:
            print(f"  ❌ {pkg} — run: pip install {pkg}")

def check_db_connection():
    import psycopg2
    from psycopg2 import pool as pg_pool

    db_config = {
        "host":     os.getenv("DB_HOST", "127.0.0.1"),
        "port":     int(os.getenv("DB_PORT", 5432)),
        "dbname":   os.getenv("DB_NAME", "dvdrental"),
        "user":     os.getenv("DB_USER", "appuser"),
        "password": os.getenv("DB_PASSWORD"),
        "connect_timeout": 5,
    }

    # Use a minimal connection pool (context manager ensures clean close)
    connection_pool = None
    try:
        connection_pool = pg_pool.SimpleConnectionPool(
            minconn=1, maxconn=2, **db_config
        )
        conn = connection_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            pg_version = cur.fetchone()[0]
            cur.execute("SELECT current_user, current_database();")
            user, dbname = cur.fetchone()
        connection_pool.putconn(conn)  # return to pool — no leak
        print(f"  ✅ PostgreSQL connected | user={user} db={dbname}")
        print(f"     {pg_version[:60]}...")
    except Exception as exc:
        print(f"  ❌ DB connection failed: {exc}")
    finally:
        if connection_pool:
            connection_pool.closeall()  # always close pool

def check_sqlalchemy_connection():
    from sqlalchemy import create_engine, text

    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )
    engine = create_engine(
        db_url,
        pool_size=2,
        max_overflow=1,
        pool_pre_ping=True,       # detect stale connections
        pool_recycle=1800,        # recycle connections every 30 min
        echo=False,
    )
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables"))
            table_count = result.scalar()
        print(f"  ✅ SQLAlchemy engine OK | tables visible: {table_count}")
    except Exception as exc:
        print(f"  ❌ SQLAlchemy failed: {exc}")
    finally:
        engine.dispose()  # close all pool connections — no memory leak

def main():
    print("\n" + "="*55)
    print("  DAY 01 — Environment Verification")
    print("="*55)

    print("\n[1] Python Version")
    check_python_version()

    print("\n[2] Required Packages")
    check_packages()

    print("\n[3] PostgreSQL Direct Connection (psycopg2)")
    check_db_connection()

    print("\n[4] SQLAlchemy ORM Connection")
    check_sqlalchemy_connection()

    print("\n" + "="*55)
    print("  Verification complete. Review any ❌ above.")
    print("="*55 + "\n")

if __name__ == "__main__":
    main()
```

**✅ Checkpoint:** Running `python verify_setup.py` shows all green checkmarks.

---

### EXERCISE 7 — Daily Log + Git Push Automation (Block E)

**Objective:** Build the daily ritual: log your work, push to git. This script grows each sprint.

**Create scripts/daily_log.py:**
```python
#!/usr/bin/env python3
"""
daily_log.py — Daily Progress Logger + Git Pusher
Usage: python scripts/daily_log.py --day 1 --message "Completed env setup"
"""

import subprocess
import sys
import os
import argparse
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

def write_log_entry(day: int, message: str, status: str) -> Path:
    """Append entry to logs/progress.md — create if missing."""
    log_file = PROJECT_ROOT / "logs" / "progress.md"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    header_needed = not log_file.exists() or log_file.stat().st_size == 0
    with open(log_file, "a") as f:
        if header_needed:
            f.write("# 📓 90-Day Learning Progress Log\n\n")
            f.write("| Day | Date | Status | Notes |\n")
            f.write("|-----|------|--------|-------|\n")
        f.write(f"| {day:03d} | {timestamp} | {status} | {message} |\n")

    print(f"  ✅ Log written → {log_file}")
    return log_file

def git_add_commit_push(day: int, message: str):
    """Stage all changes, commit with day prefix, push to remote."""
    try:
        subprocess.run(["git", "-C", str(PROJECT_ROOT), "add", "."],
                       check=True, capture_output=True)

        commit_msg = f"[DAY-{day:03d}] {message}"
        subprocess.run(
            ["git", "-C", str(PROJECT_ROOT), "commit", "-m", commit_msg],
            check=True, capture_output=True
        )
        print(f"  ✅ Git commit: {commit_msg}")

        result = subprocess.run(
            ["git", "-C", str(PROJECT_ROOT), "push"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print("  ✅ Git push: success")
        else:
            print(f"  ⚠️  Git push warning: {result.stderr.strip()}")

    except subprocess.CalledProcessError as e:
        print(f"  ❌ Git error: {e.stderr.decode().strip()}")

def main():
    parser = argparse.ArgumentParser(description="Log daily progress and push to git")
    parser.add_argument("--day",     type=int, required=True, help="Day number (1-90)")
    parser.add_argument("--message", type=str, required=True, help="Progress note")
    parser.add_argument("--status",  type=str, default="✅ Done",
                        choices=["✅ Done", "🔄 In Progress", "⚠️ Blocked", "❌ Failed"])
    args = parser.parse_args()

    print(f"\n📋 Logging Day {args.day}...")
    write_log_entry(args.day, args.message, args.status)
    git_add_commit_push(args.day, args.message)
    print("  🎯 Daily log complete.\n")

if __name__ == "__main__":
    main()
```

**Run for Day 1:**
```bash
cd ~/python-de-journey
source .venv/bin/activate

python scripts/daily_log.py \
  --day 1 \
  --message "Environment setup: Python 3.11, VS Code, PostgreSQL 15, appuser, Git repo initialised" \
  --status "✅ Done"
```

---

## 📤 GIT PUSH STEPS FOR DAY 01

```bash
# Ensure you are on the correct branch
cd ~/python-de-journey
git checkout sprint-01/day-01-env-setup

# Stage all day 01 files
git add sprint-01/day-01/
git add scripts/daily_log.py
git add requirements.txt
git add verify_setup.py
git add .gitignore
git add README.md
git add logs/

# Commit
git commit -m "[DAY-001] Sprint 01 Day 1: Full env setup — Python, PostgreSQL, Git, appuser"

# Push branch to remote
git push -u origin sprint-01/day-01-env-setup

# (Optional) Open a Pull Request to develop on GitHub
# gh pr create --base develop --head sprint-01/day-01-env-setup \
#   --title "[Sprint 01] Day 1: Environment Setup" \
#   --body "Closes TASK-001 — Python + PostgreSQL + Git foundation complete"
```

---

## ✅ DAY 01 COMPLETION CHECKLIST

| # | Task                                              | Done? |
|---|---------------------------------------------------|-------|
| 1 | Python 3.11+ installed and accessible             | [ ]   |
| 2 | Virtual environment created and activated         | [ ]   |
| 3 | VS Code installed with all 8 extensions           | [ ]   |
| 4 | Git repo initialised with main + develop branches | [ ]   |
| 5 | Remote repo created (GitHub/GitLab)               | [ ]   |
| 6 | PostgreSQL 15 installed and running               | [ ]   |
| 7 | appuser created (no superuser privileges)         | [ ]   |
| 8 | dvdrental database created                        | [ ]   |
| 9 | pg_hba.conf configured for scram-sha-256          | [ ]   |
|10 | postgresql.conf hardened (timeouts, logging)      | [ ]   |
|11 | .env created and added to .gitignore              | [ ]   |
|12 | requirements.txt created + packages installed     | [ ]   |
|13 | verify_setup.py passes all checks                 | [ ]   |
|14 | daily_log.py created and executed                 | [ ]   |
|15 | Day 01 branch pushed to remote                    | [ ]   |

---

## 🔜 PREVIEW: DAY 02

**Topic:** DVD Rental / Pagila database population  
**What you'll do:** Download and restore the DVD Rental dump, explore schema via psql and Python, write your first SQL queries from Python, explore tables and row counts.

---

*Day 01 | Sprint 01 | EP-01 | TASK-001*
