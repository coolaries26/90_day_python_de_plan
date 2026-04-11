#!/usr/bin/env bash
# =============================================================================
# setup_git.sh — Day 01 Sprint 01
# Git repo init, branching strategy, SSH key setup, daily push workflow
# Usage: bash setup_git.sh [--github-user YOUR_GITHUB_USERNAME]
# =============================================================================

set -euo pipefail
LOG_FILE="setup_git.log"

log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
fail() { log "ERROR: $*"; exit 1; }
hr()   { log "------------------------------------------------------"; }

log "======================================================"
log "  Git Setup — Python DE Journey"
log "======================================================"

# -------------------------------------------------------
# 0. Parse args
# -------------------------------------------------------
GITHUB_USER=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --github-user) GITHUB_USER="$2"; shift 2 ;;
        *) shift ;;
    esac
done

# -------------------------------------------------------
# 1. Install Git
# -------------------------------------------------------
if ! command -v git &>/dev/null; then
    OS="$(uname -s)"
    if [ "$OS" = "Linux" ]; then
        sudo apt-get install -y git 2>&1 | tee -a "$LOG_FILE"
    elif [ "$OS" = "Darwin" ]; then
        brew install git 2>&1 | tee -a "$LOG_FILE"
    fi
fi

GIT_VER=$(git --version)
log "Git: $GIT_VER"

# -------------------------------------------------------
# 2. Configure Git globals
# -------------------------------------------------------
hr
log "Configuring Git globals..."

if [ -z "$(git config --global user.name 2>/dev/null || true)" ]; then
    read -rp "Enter your full name for Git commits: " GIT_NAME
    git config --global user.name "$GIT_NAME"
fi

if [ -z "$(git config --global user.email 2>/dev/null || true)" ]; then
    read -rp "Enter your email for Git commits: " GIT_EMAIL
    git config --global user.email "$GIT_EMAIL"
fi

git config --global core.autocrlf input          # consistent line endings
git config --global push.default current          # push current branch by default
git config --global pull.rebase false             # merge (not rebase) on pull
git config --global init.defaultBranch main       # default branch name

log "Git globals:"
log "  user.name  = $(git config --global user.name)"
log "  user.email = $(git config --global user.email)"

# -------------------------------------------------------
# 3. Generate SSH key for GitHub (if not exists)
# -------------------------------------------------------
hr
SSH_KEY_FILE="${HOME}/.ssh/id_ed25519_github"
if [ ! -f "$SSH_KEY_FILE" ]; then
    log "Generating SSH key for GitHub..."
    GIT_EMAIL=$(git config --global user.email)
    ssh-keygen -t ed25519 -C "$GIT_EMAIL" -f "$SSH_KEY_FILE" -N "" 2>&1 | tee -a "$LOG_FILE"
    eval "$(ssh-agent -s)"
    ssh-add "$SSH_KEY_FILE"
    log "SSH key generated: $SSH_KEY_FILE"
    log ""
    log "🔑 Add this public key to GitHub → Settings → SSH Keys:"
    log "------------------------------------------------------------"
    cat "${SSH_KEY_FILE}.pub"
    log "------------------------------------------------------------"
    log "GitHub URL: https://github.com/settings/ssh/new"
else
    log "SSH key already exists: $SSH_KEY_FILE"
fi

# -------------------------------------------------------
# 4. Initialise project repository
# -------------------------------------------------------
hr
PROJECT_DIR="${HOME}/python-de-journey"
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"
log "Working in: $PROJECT_DIR"

if [ ! -d ".git" ]; then
    log "Initialising git repository..."
    git init
    git branch -M main
    log "Repository initialised"
else
    log "Git repository already initialised"
fi

# -------------------------------------------------------
# 5. Create .gitignore
# -------------------------------------------------------
cat > .gitignore << 'GITEOF'
# ============================================================
# .gitignore — Python DE Journey
# ============================================================

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
*.egg
*.egg-info/
dist/
build/
.eggs/

# Environments
.venv/
venv/
env/
ENV/
env.bak/
venv.bak/

# Credentials — NEVER COMMIT THESE
.env
.env.*
!.env.example
secrets/
*.pem
*.key
db_config.ini
config.ini

# IDEs
.vscode/settings.json
.idea/
*.iml
*.sublime-project
*.sublime-workspace

# Logs
*.log
logs/
!logs/.gitkeep

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Jupyter
.ipynb_checkpoints/
*.ipynb

# Data files (large)
*.csv
*.parquet
*.feather
data/raw/
data/processed/
!data/.gitkeep

# pytest
.pytest_cache/
.coverage
htmlcov/

# mypy
.mypy_cache/
.dmypy.json

# Airflow
airflow/logs/
airflow/dags/__pycache__/
GITEOF

log ".gitignore created"

# -------------------------------------------------------
# 6. Create project folder structure
# -------------------------------------------------------
hr
log "Creating project folder structure..."

mkdir -p \
    sprint-01/{day-01,day-02,day-03,day-04,day-05,day-06,day-07} \
    sprint-02 sprint-03 sprint-04 sprint-05 sprint-06 \
    sprint-07 sprint-08 sprint-09 sprint-10 sprint-11 \
    sprint-12 sprint-13 \
    scripts \
    logs \
    data \
    tests \
    docs

touch logs/.gitkeep
touch data/.gitkeep

# Create .env.example (safe to commit — no real credentials)
cat > .env.example << 'ENVEOF'
# Copy this to .env and fill in real values
# NEVER commit .env to git
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=dvdrental
DB_USER=appuser
DB_PASSWORD=your_password_here
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=2
DB_POOL_RECYCLE=1800
APP_ENV=development
LOG_LEVEL=INFO
ENVEOF

log "Folder structure created"
log "$(find . -maxdepth 2 -type d | sort | grep -v '.git')"

# -------------------------------------------------------
# 7. Initial commit on main
# -------------------------------------------------------
hr
log "Creating initial commit on main..."

git add .
git commit -m "[INIT] Project scaffold: folder structure, .gitignore, .env.example" \
    2>&1 | tee -a "$LOG_FILE" || log "Nothing to commit (already initialised)"

# -------------------------------------------------------
# 8. Create develop branch
# -------------------------------------------------------
hr
log "Creating develop branch..."

git checkout -b develop 2>/dev/null || git checkout develop
log "On branch: $(git branch --show-current)"

# -------------------------------------------------------
# 9. Connect to remote (GitHub)
# -------------------------------------------------------
hr
if [ -n "$GITHUB_USER" ]; then
    REMOTE_URL="git@github.com:${GITHUB_USER}/python-de-journey.git"
    if ! git remote get-url origin &>/dev/null; then
        log "Adding remote origin: $REMOTE_URL"
        git remote add origin "$REMOTE_URL"
    else
        log "Remote origin already set: $(git remote get-url origin)"
    fi

    log "Pushing main and develop to remote..."
    git push -u origin main    2>&1 | tee -a "$LOG_FILE" || log "Push failed — check SSH key is added to GitHub"
    git push -u origin develop 2>&1 | tee -a "$LOG_FILE" || true
else
    log "No --github-user provided. To add remote manually:"
    log "  git remote add origin git@github.com:YOUR_USERNAME/python-de-journey.git"
    log "  git push -u origin main"
    log "  git push -u origin develop"
fi

# -------------------------------------------------------
# 10. Create Day 01 feature branch
# -------------------------------------------------------
hr
log "Creating Day 01 feature branch..."

git checkout develop
git checkout -b sprint-01/day-01-env-setup 2>/dev/null || \
    git checkout sprint-01/day-01-env-setup

log "Current branch: $(git branch --show-current)"

# -------------------------------------------------------
# 11. Write daily push helper script
# -------------------------------------------------------
hr
cat > scripts/git_daily_push.sh << 'PUSHEOF'
#!/usr/bin/env bash
# =============================================================================
# git_daily_push.sh — Daily Git Commit & Push Workflow
# Usage: bash scripts/git_daily_push.sh --day 1 --msg "Completed env setup"
# =============================================================================
set -euo pipefail

DAY=""
MSG=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --day) DAY="$2"; shift 2 ;;
        --msg) MSG="$2"; shift 2 ;;
        *) shift ;;
    esac
done

[ -z "$DAY" ] && { echo "Usage: $0 --day N --msg 'message'"; exit 1; }
[ -z "$MSG" ] && MSG="Daily progress update"

BRANCH=$(git branch --show-current)
COMMIT_MSG="[DAY-$(printf '%03d' "$DAY")] $MSG"

echo "Branch:  $BRANCH"
echo "Commit:  $COMMIT_MSG"
echo ""

git add .
git commit -m "$COMMIT_MSG" || echo "Nothing to commit"
git push -u origin "$BRANCH" && echo "✅ Pushed to $BRANCH" || echo "⚠️  Push failed"
PUSHEOF

chmod +x scripts/git_daily_push.sh
log "scripts/git_daily_push.sh created"

# -------------------------------------------------------
# 12. Summary
# -------------------------------------------------------
log "======================================================"
log "  Git Setup Complete"
log "======================================================"
log "Repo:    $PROJECT_DIR"
log "Branch:  $(git branch --show-current)"
log "Remote:  $(git remote get-url origin 2>/dev/null || echo 'Not set')"
log ""
log "DAILY WORKFLOW:"
log "  1. git checkout sprint-XX/day-YY-topic"
log "  2. Do your work"
log "  3. bash scripts/git_daily_push.sh --day N --msg 'What you did'"
log "  4. Open PR to develop on GitHub"
log "======================================================"
