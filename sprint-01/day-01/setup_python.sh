#!/usr/bin/env bash
# =============================================================================
# setup_python.sh — Day 01 Sprint 01
# Python 3.11 + venv + VS Code + required extensions setup
# Usage: bash setup_python.sh
# =============================================================================

set -euo pipefail
LOG_FILE="setup_python.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
fail() { log "ERROR: $*"; exit 1; }

log "======================================================"
log "  Python DE Journey — Environment Setup Script"
log "======================================================"

# -------------------------------------------------------
# 1. Detect OS
# -------------------------------------------------------
OS="$(uname -s)"
log "Detected OS: $OS"

install_python_linux() {
    log "Installing Python 3.11 on Linux..."
    sudo apt-get update -qq
    sudo apt-get install -y \
        software-properties-common \
        build-essential \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        python3.11 \
        python3.11-venv \
        python3.11-dev \
        python3-pip \
        git \
        curl \
        wget \
        2>&1 | tee -a "$LOG_FILE"

    # Set python3.11 as default
    sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
        2>/dev/null || true
    log "Python 3.11 installed on Linux"
}

install_python_mac() {
    log "Installing Python 3.11 on macOS..."
    if ! command -v brew &>/dev/null; then
        log "Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    brew install python@3.11 git 2>&1 | tee -a "$LOG_FILE"
    brew link --overwrite python@3.11
    log "Python 3.11 installed on macOS"
}

case "$OS" in
    Linux*)  install_python_linux ;;
    Darwin*) install_python_mac   ;;
    *)       fail "Unsupported OS: $OS. Please install Python 3.11 manually." ;;
esac

# -------------------------------------------------------
# 2. Verify Python version
# -------------------------------------------------------
PY_BIN=$(command -v python3.11 || command -v python3 || fail "python3 not found in PATH")
PY_VER=$("$PY_BIN" --version 2>&1)
log "Python binary: $PY_BIN"
log "Python version: $PY_VER"

# -------------------------------------------------------
# 3. Create project directory
# -------------------------------------------------------
PROJECT_DIR="${HOME}/python-de-journey"
log "Project directory: $PROJECT_DIR"
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

# -------------------------------------------------------
# 4. Create virtual environment
# -------------------------------------------------------
if [ ! -d ".venv" ]; then
    log "Creating virtual environment..."
    "$PY_BIN" -m venv .venv
    log "Virtual environment created at .venv/"
else
    log "Virtual environment already exists, skipping."
fi

# Activate
# shellcheck disable=SC1091
source .venv/bin/activate
log "Virtual environment activated"

# -------------------------------------------------------
# 5. Upgrade pip and core tools
# -------------------------------------------------------
log "Upgrading pip, setuptools, wheel..."
pip install --upgrade pip setuptools wheel 2>&1 | tee -a "$LOG_FILE"

# -------------------------------------------------------
# 6. Install project dependencies
# -------------------------------------------------------
if [ -f "requirements.txt" ]; then
    log "Installing from requirements.txt..."
    pip install -r requirements.txt 2>&1 | tee -a "$LOG_FILE"
else
    log "requirements.txt not found — installing core packages only..."
    pip install \
        psycopg2-binary==2.9.9 \
        SQLAlchemy==2.0.23 \
        alembic==1.13.0 \
        pandas==2.1.4 \
        numpy==1.26.2 \
        python-dotenv==1.0.0 \
        pyyaml==6.0.1 \
        click==8.1.7 \
        black==23.12.0 \
        pylint==3.0.3 \
        pytest==7.4.3 \
        pytest-cov==4.1.0 \
        loguru==0.7.2 \
        2>&1 | tee -a "$LOG_FILE"

    # Generate requirements.txt from what was installed
    pip freeze > requirements.txt
    log "requirements.txt generated from installed packages"
fi

# -------------------------------------------------------
# 7. Install VS Code extensions (if code CLI available)
# -------------------------------------------------------
if command -v code &>/dev/null; then
    log "Installing VS Code extensions..."
    EXTENSIONS=(
        "ms-python.python"
        "ms-python.vscode-pylance"
        "ms-python.black-formatter"
        "ms-toolsai.jupyter"
        "mtxr.sqltools"
        "mtxr.sqltools-driver-pg"
        "eamodio.gitlens"
        "christian-kohler.path-intellisense"
    )
    for ext in "${EXTENSIONS[@]}"; do
        code --install-extension "$ext" --force 2>&1 | tee -a "$LOG_FILE"
    done
    log "VS Code extensions installed"
else
    log "WARNING: VS Code CLI not found. Install extensions manually from the list in DAY_01_PLAN.md"
fi

# -------------------------------------------------------
# 8. Create .vscode/settings.json
# -------------------------------------------------------
mkdir -p .vscode
cat > .vscode/settings.json << 'VSEOF'
{
    "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
    "editor.formatOnSave": true,
    "[python]": {
        "editor.defaultFormatter": "ms-python.black-formatter"
    },
    "python.linting.enabled": true,
    "files.trimTrailingWhitespace": true,
    "editor.rulers": [88],
    "python.analysis.typeCheckingMode": "basic",
    "terminal.integrated.env.linux": {
        "VIRTUAL_ENV": "${workspaceFolder}/.venv"
    }
}
VSEOF
log ".vscode/settings.json created"

# -------------------------------------------------------
# 9. Summary
# -------------------------------------------------------
log "======================================================"
log "  Setup Complete Summary"
log "======================================================"
log "Python:     $("$PY_BIN" --version)"
log "pip:        $(pip --version | cut -d' ' -f1-2)"
log "Venv:       $PROJECT_DIR/.venv"
log "Packages:   $(pip list | wc -l) installed"
log "Log file:   $PROJECT_DIR/$LOG_FILE"
log ""
log "Next step: run  python verify_setup.py"
log "======================================================"
