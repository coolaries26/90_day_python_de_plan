# =============================================================================
# setup_git.ps1 - Day 01 Sprint 01
# Git repo init, branching strategy, SSH key setup, daily push workflow
# Usage: .\setup_git.ps1 -GitHubUser YOUR_GITHUB_USERNAME
# =============================================================================

param(
    [string]$GitHubUser = ""
)

$ErrorActionPreference = "Continue"
$LogFile = "setup_git.log"

function Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $output = "[$timestamp] $Message"
    Write-Host $output
    Add-Content -Path $LogFile -Value $output
}

function HR {
    Log "------------------------------------------------------"
}

Log "======================================================"
Log "  Git Setup - Python DE Journey"
Log "======================================================"

# 1. Check if Git is installed
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Log "Git not found. Please install Git from https://git-scm.com/download/win"
    exit 1
}

$gitVersion = git --version
Log "Git: $gitVersion"

# 2. Configure Git globals
HR
Log "Configuring Git globals..."

$gitName = git config --global user.name 2>$null
if ([string]::IsNullOrEmpty($gitName)) {
    $gitName = Read-Host "Enter your full name for Git commits"
    git config --global user.name $gitName
}

$gitEmail = git config --global user.email 2>$null
if ([string]::IsNullOrEmpty($gitEmail)) {
    $gitEmail = Read-Host "Enter your email for Git commits"
    git config --global user.email $gitEmail
}

git config --global core.autocrlf input
git config --global push.default current
git config --global pull.rebase false
git config --global init.defaultBranch main

Log "Git globals:"
Log "  user.name  = $(git config --global user.name)"
Log "  user.email = $(git config --global user.email)"

# 3. Generate SSH key for GitHub (if not exists)
HR
$sshKeyFile = "$HOME\.ssh\id_ed25519_github"
$sshDir = "$HOME\.ssh"

if (-not (Test-Path $sshKeyFile)) {
    Log "Generating SSH key for GitHub..."
    
    if (-not (Test-Path $sshDir)) {
        New-Item -ItemType Directory -Path $sshDir -Force | Out-Null
    }
    
    $gitEmail = git config --global user.email
    ssh-keygen -t ed25519 -C $gitEmail -f $sshKeyFile -N '""'
    
    Log "SSH key generated: $sshKeyFile"
    Log ""
    Log "Add this public key to GitHub - Settings - SSH Keys:"
    Log "------------------------------------------------------------"
    Get-Content "$sshKeyFile.pub"
    Log "------------------------------------------------------------"
    Log "GitHub URL: https://github.com/settings/ssh/new"
} else {
    Log "SSH key already exists: $sshKeyFile"
}

# 4. Initialize project repository
HR
$projectDir = "$HOME\python-de-journey"
New-Item -ItemType Directory -Path $projectDir -Force | Out-Null
Set-Location $projectDir
Log "Working in: $projectDir"

# Fix git safe directory warning
git config --global --add safe.directory $projectDir

if (-not (Test-Path ".git")) {
    Log "Initialising git repository..."
    git init
    git branch -M main
    Log "Repository initialised"
} else {
    Log "Git repository already initialised"
}

# 5. Create .gitignore
$gitignoreLines = @"
__pycache__/
*.py[cod]
*.so
.Python
*.egg
*.egg-info/
dist/
build/
.eggs/
.venv/
venv/
env/
ENV/
env.bak/
venv.bak/
.env
.env.*
!.env.example
secrets/
*.pem
*.key
db_config.ini
config.ini
.vscode/settings.json
.idea/
*.iml
*.sublime-project
*.sublime-workspace
*.log
logs/
!logs/.gitkeep
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db
.ipynb_checkpoints/
*.ipynb
*.csv
*.parquet
*.feather
data/raw/
data/processed/
!data/.gitkeep
.pytest_cache/
.coverage
htmlcov/
.mypy_cache/
.dmypy.json
airflow/logs/
airflow/dags/__pycache__/
"@

Set-Content -Path ".gitignore" -Value $gitignoreLines -Force
Log ".gitignore created"

# 6. Create project folder structure
HR
Log "Creating project folder structure..."

$folders = @(
    "sprint-01\day-01",
    "sprint-01\day-02",
    "sprint-01\day-03",
    "sprint-01\day-04",
    "sprint-01\day-05",
    "sprint-01\day-06",
    "sprint-01\day-07",
    "sprint-02",
    "sprint-03",
    "sprint-04",
    "sprint-05",
    "sprint-06",
    "sprint-07",
    "sprint-08",
    "sprint-09",
    "sprint-10",
    "sprint-11",
    "sprint-12",
    "sprint-13",
    "scripts",
    "logs",
    "data",
    "tests",
    "docs"
)

foreach ($folder in $folders) {
    New-Item -ItemType Directory -Path $folder -Force | Out-Null
}

New-Item -ItemType File -Path "logs\.gitkeep" -Force | Out-Null
New-Item -ItemType File -Path "data\.gitkeep" -Force | Out-Null

# Create .env.example
$envContent = @"
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
"@

Set-Content -Path ".env.example" -Value $envContent -Force
Log "Folder structure created"

# 7. Initial commit on main
HR
Log "Creating initial commit on main..."

git add .
try {
    git commit -m "[INIT] Project scaffold: folder structure, .gitignore, .env.example"
} catch {
    Log "Nothing to commit (already initialised)"
}

# 8. Create develop branch
HR
Log "Creating develop branch..."

try {
    git checkout -b develop
} catch {
    git checkout develop
}

$currentBranch = git branch --show-current
Log "On branch: $currentBranch"

# 9. Connect to remote (GitHub)
HR
if ($GitHubUser) {
    $remoteUrl = "git@github.com:$GitHubUser/python-de-journey.git"
    
    try {
        $existingRemote = git remote get-url origin 2>$null
    } catch {
        $existingRemote = ""
    }
    
    if (-not $existingRemote) {
        Log "Adding remote origin: $remoteUrl"
        git remote add origin $remoteUrl
    } else {
        Log "Remote origin already set: $existingRemote"
    }
    
    Log "Pushing main and develop to remote..."
    try {
        git push -u origin main
    } catch {
        Log "Push failed - check SSH key is added to GitHub"
    }
    
    try {
        git push -u origin develop
    } catch {
        Log "Push develop failed"
    }
} else {
    Log "No -GitHubUser provided. To add remote manually:"
    Log "  git remote add origin git@github.com:YOUR_USERNAME/python-de-journey.git"
    Log "  git push -u origin main"
}

# 10. Create Day 01 feature branch
HR
Log "Creating Day 01 feature branch..."

git checkout develop
try {
    git checkout -b sprint-01/day-01-env-setup
} catch {
    git checkout sprint-01/day-01-env-setup
}

$currentBranch = git branch --show-current
Log "Current branch: $currentBranch"

# 11. Write daily push helper script
HR
$scriptContent = @'
param(
    [int]$Day,
    [string]$Message = "Daily progress update"
)

if (-not $Day) {
    Write-Host "Usage: `$0 -Day N -Message 'message'"
    exit 1
}

$branch = git branch --show-current
$dayNum = $("{0:D3}" -f $Day)
$commitMsg = "[DAY-$dayNum] $Message"

Write-Host "Branch:  $branch"
Write-Host "Commit:  $commitMsg"
Write-Host ""

git add .
try {
    git commit -m "$commitMsg"
} catch {
    Write-Host "Nothing to commit"
}

try {
    git push -u origin $branch
    Write-Host "Pushed to $branch"
} catch {
    Write-Host "Push failed"
}
'@

New-Item -ItemType Directory -Path "scripts" -Force | Out-Null
Set-Content -Path "scripts\git_daily_push.ps1" -Value $scriptContent -Force
Log "scripts\git_daily_push.ps1 created"

# 12. Summary
Log "======================================================"
Log "  Git Setup Complete"
Log "======================================================"
Log "Repo:    $projectDir"
Log "Branch:  $(git branch --show-current)"
try {
    $remote = git remote get-url origin
} catch {
    $remote = "Not set"
}
Log "Remote:  $remote"
Log ""
Log "DAILY WORKFLOW:"
Log "  1. git checkout sprint-XX/day-YY-topic"
Log "  2. Do your work"
Log "  3. .\scripts\git_daily_push.ps1 -Day N"
Log "  4. Open PR to develop on GitHub"
Log "======================================================"
