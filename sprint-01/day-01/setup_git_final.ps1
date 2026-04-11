# ============================================================
# setup_git.ps1 — FINAL TESTED VERSION
# ============================================================

param(
    [string]$GitHubUser = ""
)

$ErrorActionPreference = "Stop"
$LOG_FILE = "setup_git.log"

function Log {
    param ([string]$msg)
    $time = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$time] $msg"
    Write-Host $line
    Add-Content -Path $LOG_FILE -Value $line
}

function HR {
    Log "------------------------------------------------------"
}

Log "======================================================"
Log "Git Setup — FINAL TESTED VERSION"
Log "======================================================"

# CHECK GIT
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Log "ERROR: Git not installed."
    exit 1
}

Log "Git: $(git --version)"

# CONFIG
HR
$gitName  = git config --global user.name
$gitEmail = git config --global user.email

if (-not $gitName) {
    $gitName = Read-Host "Enter your full name"
    git config --global user.name "$gitName"
}

if (-not $gitEmail) {
    $gitEmail = Read-Host "Enter your email"
    git config --global user.email "$gitEmail"
}

# PROJECT
HR
$projectDir = "$HOME\python-de-journey"
New-Item -ItemType Directory -Force -Path $projectDir | Out-Null
Set-Location $projectDir

if (!(Test-Path ".git")) {
    git init | Out-Null
    git branch -M main
}

# .gitignore
$gitignore = @'
__pycache__/
*.pyc
.env
*.log
'@
$gitignore | Set-Content ".gitignore"

# commit
git add .
git commit -m "init" 2>$null

# DAILY SCRIPT
HR

$dailyScript = @'
param(
    [int]$Day,
    [string]$Msg = "Daily progress"
)

$branch = git branch --show-current
$dayFormatted = "{0:D3}" -f $Day
$commitMsg = "[DAY-$dayFormatted] $Msg"

Write-Host "Branch: $branch"
Write-Host "Commit: $commitMsg"

git add .
git commit -m $commitMsg
git push -u origin $branch
'@

New-Item -ItemType Directory -Force -Path "scripts" | Out-Null
$dailyScript | Set-Content "scripts\git_daily_push.ps1"

HR
Log "SUCCESS: Script completed"
