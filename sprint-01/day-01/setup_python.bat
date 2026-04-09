@echo off
setlocal enabledelayedexpansion

REM =============================================================================
REM setup_python.bat — Day 01 Sprint 01 (Windows)
REM Python 3.11 + venv + VS Code + required extensions setup
REM Usage: setup_python.bat
REM =============================================================================

set LOG_FILE=setup_python.log


call :log ======================================================
call :log  "Python DE Journey — Environment Setup Script (Windows)"
call :log ======================================================

REM ------------------------------------------------------------
REM 1. Install Python 3.11 (if not present)
REM ------------------------------------------------------------
python --version >nul 2>&1
if %errorlevel% neq 0 (
    call :log Python not found, attempting install via Chocolatey...

	choco --version >nul 2>&1
        call :log Chocolatey installed.
    )

    choco install -y python --version=3.11.0 git curl wget >> "%LOG_FILE%" 2>&1
    if %errorlevel% neq 0 call :fail Failed to install Python 3.11 via Chocolatey.
    call :log Python 3.11 installed via Chocolatey.
) else (
    call :log Python already present, skipping install.
)

REM Refresh PATH for this session
for /f "delims=" %%i in ('where python') do set PY_BIN=%%i

if "%PY_BIN%"=="" (
    call :fail python not found in PATH even after install.
)

for /f "delims=" %%v in ('"%PY_BIN%" --version 2^>^&1') do set PY_VER=%%v
call :log Python binary: %PY_BIN%
call :log Python version: %PY_VER%

REM ------------------------------------------------------------
REM 3. Create project directory
REM ------------------------------------------------------------
set "PROJECT_DIR=%USERPROFILE%\python-de-journey"
call :log Project directory: %PROJECT_DIR%
if not exist "%PROJECT_DIR%" mkdir "%PROJECT_DIR%"
cd /d "%PROJECT_DIR%"

REM ------------------------------------------------------------
REM 4. Create virtual environment
REM ------------------------------------------------------------
if not exist ".venv" (
    call :log Creating virtual environment...
    "%PY_BIN%" -m venv .venv >> "%LOG_FILE%" 2>&1
    if %errorlevel% neq 0 call :fail Failed to create virtual environment.
    call :log Virtual environment created at .venv\
) else (
    call :log Virtual environment already exists, skipping.
)

REM Activate venv for this script
call ".venv\Scripts\activate.bat"
if %errorlevel% neq 0 call :fail Failed to activate virtual environment.
call :log Virtual environment activated.

REM ------------------------------------------------------------
REM 5. Upgrade pip and core tools
REM ------------------------------------------------------------
call :log Upgrading pip, setuptools, wheel...
python -m pip install --upgrade pip setuptools wheel >> "%LOG_FILE%" 2>&1
if %errorlevel% neq 0 call :fail Failed to upgrade pip/setuptools/wheel.

REM ------------------------------------------------------------
REM 6. Install project dependencies
REM ------------------------------------------------------------
if exist "requirements.txt" (
    call :log Installing from requirements.txt...
    pip install -r requirements.txt >> "%LOG_FILE%" 2>&1
    if %errorlevel% neq 0 call :fail Failed to install from requirements.txt.
) else (
    call :log requirements.txt not found — installing core packages only...
    pip install ^
        psycopg2-binary==2.9.9 ^
        SQLAlchemy==2.0.23 ^
        alembic==1.13.0 ^
        pandas==2.1.4 ^
        numpy==1.26.2 ^
        python-dotenv==1.0.0 ^
        pyyaml==6.0.1 ^
        click==8.1.7 ^
        black==23.12.0 ^
        pylint==3.0.3 ^
        pytest==7.4.3 ^
        pytest-cov==4.1.0 ^
        loguru==0.7.2 ^
        >> "%LOG_FILE%" 2>&1
    if %errorlevel% neq 0 call :fail Failed to install core packages.

    pip freeze > requirements.txt
    call :log requirements.txt generated from installed packages.
)

REM ------------------------------------------------------------
REM 7. Install VS Code extensions (if code CLI available)
REM ------------------------------------------------------------
where code 1>nul 2>nul
if %errorlevel%==0 (
    call :log Installing VS Code extensions...
    for %%E in (
        ms-python.python
        ms-python.vscode-pylance
        ms-python.black-formatter
        ms-toolsai.jupyter
        mtxr.sqltools
        mtxr.sqltools-driver-pg
        eamodio.gitlens
        christian-kohler.path-intellisense
    ) do (
        code --install-extension %%E --force >> "%LOG_FILE%" 2>&1
    )
    call :log VS Code extensions installed.
) else (
    call :log WARNING: VS Code CLI not found. Install extensions manually from DAY_01_PLAN.md.
)

REM ------------------------------------------------------------
REM 8. Create .vscode/settings.json
REM ------------------------------------------------------------
if not exist ".vscode" mkdir ".vscode"

> ".vscode\settings.json" (
    echo {
    echo   "python.defaultInterpreterPath": "${workspaceFolder}\\.venv\\Scripts\\python.exe",
    echo   "editor.formatOnSave": true,
    echo   "[python]": {
    echo     "editor.defaultFormatter": "ms-python.black-formatter"
    echo   },
    echo   "python.linting.enabled": true,
    echo   "files.trimTrailingWhitespace": true,
    echo   "editor.rulers": [88],
    echo   "python.analysis.typeCheckingMode": "basic",
    echo   "terminal.integrated.env.windows": {
    echo     "VIRTUAL_ENV": "${workspaceFolder}\\.venv"
    echo   }
    echo }
)

call :log .vscode\settings.json created.

REM ------------------------------------------------------------
REM 9. Summary
REM ------------------------------------------------------------
for /f "delims=" %%p in ('python --version 2^>^&1') do set PY_SUM=%%p
for /f "tokens=1-2" %%p in ('pip --version') do set PIP_SUM=%%p %%q

call :log ======================================================
call :log  Setup Complete Summary
call :log ======================================================
call :log Python: %PY_SUM%
call :log pip: %PIP_SUM%
call :log Venv: %PROJECT_DIR%\.venv
for /f "skip=2" %%c in ('pip list ^| find /c /v ""') do set PKG_COUNT=%%c
call :log Packages: %PKG_COUNT% installed
call :log Log file: %PROJECT_DIR%\%LOG_FILE%
call :log
call :log Next step: run "python verify_setup.py"
call :log ======================================================

endlocal
exit /b 0

:log
set MSG=%*
for /f "usebackq tokens=1-2 delims= " %%a in (`powershell -NoLogo -Command "Get-Date -Format \"yyyy-MM-dd HH:mm:ss\""`) do set NOW=%%a %%b
echo [%NOW%] %MSG%>>"%LOG_FILE%"
echo [%NOW%] %MSG%
goto :eof

:fail
call :log ERROR: %*
exit /b 1
