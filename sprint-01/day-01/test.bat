@echo off
REM Check if python is available on PATH
python --version >nul 2>&1
if errorlevel 1 (
    echo Python NOT found on PATH.
) else (
    echo Python found on PATH.
)