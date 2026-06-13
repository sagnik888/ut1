@echo off
setlocal EnableDelayedExpansion
title UT1 Index Trading System
color 0B

echo ========================================================
echo        UT1 INDEX TRADING SYSTEM - STARTUP
echo ========================================================
echo.

cd /d "%~dp0"
echo [INFO] Working Directory: %CD%

:: Check Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.10+
    pause
    exit /b
)

:: Activate virtual environment
if exist "venv\Scripts\activate.bat" (
    echo [INFO] Activating virtual environment...
    call venv\Scripts\activate.bat
) else (
    echo [WARNING] Virtual environment not found. Using system Python.
)

:: Install dependencies if needed
echo [INFO] Checking dependencies...
python -c "import uvicorn" >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Installing dependencies...
    pip install -r requirements.txt
)

:: Start server
echo ========================================================
echo [INFO] Starting server on http://localhost:7000
echo ========================================================
echo.

start http://localhost:7000
python main.py

pause
