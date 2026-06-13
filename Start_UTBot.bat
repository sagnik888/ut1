@echo off
setlocal EnableDelayedExpansion
title UT1 Index Trading System - Advanced Launcher
color 0B

echo ========================================================
echo        UT1 INDEX TRADING SYSTEM - STARTUP SCRIPT
echo ========================================================
echo.

:: 1. Force script to run in its own directory
cd /d "%~dp0"
echo [INFO] Working Directory: %CD%

:: 2. Check for Python installation
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed or not added to PATH.
    echo Please install Python 3.10+ from python.org and check "Add Python to PATH".
    pause
    exit /b
)

:: 3. Setup Virtual Environment if missing
if not exist "venv\Scripts\activate.bat" (
    echo [INFO] Virtual environment 'venv' not found. Creating one now...
    python -m venv venv
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to create virtual environment.
        pause
        exit /b
    )
    echo [INFO] Virtual environment created successfully.
)

:: 4. Activate Virtual Environment
echo [INFO] Activating virtual environment...
call venv\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo [ERROR] Failed to activate virtual environment.
    pause
    exit /b
)

:: 5. Ensure dependencies are installed (Quick check)
echo [INFO] Verifying dependencies...
python -c "import uvicorn, loguru" >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Missing core dependencies. Installing from requirements.txt...
    pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to install dependencies. Check your internet connection.
        pause
        exit /b
    )
)

:: 6. Launch the Server
echo ========================================================
echo [INFO] Starting Backend Server (main.py)...
echo [INFO] Dashboard will be available at http://127.0.0.1:7000/
echo ========================================================

:: Open dashboard in browser async
start "" http://127.0.0.1:7000/

:: Start the python process
python main.py

:: 7. Error Handling
echo.
echo ========================================================
echo [WARNING] The server process has terminated or crashed.
echo ========================================================
pause
