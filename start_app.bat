@echo off

python --version >nul 2>&1
if errorlevel 1 (
    pause
    exit /b 1
)

for /f "tokens=5" %%a in ('netstat -aon ^| findstr :5000') do (
    taskkill /f /pid %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul

set VENV_DIR=.venv

if not exist "%VENV_DIR%" (
    python -m venv "%VENV_DIR%"
)

call "%VENV_DIR%\Scripts\activate.bat"

python -m pip install --upgrade pip >nul 2>&1

pip install -r requirements.txt >nul 2>&1

start /b python run.py

timeout /t 3 /nobreak >nul

start http://localhost:5000

pause 