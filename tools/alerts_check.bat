@echo off
echo ==================================================
echo   🚨 ALERTS SERVICE TESTING SUITE
echo ==================================================
echo.

:: Navigate to the project root directory (one level up from the tools folder)
cd /d "%~dp0\.."

:: Activate the virtual environment
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
) else (
    echo [ERROR] Virtual environment not found! Please check your .venv folder.
    pause
    exit /b
)

echo [1/3] 🧹 Resetting Gold Alerts Layer...
python tools\reset_alerts_gold.py
echo.

echo [2/3] 📊 Starting Alerts Dashboard...
:: Start Streamlit in a new window so the batch script can continue
start "Alerts Dashboard" cmd /c "call .venv\Scripts\activate.bat && streamlit run alerts_service\dashboard_alerts.py"

:: Wait 2 seconds for the dashboard to initialize before asking the user
timeout /t 2 /nobreak > nul
echo.

echo [3/3] ⚙️  Alerts Service Engine
set /p START_APP="Do you want to start alerts_service\app.py now? (Y/N): "

if /I "%START_APP%"=="Y" (
    echo.
    echo 🚀 Starting Alerts Engine...
    :: Start the app in a new window and keep it open (/k) so you can see the logs
    start "Alerts App Engine" cmd /k "call .venv\Scripts\activate.bat && python alerts_service\app.py"
) else (
    echo.
    echo 🛑 Skipping Alerts Engine startup.
)

echo.
echo ✅ Alerts check sequence completed!
pause