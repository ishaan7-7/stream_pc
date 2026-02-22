@echo off
echo Starting Telemetry Observer System...

:: ------------------------------------------------------
:: 1. Start the Hybrid Backend Engine
:: ------------------------------------------------------
:: Runs as a module (-m) to ensure it finds the 'ingest' config correctly
start "Observer Engine (Backend)" cmd /k "call .venv\Scripts\activate.bat && python -m telemetry_observer.observer_backend"

:: ------------------------------------------------------
:: 2. Start the Streamlit UI
:: ------------------------------------------------------
:: Launches the dashboard in your default browser
start "Observer UI (Streamlit)" cmd /k "call .venv\Scripts\activate.bat && streamlit run telemetry_observer/ui.py"

echo.
echo Telemetry Observer is launching...
echo 1. The Backend window will show log snapshots every 5 seconds.
echo 2. The UI window will launch your browser.
echo.
pause