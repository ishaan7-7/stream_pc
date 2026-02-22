@echo off
setlocal ENABLEDELAYEDEXPANSION

REM -------------------------------------------------
REM Resolve repo root (parent of kafka_metrics)
REM -------------------------------------------------
set SCRIPT_DIR=%~dp0
for %%i in ("%SCRIPT_DIR%\..") do set REPO_ROOT=%%~fi

REM -------------------------------------------------
REM Activate virtual environment
REM -------------------------------------------------
if not exist "%REPO_ROOT%\.venv\Scripts\activate.bat" (
    echo ERROR: .venv not found at %REPO_ROOT%\.venv
    exit /b 1
)

call "%REPO_ROOT%\.venv\Scripts\activate.bat"

REM -------------------------------------------------
REM Set PYTHONPATH so absolute imports work
REM -------------------------------------------------
set PYTHONPATH=%REPO_ROOT%

REM -------------------------------------------------
REM Start Kafka Metrics FastAPI service
REM -------------------------------------------------
echo Starting Kafka Metrics Service...
echo Repo root: %REPO_ROOT%
echo PYTHONPATH=%PYTHONPATH%
echo.

uvicorn kafka_metrics.app.main:app ^
  --host 127.0.0.1 ^
  --port 9201 ^
  --log-level info

REM -------------------------------------------------
REM Graceful shutdown
REM -------------------------------------------------
echo Kafka Metrics Service stopped.
endlocal
