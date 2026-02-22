@echo off
title Streaming Pipeline Launcher
cd /d "%~dp0"

echo =======================================================
echo 🚀 STREAMING EMULATOR: PIPELINE LAUNCHER
echo =======================================================
echo.

echo [1/2] Launching Inference Cluster...
:: Open a new CMD window, activate venv, and run the inference cluster
start "Inference Cluster Launcher" cmd /k "call .venv\Scripts\activate.bat && python inference_service\start_inference_cluster.py"

echo.
echo Waiting 30 seconds for ML models to load into memory...
timeout /t 30 /nobreak

echo.
:: Prompt for Y/N (Choice automatically waits for valid input without needing Enter)
choice /C YN /M "Do you want to start the Writer Cluster now?"
if errorlevel 2 goto :skip_writer
if errorlevel 1 goto :start_writer

:start_writer
echo.
echo [2/2] Launching Writer Cluster...
:: Open a new CMD window, activate venv, and run the writer cluster
start "Writer Cluster Launcher" cmd /k "call .venv\Scripts\activate.bat && python writer_service\src\start_writer_cluster.py"
goto :end

:skip_writer
echo.
echo Skipping Writer Cluster.

:end
echo.
echo ✅ Launcher finished. You can safely close this specific window.
pause