@echo off
rem ---------------------------------------------------------------------------
rem Convenience for a ONE-computer demonstration: starts the Bootstrap Node,
rem then N workers one at a time (1 s apart), then a client. On several
rem computers start each part separately instead (see README.md).
rem Usage: demo-local.bat [N]     (default 5 workers)
rem ---------------------------------------------------------------------------
setlocal enabledelayedexpansion
cd /d "%~dp0.."
if not exist target\distrilab.jar (
  echo target\distrilab.jar not found - run scripts\build.bat first.
  pause
  exit /b 1
)
set N=%1
if "%N%"=="" set N=5

echo Starting Bootstrap Node on port 50050 ...
start "DistriLab Bootstrap" cmd /k java -cp target\distrilab.jar bootstrap.BootstrapNode --port=50050
timeout /t 2 /nobreak >nul

for /L %%i in (1,1,%N%) do (
  set /a PORT=50050+%%i
  echo Starting Worker %%i on port !PORT! ...
  start "DistriLab Worker %%i" cmd /k java -cp target\distrilab.jar worker.Worker --bootstrap=localhost:50050 --id=%%i --port=!PORT! --minWorkersForFirstElection=%N%
  timeout /t 1 /nobreak >nul
)

echo Waiting for the first election ...
timeout /t 6 /nobreak >nul
start "DistriLab Client" javaw -cp target\distrilab.jar client.Client --bootstrap=localhost:50050
echo Done. Use stop-all.bat to close everything.
