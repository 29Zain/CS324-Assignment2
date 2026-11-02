@echo off
rem Builds the project and runs the unit tests -> target\distrilab.jar
cd /d "%~dp0.."
call mvn -q -B clean package
if errorlevel 1 (
  echo BUILD FAILED
  pause
  exit /b 1
)
echo BUILD SUCCESSFUL: target\distrilab.jar
pause
