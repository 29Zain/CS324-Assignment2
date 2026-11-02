@echo off
rem Automated fault-tolerance tests (about 5 minutes). Build first with build.bat.
rem Results: fault-tests-results.txt in the project folder; process logs in logs\fault-tests\
cd /d "%~dp0.."
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0fault-tests.ps1" > fault-tests-results.txt 2>&1
type fault-tests-results.txt
pause
