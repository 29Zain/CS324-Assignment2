@echo off
rem Simulates a CRASH of one worker (no clean shutdown).   Usage: kill-worker.bat 3
if "%1"=="" (
  echo Usage: kill-worker.bat ^<worker id^>
  exit /b 1
)
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | Where-Object { $_.CommandLine -like '*worker.Worker*--id=%1 *' -or $_.CommandLine -like '*worker.Worker*--id=%1' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Host ('Worker %1 killed (pid ' + $_.ProcessId + ')') }"
