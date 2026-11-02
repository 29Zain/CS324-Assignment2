@echo off
rem Stops every DistriLab process (Bootstrap, workers, clients) on this computer.
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='java.exe' OR Name='javaw.exe'\" | Where-Object { $_.CommandLine -like '*distrilab.jar*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
taskkill /F /FI "WINDOWTITLE eq DistriLab*" >nul 2>nul
echo DistriLab processes stopped.
