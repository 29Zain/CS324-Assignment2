@echo off
rem Starts ONE worker in its own window. Run it on every computer that should compute.
rem Usage: start-worker.bat --bootstrap=192.168.1.50:50050 [--id=3] [--port=50053] [--host=192.168.1.53]
rem Without --id the Bootstrap Node gives the next unused id; without --port a free port is used.
cd /d "%~dp0.."
start "DistriLab Worker %*" cmd /k java -cp target\distrilab.jar worker.Worker %*
