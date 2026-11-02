@echo off
rem Starts the Bootstrap Node in its own window.   Usage: start-bootstrap.bat [--port=50050]
cd /d "%~dp0.."
start "DistriLab Bootstrap" cmd /k java -cp target\distrilab.jar bootstrap.BootstrapNode %*
