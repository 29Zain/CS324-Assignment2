@echo off
rem Starts the client GUI.   Usage: start-client.bat [--bootstrap=192.168.1.50:50050]
cd /d "%~dp0.."
start "DistriLab Client" javaw -cp target\distrilab.jar client.Client %*
