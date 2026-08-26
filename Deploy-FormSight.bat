@echo off
setlocal
chcp 65001 >nul
title FormSight Deployment Wizard
set "FORMSIGHT_ROOT=%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%FORMSIGHT_ROOT%scripts\deploy-wizard.ps1"
set "FORMSIGHT_EXIT=%ERRORLEVEL%"
echo.
if not "%FORMSIGHT_EXIT%"=="0" echo Deployment stopped with error code %FORMSIGHT_EXIT%.
pause
exit /b %FORMSIGHT_EXIT%
