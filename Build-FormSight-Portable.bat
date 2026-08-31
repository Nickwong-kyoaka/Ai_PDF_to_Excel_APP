@echo off
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\build-portable.ps1"
if errorlevel 1 (
  echo.
  echo Portable build failed. Review the message above.
  pause
  exit /b 1
)
echo.
echo FormSight-Local-Portable.exe is ready in the release folder.
pause
