@echo off
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\package-transfer.ps1"
if errorlevel 1 (
  echo.
  echo Transfer package failed. / 傳輸套件建立失敗。
  pause
  exit /b 1
)
echo.
echo Transfer ZIP is ready in the release folder. / 傳輸 ZIP 已建立於 release 資料夾。
pause
