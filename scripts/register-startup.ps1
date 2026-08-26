$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$PowerShell = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$startAction = New-ScheduledTaskAction -Execute $PowerShell -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$(Join-Path $Root 'scripts\start.ps1')`""
$startTrigger = New-ScheduledTaskTrigger -AtStartup
$cleanupAction = New-ScheduledTaskAction -Execute $PowerShell -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$(Join-Path $Root 'scripts\cleanup.ps1')`""
$cleanupTrigger = New-ScheduledTaskTrigger -Daily -At 2:30AM
Register-ScheduledTask -TaskName "FormSight Server" -Action $startAction -Trigger $startTrigger -RunLevel Highest -Force | Out-Null
Register-ScheduledTask -TaskName "FormSight Retention Cleanup" -Action $cleanupAction -Trigger $cleanupTrigger -RunLevel Highest -Force | Out-Null
Write-Host "Startup and daily retention tasks registered."
