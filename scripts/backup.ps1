$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python = Join-Path $Root "backend\.venv\Scripts\python.exe"
$Destination = Join-Path $Root "backups"
Push-Location (Join-Path $Root "backend")
try { & $Python -m app.maintenance backup $Destination } finally { Pop-Location }
