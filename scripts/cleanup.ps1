$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python = Join-Path $Root "backend\.venv\Scripts\python.exe"
Push-Location (Join-Path $Root "backend")
try { & $Python -m app.maintenance purge } finally { Pop-Location }
