param(
    [string]$Python = "python",
    [switch]$WithML
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Backend = Join-Path $Root "backend"
$VenvPython = Join-Path $Backend ".venv\Scripts\python.exe"

Write-Host "Installing FormSight in $Root"
if (-not (Test-Path -LiteralPath $VenvPython)) {
    & $Python -m venv (Join-Path $Backend ".venv")
}
$requirements = if ($WithML) { "requirements-ml.txt" } else { "requirements.txt" }
& $VenvPython -m pip install -r (Join-Path $Backend $requirements)

foreach ($pair in @(@(".env.example", ".env"), @("backend\.env.example", "backend\.env"))) {
    $source = Join-Path $Root $pair[0]
    $target = Join-Path $Root $pair[1]
    if (-not (Test-Path -LiteralPath $target)) { Copy-Item -LiteralPath $source -Destination $target }
}

Push-Location $Root
try {
    npm install
    npm run build
} finally {
    Pop-Location
}

New-Item -ItemType Directory -Force -Path (Join-Path $Backend "models"), (Join-Path $Root "runtime") | Out-Null

Write-Host "Installation complete. Run scripts\preflight.ps1 after configuration, or use Deploy-FormSight.bat for guided deployment."
