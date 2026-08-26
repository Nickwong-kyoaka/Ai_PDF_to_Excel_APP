$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runtime = Join-Path $Root "runtime"
if (-not (Test-Path -LiteralPath $Runtime)) { Write-Host "FormSight is not running."; exit 0 }

foreach ($name in @("api", "worker", "web", "caddy")) {
    $pidPath = Join-Path $Runtime "$name.pid"
    if (-not (Test-Path -LiteralPath $pidPath)) { continue }
    $processId = [int](Get-Content -LiteralPath $pidPath -Raw)
    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $processId" -ErrorAction SilentlyContinue
    if ($process -and ($process.CommandLine -like "*$Root*" -or $name -eq "caddy")) {
        Stop-Process -Id $processId -ErrorAction SilentlyContinue
        Write-Host "Stopped $name ($processId)"
    } else {
        Write-Warning "Skipped PID $processId because it no longer belongs to this FormSight installation."
    }
    Remove-Item -LiteralPath $pidPath -Force
}
