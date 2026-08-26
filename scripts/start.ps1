param([switch]$SkipLMStudio)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Backend = Join-Path $Root "backend"
$Runtime = Join-Path $Root "runtime"
$Python = Join-Path $Backend ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) { throw "Run scripts\install.ps1 first." }
New-Item -ItemType Directory -Force -Path $Runtime | Out-Null

if (-not $SkipLMStudio) {
    $lms = Get-Command lms -ErrorAction SilentlyContinue
    if ($lms) {
        Start-Process -FilePath $lms.Source -ArgumentList @("daemon", "up") -WindowStyle Hidden -Wait
        Start-Process -FilePath $lms.Source -ArgumentList @("server", "start", "--bind", "127.0.0.1", "--port", "1234") -WindowStyle Hidden -Wait
    } else {
        Write-Warning "lms was not found. Start LM Studio manually on 127.0.0.1:1234."
    }
}

$api = Start-Process -FilePath $Python -ArgumentList @("-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000") -WorkingDirectory $Backend -WindowStyle Hidden -PassThru
$worker = Start-Process -FilePath $Python -ArgumentList @("-m", "app.worker") -WorkingDirectory $Backend -WindowStyle Hidden -PassThru
$npm = (Get-Command npm).Source
$web = Start-Process -FilePath $npm -ArgumentList @("run", "start") -WorkingDirectory $Root -WindowStyle Hidden -PassThru

Set-Content -LiteralPath (Join-Path $Runtime "api.pid") -Value $api.Id
Set-Content -LiteralPath (Join-Path $Runtime "worker.pid") -Value $worker.Id
Set-Content -LiteralPath (Join-Path $Runtime "web.pid") -Value $web.Id

$caddy = Get-Command caddy -ErrorAction SilentlyContinue
if ($caddy -and (Test-Path -LiteralPath (Join-Path $Root "Caddyfile"))) {
    $proxy = Start-Process -FilePath $caddy.Source -ArgumentList @("run", "--config", (Join-Path $Root "Caddyfile")) -WorkingDirectory $Root -WindowStyle Hidden -PassThru
    Set-Content -LiteralPath (Join-Path $Runtime "caddy.pid") -Value $proxy.Id
}
Write-Host "FormSight started. Use the HTTPS address configured in Caddyfile, or http://127.0.0.1:3000 locally."
