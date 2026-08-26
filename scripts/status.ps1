$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runtime = Join-Path $Root "runtime"
foreach ($name in @("api", "worker", "web", "caddy")) {
    $path = Join-Path $Runtime "$name.pid"
    $state = "stopped"
    if (Test-Path -LiteralPath $path) {
        $processId = [int](Get-Content -LiteralPath $path -Raw)
        if (Get-Process -Id $processId -ErrorAction SilentlyContinue) { $state = "running (PID $processId)" }
    }
    "{0,-10} {1}" -f $name, $state
}
try { "API health  " + (Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/health" -TimeoutSec 3).status } catch { "API health  offline" }
try { "Web health  HTTP " + (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:3000" -TimeoutSec 3).StatusCode } catch { "Web health  offline" }
