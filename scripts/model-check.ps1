param(
    [string]$BaseUrl = "",
    [string]$Token = "",
    [string[]]$RequiredModels = @()
)

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$BackendEnv = Join-Path $Root "backend\.env"

function Read-EnvSetting {
    param([string]$Name)
    if (-not (Test-Path -LiteralPath $BackendEnv)) { return "" }
    $line = Get-Content -LiteralPath $BackendEnv | Where-Object { $_ -match "^$([Regex]::Escape($Name))=" } | Select-Object -Last 1
    if (-not $line) { return "" }
    $value = ($line -split "=", 2)[1].Trim()
    if ($value.Length -ge 2 -and (($value[0] -eq "'" -and $value[-1] -eq "'") -or ($value[0] -eq '"' -and $value[-1] -eq '"'))) {
        return $value.Substring(1, $value.Length - 2)
    }
    return $value
}

if (-not $BaseUrl) { $BaseUrl = Read-EnvSetting "FORMSIGHT_LMSTUDIO_BASE_URL" }
if (-not $BaseUrl) { $BaseUrl = "http://127.0.0.1:1234/v1" }
if (-not $Token) { $Token = Read-EnvSetting "FORMSIGHT_LMSTUDIO_API_KEY" }
if (-not $Token) {
    $encodedToken = Read-EnvSetting "FORMSIGHT_LMSTUDIO_API_KEY_B64"
    if ($encodedToken) { $Token = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($encodedToken)) }
}
if ($RequiredModels.Count -eq 0) {
    $RequiredModels = @(
        (Read-EnvSetting "FORMSIGHT_EXTRACTOR_MODEL_ID"),
        (Read-EnvSetting "FORMSIGHT_JUDGE_MODEL_ID")
    ) | Where-Object { $_ }
}
if ($RequiredModels.Count -eq 0) { $RequiredModels = @("qwen/qwen3-vl-8b", "qwen/qwen3-8b") }

$headers = @{}
if ($Token) { $headers.Authorization = "Bearer $Token" }
$response = Invoke-RestMethod -Uri "$($BaseUrl.TrimEnd('/'))/models" -Headers $headers -TimeoutSec 10
$ids = @($response.data | ForEach-Object { $_.id })
foreach ($model in $RequiredModels) {
    if ($ids -contains $model) { Write-Host "FOUND  $model" -ForegroundColor Green } else { Write-Warning "MISSING $model (installed IDs may differ; update the approved model profile if needed)" }
}
