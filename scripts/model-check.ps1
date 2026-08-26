param(
    [string]$BaseUrl = "http://127.0.0.1:1234/v1",
    [string]$Token = $env:FORMSIGHT_LMSTUDIO_API_KEY
)
$headers = @{}
if ($Token) { $headers.Authorization = "Bearer $Token" }
$response = Invoke-RestMethod -Uri "$($BaseUrl.TrimEnd('/'))/models" -Headers $headers -TimeoutSec 10
$ids = @($response.data | ForEach-Object { $_.id })
$required = @("qwen/qwen3-vl-8b", "qwen/qwen3-8b")
foreach ($model in $required) {
    if ($ids -contains $model) { Write-Host "FOUND  $model" -ForegroundColor Green } else { Write-Warning "MISSING $model (installed IDs may differ; update the approved model profile if needed)" }
}
