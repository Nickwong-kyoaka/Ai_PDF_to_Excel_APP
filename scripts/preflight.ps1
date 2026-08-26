param(
    [string]$BaseUrl = "",
    [string]$Token = "",
    [string[]]$RequiredModels = @()
)

$ErrorActionPreference = "Continue"
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
    if ($encodedToken) { try { $Token = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($encodedToken)) } catch {} }
}
if ($RequiredModels.Count -eq 0) {
    $RequiredModels = @(
        (Read-EnvSetting "FORMSIGHT_EXTRACTOR_MODEL_ID"),
        (Read-EnvSetting "FORMSIGHT_JUDGE_MODEL_ID")
    ) | Where-Object { $_ }
}
Write-Host "FormSight Windows preflight" -ForegroundColor Cyan
Get-CimInstance Win32_OperatingSystem | Select-Object Caption, Version, OSArchitecture, @{N="RAM_GB";E={[math]::Round($_.TotalVisibleMemorySize / 1MB, 1)}} | Format-List
Get-CimInstance Win32_Processor | Select-Object Name, NumberOfLogicalProcessors | Format-List
$coreinfo = Get-Command coreinfo.exe -ErrorAction SilentlyContinue
if ($coreinfo) {
    $avx2 = (& $coreinfo.Source -f 2>$null | Select-String "AVX2")
    if ($avx2 -match "\*") { Write-Host "AVX2 supported" -ForegroundColor Green } else { Write-Warning "AVX2 was not detected" }
} else { Write-Warning "coreinfo.exe is not installed; LM Studio startup will be the AVX2 runtime check." }

$nvidia = Get-Command nvidia-smi.exe -ErrorAction SilentlyContinue
if ($nvidia) { & $nvidia.Source --query-gpu=name,memory.total,driver_version --format=csv,noheader } else { Write-Warning "nvidia-smi was not found." }

$weights = Join-Path $Root "backend\models\questionnaire_marks.onnx"
if (Test-Path -LiteralPath $weights) { Write-Host "YOLO weights found" -ForegroundColor Green } else { Write-Warning "YOLO weights missing: $weights" }
try {
    $headers = @{}
    if ($Token) { $headers.Authorization = "Bearer $Token" }
    $models = Invoke-RestMethod -Uri "$($BaseUrl.TrimEnd('/'))/models" -Headers $headers -TimeoutSec 5
    $modelIds = @($models.data | ForEach-Object { $_.id })
    Write-Host "LM Studio online: $($modelIds -join ', ')" -ForegroundColor Green
    foreach ($required in $RequiredModels) {
        if ($modelIds -notcontains $required) { Write-Warning "Configured model is not currently reported by LM Studio: $required" }
    }
} catch { Write-Warning "LM Studio is not reachable on loopback port 1234. Start it with authentication enabled." }

$drive = Get-PSDrive -Name ([IO.Path]::GetPathRoot($Root).TrimEnd('\').TrimEnd(':'))
Write-Host "Free disk: $([math]::Round($drive.Free / 1GB, 1)) GB"
