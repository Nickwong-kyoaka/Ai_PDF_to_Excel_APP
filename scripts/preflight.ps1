$ErrorActionPreference = "Continue"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$BackendEnv = Join-Path $Root "backend\.env"
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
    $models = Invoke-RestMethod -Uri "http://127.0.0.1:1234/v1/models" -TimeoutSec 5
    Write-Host "LM Studio online: $($models.data.id -join ', ')" -ForegroundColor Green
} catch { Write-Warning "LM Studio is not reachable on loopback port 1234. Start it with authentication enabled." }

$drive = Get-PSDrive -Name ([IO.Path]::GetPathRoot($Root).TrimEnd('\').TrimEnd(':'))
Write-Host "Free disk: $([math]::Round($drive.Free / 1GB, 1)) GB"
