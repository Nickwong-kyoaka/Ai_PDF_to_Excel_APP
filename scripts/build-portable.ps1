param(
    [switch]$SkipTests,
    [switch]$SkipDependencyInstall
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnvironmentRoot = Join-Path $ProjectRoot ".desktop-venv"
$Python = Join-Path $EnvironmentRoot "Scripts\python.exe"
$ReleaseRoot = Join-Path $ProjectRoot "release"
$BuildWork = Join-Path $ProjectRoot "build\portable"
$PortableExe = Join-Path $ReleaseRoot "FormSight-Local-Portable.exe"

if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "Desktop build environment not found. Run Build-FormSight-Local.bat once on the build PC."
}
if (-not $SkipDependencyInstall) {
    & $Python -m pip install -r (Join-Path $ProjectRoot "desktop\requirements-desktop.txt")
    if ($LASTEXITCODE -ne 0) { throw "Portable build dependency installation failed." }
}
if (-not $SkipTests) {
    Push-Location $ProjectRoot
    try {
        $TestTemp = Join-Path $ProjectRoot ("build\portable-test-temp-" + [guid]::NewGuid().ToString("N"))
        New-Item -ItemType Directory -Path $TestTemp -Force | Out-Null
        & $Python -m pytest desktop\tests -q --basetemp $TestTemp
        if ($LASTEXITCODE -ne 0) { throw "Desktop tests failed." }
    } finally {
        Pop-Location
    }
}

New-Item -ItemType Directory -Path $ReleaseRoot -Force | Out-Null
if (Test-Path -LiteralPath $PortableExe) {
    Remove-Item -LiteralPath $PortableExe -Force
}
Push-Location $ProjectRoot
try {
    & $Python -m PyInstaller --noconfirm --clean `
        --distpath $ReleaseRoot `
        --workpath $BuildWork `
        desktop\formsight-local-portable.spec
    if ($LASTEXITCODE -ne 0) { throw "Portable PyInstaller build failed." }
} finally {
    Pop-Location
}

if (-not (Test-Path -LiteralPath $PortableExe -PathType Leaf)) {
    throw "Portable executable was not created: $PortableExe"
}
$Portable = Get-Item -LiteralPath $PortableExe
$Hash = (Get-FileHash -LiteralPath $PortableExe -Algorithm SHA256).Hash
Write-Host "Portable executable created: $($Portable.FullName)" -ForegroundColor Green
Write-Host "Size: $([math]::Round($Portable.Length / 1MB, 1)) MiB"
Write-Host "SHA256: $Hash"
