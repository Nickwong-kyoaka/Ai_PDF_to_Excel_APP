param(
    [string]$SetupPath,
    [string]$OutputPath
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ReleaseRoot = Join-Path $ProjectRoot "release"
$BuildRoot = Join-Path $ProjectRoot "build"
$StagingRoot = Join-Path $BuildRoot "transfer-package"

if (-not $SetupPath) {
    $SetupPath = Join-Path $ReleaseRoot "FormSight-Local-Setup.exe"
}
if (-not (Test-Path -LiteralPath $SetupPath -PathType Leaf)) {
    throw "Installer not found: $SetupPath. Run Build-FormSight-Local.bat first."
}

$VersionSource = Get-Content -LiteralPath (Join-Path $ProjectRoot "desktop\__init__.py") -Raw
$VersionMatch = [regex]::Match($VersionSource, '__version__\s*=\s*"([^"]+)"')
if (-not $VersionMatch.Success) { throw "Could not read the desktop application version." }
$Version = $VersionMatch.Groups[1].Value
if (-not $OutputPath) {
    $OutputPath = Join-Path $ReleaseRoot "FormSight-Local-v$Version-Transfer.zip"
}

$ResolvedBuild = [IO.Path]::GetFullPath($BuildRoot)
$ResolvedStaging = [IO.Path]::GetFullPath($StagingRoot)
if (-not $ResolvedStaging.StartsWith($ResolvedBuild + [IO.Path]::DirectorySeparatorChar)) {
    throw "Refusing to clear a staging directory outside the build directory."
}
if (Test-Path -LiteralPath $StagingRoot) {
    Remove-Item -LiteralPath $StagingRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $StagingRoot -Force | Out-Null

$StagedSetup = Join-Path $StagingRoot "FormSight-Local-Setup.exe"
Copy-Item -LiteralPath $SetupPath -Destination $StagedSetup
Copy-Item -LiteralPath (Join-Path $ProjectRoot "desktop\TRANSFER-README.txt") `
    -Destination (Join-Path $StagingRoot "README-FIRST.txt")

$SetupHash = (Get-FileHash -LiteralPath $StagedSetup -Algorithm SHA256).Hash
Set-Content -LiteralPath (Join-Path $StagingRoot "SHA256SUMS.txt") `
    -Value "$SetupHash  FormSight-Local-Setup.exe" -Encoding ascii

if (Test-Path -LiteralPath $OutputPath) {
    Remove-Item -LiteralPath $OutputPath -Force
}
Compress-Archive -Path (Join-Path $StagingRoot "*") -DestinationPath $OutputPath -CompressionLevel Optimal

$Zip = Get-Item -LiteralPath $OutputPath
$ZipHash = (Get-FileHash -LiteralPath $OutputPath -Algorithm SHA256).Hash
Write-Host "Transfer package created: $($Zip.FullName)" -ForegroundColor Green
Write-Host "Size: $([math]::Round($Zip.Length / 1MB, 1)) MiB"
Write-Host "ZIP SHA256: $ZipHash"
