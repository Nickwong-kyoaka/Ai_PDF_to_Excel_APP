param(
    [switch]$SkipTests,
    [switch]$SkipDependencyInstall,
    [switch]$AppOnly,
    [switch]$InstallerOnly
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnvironmentRoot = Join-Path $ProjectRoot ".desktop-venv"
$Python = Join-Path $EnvironmentRoot "Scripts\python.exe"
$ReleaseRoot = Join-Path $ProjectRoot "release"
$ApplicationOutput = Join-Path $ReleaseRoot "app"
$BuildWork = Join-Path $ProjectRoot "build\desktop"

function Find-CompatiblePython {
    $candidate = Get-Command python -ErrorAction SilentlyContinue
    if ($candidate) {
        try {
            $version = & $candidate.Source -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
            if ($version.Trim() -in @("3.11", "3.12")) { return $candidate.Source }
        } catch {
            # Continue to the Python launcher.
        }
    }
    $launcher = Get-Command py -ErrorAction SilentlyContinue
    if ($launcher) {
        try {
            $null = & py -3.11 -c "import sys; print(sys.executable)" 2>$null
            if ($LASTEXITCODE -eq 0) { return "py -3.11" }
        } catch {
            # Continue to a directly installed python.exe.
        }
    }
    throw "64-bit Python 3.11 or 3.12 is required on the build PC. It is bundled into the finished application."
}

if (-not $InstallerOnly) {
    if (-not (Test-Path -LiteralPath $Python)) {
        $BootstrapPython = Find-CompatiblePython
        if ($BootstrapPython -eq "py -3.11") {
            & py -3.11 -m venv $EnvironmentRoot
        } else {
            & $BootstrapPython -m venv $EnvironmentRoot
        }
    }

    if (-not $SkipDependencyInstall) {
        & $Python -m pip install --upgrade pip
        & $Python -m pip install -r (Join-Path $ProjectRoot "desktop\requirements-desktop.txt")
    }

    if (-not $SkipTests) {
        Push-Location $ProjectRoot
        try {
            # The desktop environment intentionally excludes FastAPI; web tests run in backend\.venv.
            # A fresh base avoids Windows file-handle races from a previous pytest run.
            $TestTemp = Join-Path $ProjectRoot ("build\desktop-test-temp-" + [guid]::NewGuid().ToString("N"))
            New-Item -ItemType Directory -Path $TestTemp -Force | Out-Null
            & $Python -m pytest desktop\tests -q --basetemp $TestTemp
            if ($LASTEXITCODE -ne 0) { throw "Desktop tests failed." }
        } finally {
            Pop-Location
        }
    }

    if (Test-Path -LiteralPath $ApplicationOutput) {
        $ResolvedRelease = [IO.Path]::GetFullPath($ReleaseRoot)
        $ResolvedOutput = [IO.Path]::GetFullPath($ApplicationOutput)
        if (-not $ResolvedOutput.StartsWith($ResolvedRelease + [IO.Path]::DirectorySeparatorChar)) {
            throw "Refusing to clear an application output outside the release directory."
        }
        Remove-Item -LiteralPath $ResolvedOutput -Recurse -Force
    }
    New-Item -ItemType Directory -Path $ApplicationOutput -Force | Out-Null

    Push-Location $ProjectRoot
    try {
        & $Python -m PyInstaller --noconfirm --clean `
            --distpath $ApplicationOutput `
            --workpath $BuildWork `
            desktop\formsight-local.spec
        if ($LASTEXITCODE -ne 0) { throw "PyInstaller build failed." }
    } finally {
        Pop-Location
    }
}

$Executable = Join-Path $ApplicationOutput "FormSightLocal\FormSightLocal.exe"
if (-not (Test-Path -LiteralPath $Executable)) {
    throw "PyInstaller did not create $Executable"
}

if ($AppOnly) {
    Write-Host "Desktop application created: $Executable" -ForegroundColor Green
    exit 0
}

$InnoCandidates = @(
    (Join-Path $env:LOCALAPPDATA "Programs\Inno Setup 6\ISCC.exe"),
    (Join-Path ${env:ProgramFiles(x86)} "Inno Setup 6\ISCC.exe"),
    (Join-Path $env:ProgramFiles "Inno Setup 6\ISCC.exe"),
    (Get-Command iscc.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -ErrorAction SilentlyContinue)
) | Where-Object { $_ -and (Test-Path -LiteralPath $_) }
$InnoCompiler = $InnoCandidates | Select-Object -First 1
if (-not $InnoCompiler) {
    throw "Inno Setup 6 was not found. Install it on the build PC, then rerun this script."
}

$Setup = Join-Path $ReleaseRoot "FormSight-Local-Setup.exe"
if (Test-Path -LiteralPath $Setup) {
    Remove-Item -LiteralPath $Setup -Force
}
& $InnoCompiler "/DSourceRoot=$ProjectRoot" (Join-Path $ProjectRoot "desktop\installer.iss")
if ($LASTEXITCODE -ne 0) { throw "Inno Setup compilation failed." }

if (-not (Test-Path -LiteralPath $Setup)) { throw "Installer was not created." }
Write-Host "Installer created: $Setup" -ForegroundColor Green
& (Join-Path $PSScriptRoot "package-transfer.ps1") -SetupPath $Setup
if ($LASTEXITCODE -ne 0) { throw "Transfer ZIP packaging failed." }
