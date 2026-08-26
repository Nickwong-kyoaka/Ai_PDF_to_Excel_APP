[CmdletBinding()]
param([switch]$NoStart)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Backend = Join-Path $Root "backend"
$BackendEnv = Join-Path $Backend ".env"
$FrontendEnv = Join-Path $Root ".env"
$Database = Join-Path $Backend "data\formsight.db"
$Encoding = New-Object System.Text.UTF8Encoding($false)
$adminPassword = $null
$confirmation = $null
$lmToken = $null

function Read-DefaultValue {
    param([string]$Prompt, [string]$Default)
    $value = Read-Host "$Prompt [$Default]"
    if ([string]::IsNullOrWhiteSpace($value)) { return $Default }
    return $value.Trim()
}

function Read-YesNo {
    param([string]$Prompt, [bool]$Default = $true)
    $suffix = if ($Default) { "Y/n" } else { "y/N" }
    while ($true) {
        $answer = (Read-Host "$Prompt [$suffix]").Trim().ToLowerInvariant()
        if (-not $answer) { return $Default }
        if ($answer -in @("y", "yes")) { return $true }
        if ($answer -in @("n", "no")) { return $false }
        Write-Warning "Enter Y or N."
    }
}

function ConvertFrom-SecureValue {
    param([Security.SecureString]$Value)
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer) }
}

function Read-RequiredSecret {
    param([string]$Prompt, [int]$MinimumLength = 1)
    while ($true) {
        $plain = ConvertFrom-SecureValue (Read-Host $Prompt -AsSecureString)
        if ($plain.Length -ge $MinimumLength -and $plain -notmatch "[`r`n]") { return $plain }
        Write-Warning "The value must contain at least $MinimumLength characters and no line breaks."
    }
}

function ConvertTo-Base64Utf8 {
    param([string]$Value)
    return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value))
}

function Format-DotEnvValue {
    param([string]$Value)
    if ($Value -match "[`r`n']") { throw "A configuration value contains an unsupported quote or line break." }
    return "'$Value'"
}

function Write-Utf8Lines {
    param([string]$Path, [string[]]$Lines)
    [IO.File]::WriteAllLines($Path, $Lines, $Encoding)
}

function Get-RequiredCommand {
    param([string]$Name, [string]$InstallHint)
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if (-not $command) { throw "$Name is required. $InstallHint" }
    return $command.Source
}

function Get-CompatiblePython {
    $candidates = @()
    $direct = Get-Command "python" -ErrorAction SilentlyContinue
    if ($direct) { $candidates += $direct.Source }
    $launcher = Get-Command "py" -ErrorAction SilentlyContinue
    if ($launcher) {
        try {
            $launchedPath = (& $launcher.Source -3 -c "import sys; print(sys.executable)" 2>$null | Select-Object -Last 1).Trim()
            if ($launchedPath) { $candidates += $launchedPath }
        } catch {}
    }
    foreach ($candidate in ($candidates | Select-Object -Unique)) {
        try {
            $versionText = (& $candidate -c "import platform; print(platform.python_version())" 2>$null | Select-Object -Last 1).Trim()
            if ($versionText -and ([Version]$versionText) -ge [Version]"3.11") { return $candidate }
        } catch {}
    }
    throw "Python 3.11 or newer is required. Install it and enable the Python launcher or PATH option."
}

function Test-LMStudio {
    param([string]$BaseUrl, [string]$Token)
    $headers = @{ Authorization = "Bearer $Token" }
    try {
        $response = Invoke-RestMethod -Uri "$($BaseUrl.TrimEnd('/'))/models" -Headers $headers -TimeoutSec 8
        return @($response.data | ForEach-Object { $_.id })
    } catch {
        return $null
    }
}

function Wait-ForEndpoint {
    param([string]$Url, [int]$Seconds = 30)
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 3
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) { return $true }
        } catch {}
        Start-Sleep -Seconds 1
    }
    return $false
}

try {
    Clear-Host
    Write-Host "FormSight one-click deployment" -ForegroundColor Cyan
    Write-Host "This PC will host the web app, worker, YOLO, and LM Studio."
    Write-Host "Secrets are written only to ignored local .env files."
    Write-Host ""

    if (Test-Path -LiteralPath $Database) {
        Write-Warning "An existing FormSight database was found. Accounts, jobs, and model profiles will be preserved."
        if (-not (Read-YesNo "Repair/update dependencies and restart this deployment?" $true)) { exit 0 }
        $pythonPath = Get-CompatiblePython
        $withML = Read-YesNo "Install/update YOLO GPU dependencies?" $true
        & (Join-Path $PSScriptRoot "stop.ps1")
        if ($withML) { & (Join-Path $PSScriptRoot "install.ps1") -Python $pythonPath -WithML }
        else { & (Join-Path $PSScriptRoot "install.ps1") -Python $pythonPath }
        & (Join-Path $PSScriptRoot "preflight.ps1")
        if (-not $NoStart) {
            & (Join-Path $PSScriptRoot "start.ps1")
        }
        Write-Host "Repair/update completed." -ForegroundColor Green
        exit 0
    }

    if ($PSVersionTable.PSVersion.Major -lt 5) { throw "Windows PowerShell 5.1 or newer is required." }
    $pythonPath = Get-CompatiblePython
    $nodePath = Get-RequiredCommand "node" "Install Node.js 22.13 or newer."
    $nodeVersion = [Version]((& $nodePath --version).TrimStart("v"))
    if ($nodeVersion -lt [Version]"22.13") { throw "Node.js 22.13 or newer is required; found $nodeVersion." }
    [void](Get-RequiredCommand "npm" "Install Node.js 22.13 or newer.")

    $gpu = Get-Command "nvidia-smi.exe" -ErrorAction SilentlyContinue
    if (-not $gpu -and -not (Read-YesNo "NVIDIA tools were not found. Continue in Qwen/CPU setup mode?" $false)) {
        throw "Install the tested NVIDIA driver before deployment."
    }

    $adminEmail = Read-DefaultValue "Administrator email" "admin@formsight.local"
    try { $mail = [Net.Mail.MailAddress]$adminEmail } catch { throw "Administrator email is invalid." }
    if ($mail.Address -ne $adminEmail) { throw "Administrator email is invalid." }

    while ($true) {
        $adminPassword = Read-RequiredSecret "Administrator password (minimum 12 characters)" 12
        $confirmation = Read-RequiredSecret "Confirm administrator password" 12
        if ($adminPassword -ceq $confirmation) { break }
        Write-Warning "Passwords do not match."
    }

    $lmBaseUrl = Read-DefaultValue "LM Studio API URL" "http://127.0.0.1:1234/v1"
    $lmUri = [Uri]$lmBaseUrl
    if ($lmUri.Host -notin @("127.0.0.1", "localhost", "::1")) { throw "LM Studio must use a loopback address." }
    $lmToken = Read-RequiredSecret "LM Studio API token" 8
    $extractorModel = Read-DefaultValue "Qwen vision model ID" "qwen/qwen3-vl-8b"
    $judgeModel = Read-DefaultValue "Qwen reasonableness model ID" "qwen/qwen3-8b"
    $quantization = Read-DefaultValue "Model quantization label" "Q4_K_M"

    $enableHttps = Read-YesNo "Enable LAN/VPN HTTPS through Caddy?" $true
    if ($enableHttps) {
        [void](Get-RequiredCommand "caddy" "Install Caddy and make caddy.exe available on PATH, or choose local-only mode.")
        $publicHost = Read-DefaultValue "Internal DNS hostname" "formsight.internal"
        if ([Uri]::CheckHostName($publicHost) -eq [UriHostNameType]::Unknown -or $publicHost -match "[/\\:]" ) {
            throw "Enter a hostname only, without https://, a path, or a port."
        }
        $siteOrigin = "https://$publicHost"
        $apiUrl = "/api"
    } else {
        $publicHost = "127.0.0.1"
        $siteOrigin = "http://127.0.0.1:3000"
        $apiUrl = "http://127.0.0.1:8000/api"
        Write-Warning "Local-only mode is not reachable from other PCs. Rerun with Caddy for LAN/VPN access."
    }

    $withML = Read-YesNo "Install YOLO GPU dependencies?" $true
    $weightsSource = (Read-Host "Accepted YOLO .onnx weights path (leave blank for Qwen-only mode)").Trim().Trim('"')
    if ($weightsSource) {
        $weightsSource = (Resolve-Path -LiteralPath $weightsSource).Path
        if ([IO.Path]::GetExtension($weightsSource) -ne ".onnx") { throw "YOLO weights must be an .onnx file." }
    } elseif (-not (Read-YesNo "Continue without YOLO weights? Selection fusion will be unavailable." $false)) {
        throw "Provide accepted YOLO weights or explicitly choose Qwen-only mode."
    }

    $backupRoot = Join-Path $Root ("backups\config-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
    if (Test-Path -LiteralPath $BackendEnv) {
        New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null
        Copy-Item -LiteralPath $BackendEnv -Destination (Join-Path $backupRoot "backend.env") -Force
    }
    if (Test-Path -LiteralPath $FrontendEnv) {
        New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null
        Copy-Item -LiteralPath $FrontendEnv -Destination (Join-Path $backupRoot "frontend.env") -Force
    }

    $passwordB64 = ConvertTo-Base64Utf8 $adminPassword
    $tokenB64 = ConvertTo-Base64Utf8 $lmToken
    $backendLines = @(
        "FORMSIGHT_ENVIRONMENT=production",
        "FORMSIGHT_DATA_DIR=./data",
        "FORMSIGHT_DATABASE_URL=sqlite:///./data/formsight.db",
        "FORMSIGHT_FRONTEND_ORIGINS=$(Format-DotEnvValue $siteOrigin)",
        "FORMSIGHT_COOKIE_SECURE=$($enableHttps.ToString().ToLowerInvariant())",
        "FORMSIGHT_BOOTSTRAP_ADMIN_EMAIL=$(Format-DotEnvValue $adminEmail)",
        "FORMSIGHT_BOOTSTRAP_ADMIN_PASSWORD=",
        "FORMSIGHT_BOOTSTRAP_ADMIN_PASSWORD_B64=$(Format-DotEnvValue $passwordB64)",
        "FORMSIGHT_LMSTUDIO_BASE_URL=$(Format-DotEnvValue $lmBaseUrl)",
        "FORMSIGHT_LMSTUDIO_API_KEY=",
        "FORMSIGHT_LMSTUDIO_API_KEY_B64=$(Format-DotEnvValue $tokenB64)",
        "FORMSIGHT_EXTRACTOR_MODEL_ID=$(Format-DotEnvValue $extractorModel)",
        "FORMSIGHT_JUDGE_MODEL_ID=$(Format-DotEnvValue $judgeModel)",
        "FORMSIGHT_MODEL_QUANTIZATION=$(Format-DotEnvValue $quantization)",
        "FORMSIGHT_LEGACY_V14_PATH=../universal_questionnaire_lmstudio_extractor_v14_consensus_geometry.py",
        "FORMSIGHT_YOLO_WEIGHTS=./models/questionnaire_marks.onnx",
        "FORMSIGHT_RETENTION_DAYS=30",
        "FORMSIGHT_MAX_UPLOAD_MB=250",
        "FORMSIGHT_MAX_PAGES=500"
    )
    Write-Utf8Lines $BackendEnv $backendLines
    Write-Utf8Lines $FrontendEnv @("NEXT_PUBLIC_API_URL=$apiUrl", "NEXT_PUBLIC_SITE_ORIGIN=$siteOrigin")

    if ($enableHttps) {
        $caddyConfig = @"
$publicHost {
    tls internal
    encode zstd gzip
    handle /api/* { reverse_proxy 127.0.0.1:8000 }
    handle { reverse_proxy 127.0.0.1:3000 }
    header {
        Strict-Transport-Security "max-age=31536000"
        X-Content-Type-Options "nosniff"
        Referrer-Policy "same-origin"
        Permissions-Policy "camera=(), microphone=(), geolocation=()"
    }
}
"@
        [IO.File]::WriteAllText((Join-Path $Root "Caddyfile"), $caddyConfig, $Encoding)
    }

    Write-Host "Installing application dependencies. This may take several minutes..." -ForegroundColor Cyan
    if ($withML) { & (Join-Path $PSScriptRoot "install.ps1") -Python $pythonPath -WithML }
    else { & (Join-Path $PSScriptRoot "install.ps1") -Python $pythonPath }

    if ($weightsSource) {
        $weightsTarget = Join-Path $Backend "models\questionnaire_marks.onnx"
        if ($weightsSource -ne $weightsTarget) { Copy-Item -LiteralPath $weightsSource -Destination $weightsTarget -Force }
        Write-Host "YOLO weights installed." -ForegroundColor Green
    }

    $models = Test-LMStudio $lmBaseUrl $lmToken
    if ($null -eq $models) {
        Write-Warning "LM Studio is not reachable or rejected the token. Start its authenticated server on $lmBaseUrl."
        if (-not (Read-YesNo "Start FormSight anyway?" $false)) { throw "LM Studio validation did not pass." }
    } else {
        foreach ($requiredModel in @($extractorModel, $judgeModel)) {
            if ($models -notcontains $requiredModel) { Write-Warning "LM Studio does not currently report model: $requiredModel" }
        }
    }

    & (Join-Path $PSScriptRoot "preflight.ps1") -BaseUrl $lmBaseUrl -Token $lmToken -RequiredModels @($extractorModel, $judgeModel)
    if (-not $NoStart) {
        & (Join-Path $PSScriptRoot "start.ps1") -SkipLMStudio
        $apiReady = Wait-ForEndpoint "http://127.0.0.1:8000/api/health"
        $webReady = Wait-ForEndpoint "http://127.0.0.1:3000/"
        if (-not $apiReady -or -not $webReady) { throw "FormSight processes started, but the health check did not pass. Run scripts\status.ps1." }
    }

    if (Read-YesNo "Register automatic startup and daily 30-day cleanup?" $true) {
        $principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
        $registerScript = Join-Path $PSScriptRoot "register-startup.ps1"
        if ($principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { & $registerScript }
        else {
            $arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$registerScript`""
            $process = Start-Process -FilePath "powershell.exe" -ArgumentList $arguments -Verb RunAs -Wait -PassThru
            if ($process.ExitCode -ne 0) { Write-Warning "Startup task registration was cancelled or failed." }
        }
    }

    $adminPassword = $null
    $confirmation = $null
    $lmToken = $null
    Write-Host ""
    Write-Host "FormSight deployment completed." -ForegroundColor Green
    Write-Host "Address: $siteOrigin"
    Write-Host "Administrator: $adminEmail"
    if ($enableHttps) { Write-Host "Trust the Caddy internal CA on each managed client PC before opening the site." }
    if (-not $NoStart -and (Read-YesNo "Open FormSight now?" $true)) { Start-Process $siteOrigin }
    exit 0
} catch {
    $adminPassword = $null
    $confirmation = $null
    $lmToken = $null
    Write-Host ""
    Write-Host "Deployment failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Fix the item above and double-click Deploy-FormSight.bat again."
    exit 1
}
