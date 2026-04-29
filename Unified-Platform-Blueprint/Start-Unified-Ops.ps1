$ErrorActionPreference = "Stop"

$launcherRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$workspaceRoot = Split-Path -Parent $launcherRoot
$dashboardPath = Join-Path $launcherRoot "dashboard.html"
$resultsPath = Join-Path $launcherRoot "results"
$configPath = Join-Path $launcherRoot "platform.config.json"
$logsPath = Join-Path $launcherRoot "logs"
$launcherHadFailures = $false

New-Item -ItemType Directory -Path $logsPath -Force | Out-Null

$config = $null
if (Test-Path $configPath) {
    $config = Get-Content $configPath -Raw | ConvertFrom-Json
    if ($config.paths.resultsRoot) {
        $resultsPath = $config.paths.resultsRoot
    }
}

function Get-ServiceConfigValue {
    param(
        [string]$ServiceKey,
        [string]$PropertyName,
        [string]$FallbackValue
    )

    if ($config -and $config.services -and $config.services.$ServiceKey -and $config.services.$ServiceKey.$PropertyName) {
        return $config.services.$ServiceKey.$PropertyName
    }

    return $FallbackValue
}

function Test-HttpEndpoint {
    param(
        [string]$Url,
        [int]$TimeoutSec = 2
    )

    try {
        Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSec | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

function Wait-HttpEndpoint {
    param(
        [string]$Url,
        [int]$Attempts = 40,
        [int]$DelayMs = 500
    )

    for ($i = 0; $i -lt $Attempts; $i++) {
        if (Test-HttpEndpoint -Url $Url) {
            return $true
        }
        Start-Sleep -Milliseconds $DelayMs
    }

    return $false
}

function Test-PythonStreamlitCommand {
    param(
        [string]$PythonCommand,
        [string[]]$PythonArguments = @()
    )

    try {
        $null = & $PythonCommand @PythonArguments -m streamlit --version 2>$null
        return ($LASTEXITCODE -eq 0)
    }
    catch {
        return $false
    }
}

function Get-StreamlitPythonCommand {
    param(
        [string]$WorkingDirectory,
        [string[]]$PreferredLaunchers = @("py", "python")
    )

    $venvCandidates = @(
        (Join-Path $WorkingDirectory ".venv\Scripts\python.exe"),
        (Join-Path $WorkingDirectory "venv\Scripts\python.exe")
    )

    foreach ($venvPython in $venvCandidates) {
        if ((Test-Path $venvPython) -and (Test-PythonStreamlitCommand -PythonCommand $venvPython)) {
            return @{ Command = $venvPython; Arguments = @() }
        }
    }

    $launcherVariants = @(
        @{ Command = "py"; Arguments = @("-3.12") },
        @{ Command = "py"; Arguments = @("-3") },
        @{ Command = "python"; Arguments = @() }
    )

    foreach ($variant in $launcherVariants) {
        if ($PreferredLaunchers -notcontains $variant.Command -and $variant.Command -ne "python") {
            continue
        }

        if (Test-PythonStreamlitCommand -PythonCommand $variant.Command -PythonArguments $variant.Arguments) {
            return $variant
        }
    }

    return $null
}

function Start-BackgroundPowerShell {
    param(
        [string]$WorkingDirectory,
        [string]$Command,
        [string]$LogName
    )

    $stdoutPath = Join-Path $logsPath ("{0}.out.log" -f $LogName)
    $stderrPath = Join-Path $logsPath ("{0}.err.log" -f $LogName)

    Start-Process -FilePath "powershell.exe" -ArgumentList @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-Command", $Command
    ) -WorkingDirectory $WorkingDirectory -WindowStyle Hidden -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath | Out-Null
}

$services = @(
    @{
        Name = "PS Copilot Hub"
        Url = (Get-ServiceConfigValue -ServiceKey "hub" -PropertyName "healthUrl" -FallbackValue "http://127.0.0.1:3020/api/v1/health")
        Launch = {
            $workingDirectory = Join-Path $workspaceRoot "PS Copilot\ps-basecamp-backend"
            $command = "Set-Location '$workingDirectory'; node server.js"
            Start-BackgroundPowerShell -WorkingDirectory $workingDirectory -Command $command -LogName "ps-copilot-hub"
        }
    },
    @{
        Name = "Incident Tool"
        Url = (Get-ServiceConfigValue -ServiceKey "incident" -PropertyName "healthUrl" -FallbackValue "http://127.0.0.1:8501/")
        Launch = {
            $workingDirectory = Join-Path $workspaceRoot "incident-tool anhand BoxIDs"
            $pythonSpec = Get-StreamlitPythonCommand -WorkingDirectory $workingDirectory
            if (-not $pythonSpec) {
                throw "Incident Tool konnte nicht gestartet werden: kein Python mit installiertem Streamlit gefunden."
            }
            $pythonPrefix = @($pythonSpec.Command) + $pythonSpec.Arguments | ForEach-Object { "'$_'" }
            $command = "Set-Location '$workingDirectory'; & " + ($pythonPrefix -join " ") + " -m streamlit run 'modern_incident_tool\app.py' --server.port 8501 --server.headless true --browser.gatherUsageStats false"
            Start-BackgroundPowerShell -WorkingDirectory $workingDirectory -Command $command -LogName "incident-tool"
        }
    },
    @{
        Name = "PDL Fast"
        Url = (Get-ServiceConfigValue -ServiceKey "pdl" -PropertyName "healthUrl" -FallbackValue "http://127.0.0.1:8502/")
        Launch = {
            $workingDirectory = Join-Path $workspaceRoot "PDL fast"
            $pythonSpec = Get-StreamlitPythonCommand -WorkingDirectory $workingDirectory
            if (-not $pythonSpec) {
                throw "PDL Fast konnte nicht gestartet werden: kein Python mit installiertem Streamlit gefunden."
            }
            $pythonPrefix = @($pythonSpec.Command) + $pythonSpec.Arguments | ForEach-Object { "'$_'" }
            $command = "Set-Location '$workingDirectory'; & " + ($pythonPrefix -join " ") + " -m streamlit run 'app.py' --server.port 8502 --server.headless true --browser.gatherUsageStats false"
            Start-BackgroundPowerShell -WorkingDirectory $workingDirectory -Command $command -LogName "pdl-fast"
        }
    },
    @{
        Name = "Waagen Performance"
        Url = (Get-ServiceConfigValue -ServiceKey "waagen" -PropertyName "healthUrl" -FallbackValue "http://127.0.0.1:8505/")
        Launch = {
            $workingDirectory = Join-Path $workspaceRoot "Waagen Performance"
            $pythonSpec = Get-StreamlitPythonCommand -WorkingDirectory $workingDirectory
            if (-not $pythonSpec) {
                throw "Waagen Performance konnte nicht gestartet werden: kein Python mit installiertem Streamlit gefunden."
            }
            $pythonPrefix = @($pythonSpec.Command) + $pythonSpec.Arguments | ForEach-Object { "'$_'" }
            $command = "Set-Location '$workingDirectory'; & " + ($pythonPrefix -join " ") + " -m streamlit run 'weekly_bug_report.py' --server.port 8505 --server.headless true --browser.gatherUsageStats false"
            Start-BackgroundPowerShell -WorkingDirectory $workingDirectory -Command $command -LogName "waagen-performance"
        }
    }
)

$failedServices = @()
foreach ($service in $services) {
    if (-not (Test-HttpEndpoint -Url $service.Url)) {
        try {
            & $service.Launch
            if (-not (Wait-HttpEndpoint -Url $service.Url)) {
                $failedServices += $service.Name
            }
        }
        catch {
            $failedServices += "$($service.Name) ($($_.Exception.Message))"
        }
    }
}

if ((-not $config) -or $config.launcher.autoOpenResults) {
    if (Test-Path $resultsPath) {
        Start-Process explorer.exe -ArgumentList $resultsPath | Out-Null
    }
}

if ((-not $config) -or $config.launcher.autoOpenDashboard) {
    if (Test-Path $dashboardPath) {
        Start-Process $dashboardPath | Out-Null
    }
    else {
        Start-Process "http://127.0.0.1:3020" | Out-Null
    }
}

if ($failedServices.Count -gt 0) {
    $launcherHadFailures = $true
    Write-Warning ("Diese Services konnten nicht bestaetigt gestartet werden: " + ($failedServices -join ", "))
    Write-Warning ("Details stehen unter: " + $logsPath)
}

if ($launcherHadFailures) {
    exit 1
}
