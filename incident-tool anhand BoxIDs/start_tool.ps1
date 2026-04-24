$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    throw "Die lokale .venv fehlt. Bitte zuerst die Umgebung neu aufbauen."
}

$python = Join-Path $root ".venv\Scripts\python.exe"
$appPath = Join-Path $root "modern_incident_tool\app.py"
$url = "http://localhost:8501/"
$expectedMarker = "Factor Incident Tool v2"

$appReachable = $false
$foreignServiceDetected = $false
try {
    $response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2
    if ($response.Content -match [regex]::Escape($expectedMarker)) {
        $appReachable = $true
    }
    else {
        $foreignServiceDetected = $true
    }
} catch {
    $appReachable = $false
}

if ($foreignServiceDetected) {
    throw "Port 8501 antwortet bereits, aber nicht mit dem Incident Tool. Wahrscheinlich laeuft dort PDL Fast ueber einen alten Launcher. Bitte diese Instanz schliessen und das Incident Tool erneut starten."
}

if (-not $appReachable) {
    Start-Process -FilePath $python -ArgumentList @(
        "-m",
        "streamlit",
        "run",
        $appPath,
        "--server.port",
        "8501",
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false"
    ) -WorkingDirectory $root | Out-Null

    for ($i = 0; $i -lt 20; $i++) {
        try {
            $response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2
            if ($response.Content -match [regex]::Escape($expectedMarker)) {
                $appReachable = $true
                break
            }
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
}

Start-Process $url | Out-Null

if (-not $appReachable) {
    Write-Warning "Die Browser-Oberflaeche wurde geoeffnet, aber der lokale Server antwortet noch nicht. Bitte 2-3 Sekunden warten und die Seite neu laden."
}