$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    throw "Die lokale .venv fehlt. Bitte zuerst das Environment aufbauen."
}

$env:STREAMLIT_BROWSER_GATHER_USAGE_STATS = "false"

& ".venv\Scripts\python.exe" -m streamlit run ".\modern_incident_tool\app.py" --browser.gatherUsageStats false