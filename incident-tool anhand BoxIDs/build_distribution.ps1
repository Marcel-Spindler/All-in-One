# ============================================================
#  Verteilungspaket (ZIP) fuer das Factor Incident Tool v2 bauen
#  Erzeugt eine saubere ZIP-Datei ohne .venv und ohne private
#  Daten, die direkt an Mitarbeiter weitergegeben werden kann.
# ============================================================
param(
    [string]$OutputDir = (Join-Path $PSScriptRoot ".."),
    [switch]$IncludeImports,
    [switch]$IncludeExports
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$stamp = Get-Date -Format "yyyy-MM-dd"
$staging = Join-Path $env:TEMP "incident-tool-dist-$stamp"
$zipName = "incident-tool_v2_$stamp.zip"
$zipPath = Join-Path (Resolve-Path $OutputDir) $zipName

if (Test-Path $staging) { Remove-Item $staging -Recurse -Force }
New-Item -ItemType Directory -Path $staging | Out-Null

$exclude = @(
    ".venv",
    "__pycache__",
    ".streamlit\secrets.toml",
    "data\incident_draft_v2.json"
)

Write-Host "[Pack] Kopiere Dateien nach $staging ..."
robocopy $root $staging /MIR `
    /XD ".venv" "__pycache__" ".vscode" `
    /XF "*.pyc" "incident_draft_v2.json" "Thumbs.db" "desktop.ini" `
    | Out-Null

if (-not $IncludeImports) {
    $imp = Join-Path $staging "Imports"
    if (Test-Path $imp) {
        Get-ChildItem $imp -File | Remove-Item -Force
    }
}
if (-not $IncludeExports) {
    $exp = Join-Path $staging "Exports"
    if (Test-Path $exp) {
        Get-ChildItem $exp -File | Remove-Item -Force
    }
}

if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Write-Host "[Pack] Erstelle ZIP: $zipPath"
Compress-Archive -Path (Join-Path $staging "*") -DestinationPath $zipPath -CompressionLevel Optimal

Remove-Item $staging -Recurse -Force
Write-Host ""
Write-Host "[OK] Fertig: $zipPath"
Write-Host "    -> Diese ZIP an Mitarbeiter weitergeben."
Write-Host "    -> Auf Zielrechner entpacken und 'setup_and_start.bat' starten."
