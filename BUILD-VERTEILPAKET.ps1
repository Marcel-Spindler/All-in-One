# ============================================================
#  All in One - Verteilungspaket bauen
#  Erstellt eine saubere ZIP-Datei aller 4 Tools, die direkt
#  an Mitarbeiter weitergegeben werden kann.
#
#  Ausgeschlossen werden automatisch:
#   - .venv  (wird auf Zielrechner neu gebaut)
#   - node_modules  (wird via "npm install" neu erzeugt)
#   - .git
#   - Caches, persoenliche Drafts, .env, Service-Account-Keys
#   - alte Imports/Exports
# ============================================================
param(
    [string]$OutputDir = $PSScriptRoot,
    [switch]$IncludeData,
    [switch]$IncludeSecrets
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$stamp = Get-Date -Format "yyyy-MM-dd"
$staging = Join-Path $env:TEMP "all-in-one-dist-$stamp"
$zipName = "All-in-One_$stamp.zip"
$zipPath = Join-Path (Resolve-Path $OutputDir) $zipName

if (Test-Path $staging) { Remove-Item $staging -Recurse -Force }
New-Item -ItemType Directory -Path $staging | Out-Null

Write-Host "[Pack] Spiegele Workspace nach $staging ..."
$excludeDirs = @(
    ".venv", ".git", "node_modules", "__pycache__", ".vscode",
    ".pytest_cache", ".mypy_cache"
)
$excludeFiles = @(
    "*.pyc", "Thumbs.db", "desktop.ini",
    ".pdl_fast_settings.json", ".factor_run_history.json",
    "incident_draft_v2.json",
    "smoke-node.err.log", "smoke-node.out.log",
    ".env"
)

$xdArgs = @()
foreach ($d in $excludeDirs) { $xdArgs += @("/XD", $d) }
$xfArgs = @()
foreach ($f in $excludeFiles) { $xfArgs += @("/XF", $f) }

# Copy
& robocopy $root $staging /MIR /NFL /NDL /NP /NJH /NJS @xdArgs @xfArgs | Out-Null

# Sensible Inhalte herausnehmen, ausser explizit angefordert
if (-not $IncludeSecrets) {
    $secretFiles = @(
        "PDL fast\secrets\hellofresh-de-problem-solve-78fb952762cd.json",
        "PS Copilot\ps-basecamp-backend\.env"
    )
    foreach ($s in $secretFiles) {
        $p = Join-Path $staging $s
        if (Test-Path $p) {
            Remove-Item $p -Force
            Write-Host "[Pack] Sensibel entfernt: $s"
        }
    }
}

if (-not $IncludeData) {
    $purgeDirs = @(
        "incident-tool anhand BoxIDs\Imports",
        "incident-tool anhand BoxIDs\Exports",
        "incident-tool anhand BoxIDs\data",
        "PDL fast\Import Factor",
        "PDL fast\Export Factor",
        "Waagen Performance\exports",
        "Unified-Platform-Blueprint\results",
        "Unified-Platform-Blueprint\logs",
        "PS Copilot\ps-basecamp-backend\data"
    )
    foreach ($d in $purgeDirs) {
        $p = Join-Path $staging $d
        if (Test-Path $p) {
            Get-ChildItem $p -Recurse -File | Where-Object { $_.Name -ne ".gitkeep" } | Remove-Item -Force -ErrorAction SilentlyContinue
        }
    }
}

# ZIP erstellen
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Write-Host "[Pack] Erstelle ZIP: $zipPath"
Compress-Archive -Path (Join-Path $staging "*") -DestinationPath $zipPath -CompressionLevel Optimal

Remove-Item $staging -Recurse -Force
$sizeMb = [math]::Round((Get-Item $zipPath).Length / 1MB, 1)
Write-Host ""
Write-Host "[OK] Fertig: $zipPath  ($sizeMb MB)"
Write-Host "    -> ZIP weitergeben."
Write-Host "    -> Auf Zielrechner entpacken und 'START-ALLES.bat' doppelklicken."
