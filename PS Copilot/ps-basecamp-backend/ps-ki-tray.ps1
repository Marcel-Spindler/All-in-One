# ps-ki-tray.ps1
# Kleines Tray-Tool zum Starten/Stoppen des PS-KI-Servers

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

# === Einstellungen ===
# BackendPath wird automatisch aus dem Skript-Verzeichnis abgeleitet,
# damit das Tool auf jedem Rechner ohne Anpassung laeuft.
$BackendPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServerPort  = 3020
$AppUrl      = "http://localhost:$ServerPort/"

# === NotifyIcon anlegen ===
$notifyIcon = New-Object System.Windows.Forms.NotifyIcon
$notifyIcon.Icon = [System.Drawing.SystemIcons]::Information
$notifyIcon.Visible = $true
$notifyIcon.Text = "PS Copilot"

# Kontextmenü
$contextMenu = New-Object System.Windows.Forms.ContextMenuStrip
$startItem = $contextMenu.Items.Add("PS Copilot starten")
$stopItem  = $contextMenu.Items.Add("PS Copilot stoppen")
$openItem  = $contextMenu.Items.Add("PS Copilot öffnen")
$exitItem  = $contextMenu.Items.Add("Beenden")

$notifyIcon.ContextMenuStrip = $contextMenu

# Merker für den gestarteten Prozess
$serverProcess = $null

function Show-Toast($title, $text) {
    $notifyIcon.BalloonTipTitle = $title
    $notifyIcon.BalloonTipText  = $text
    $notifyIcon.ShowBalloonTip(1000)
}

function Open-PSCopilot {
    Start-Process $AppUrl
}

function Start-PSKIServer {
    param()

    if ($serverProcess -and -not $serverProcess.HasExited) {
        Show-Toast "PS Copilot" "Läuft bereits unter $AppUrl"
        return
    }

    if (-not (Test-Path $BackendPath)) {
        Show-Toast "Fehler" "Backend-Pfad nicht gefunden: $BackendPath"
        return
    }

    # Prüfen, ob bereits ein Prozess auf Port läuft
    try {
        $connection = Get-NetTCPConnection -LocalPort $ServerPort -ErrorAction SilentlyContinue
        if ($connection) {
            Show-Toast "PS Copilot" "Port $ServerPort ist belegt. Zugriff: $AppUrl"
            return
        }
    } catch {}

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "cmd.exe"
    $psi.Arguments = "/c cd /d `"$BackendPath`" && npm start"
    $psi.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Minimized
    $psi.UseShellExecute = $true

    try {
        $serverProcess = [System.Diagnostics.Process]::Start($psi)
        Show-Toast "PS Copilot" "Server gestartet. Zugriff: $AppUrl"
    } catch {
        Show-Toast "Fehler" "Konnte Server nicht starten: $($_.Exception.Message)"
    }
}

function Stop-PSKIServer {
    param()

    $stopped = $false

    if ($serverProcess -and -not $serverProcess.HasExited) {
        try {
            $serverProcess.Kill()
            $serverProcess = $null
            $stopped = $true
        } catch {}
    }

    if (-not $stopped) {
        try {
            $connection = Get-NetTCPConnection -LocalPort $ServerPort -ErrorAction SilentlyContinue
            if ($connection) {
                Stop-Process -Id $connection.OwningProcess -Force -ErrorAction SilentlyContinue
                $stopped = $true
            }
        } catch {}
    }

    if ($stopped) {
        Show-Toast "PS Copilot" "Server auf Port $ServerPort gestoppt."
    } else {
        Show-Toast "PS Copilot" "Kein laufender Server auf Port $ServerPort gefunden."
    }
}

# === Event-Handler für Menüeinträge ===

$startItem.Add_Click({
    Start-PSKIServer
})

$stopItem.Add_Click({
    Stop-PSKIServer
})

$openItem.Add_Click({
    Open-PSCopilot
})

$notifyIcon.Add_DoubleClick({
    Open-PSCopilot
})

$exitItem.Add_Click({
    # Optional: Server beim Beenden auch stoppen
    if ($serverProcess -and -not $serverProcess.HasExited) {
        try { $serverProcess.Kill() } catch {}
    }
    $notifyIcon.Visible = $false
    [System.Windows.Forms.Application]::Exit()
})

# Beim Start automatisch checken, ob bereits was läuft
try {
    $connection = Get-NetTCPConnection -LocalPort $ServerPort -ErrorAction SilentlyContinue
    if ($connection) {
        Show-Toast "PS Copilot" "Bereit. Direkter Zugriff: $AppUrl"
    } else {
        Show-Toast "PS Copilot" "Bereit. Rechtsklick -> 'PS Copilot starten'."
    }
} catch {
    Show-Toast "PS Copilot" "Bereit. Direkter Zugriff: $AppUrl"
}

# Tray-App laufen lassen
[System.Windows.Forms.Application]::Run()
