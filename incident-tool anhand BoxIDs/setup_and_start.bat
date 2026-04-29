@echo off
REM ============================================================
REM  Factor Incident Tool v2 - Self-Bootstrapping Launcher
REM  Funktioniert auf jedem Windows-Rechner mit Python 3.10+.
REM  Beim ersten Start wird automatisch eine lokale .venv
REM  angelegt und alle benoetigten Pakete installiert.
REM ============================================================
setlocal EnableExtensions
set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
cd /d "%ROOT%"

set "VENV=%ROOT%\.venv"
set "PYEXE=%VENV%\Scripts\python.exe"
set "APP=%ROOT%\modern_incident_tool\app.py"
set "REQ=%ROOT%\requirements.txt"
set "URL=http://localhost:8501/"

REM --- 1) Python auf dem System suchen ---------------------------------
set "SYSPY="
where py >nul 2>&1 && set "SYSPY=py -3"
if not defined SYSPY (
    where python >nul 2>&1 && set "SYSPY=python"
)
if not defined SYSPY (
    echo.
    echo [FEHLER] Python wurde auf diesem Rechner nicht gefunden.
    echo.
    echo Bitte installieren:
    echo   https://www.python.org/downloads/windows/
    echo   ^(Version 3.10 oder neuer, beim Setup "Add Python to PATH" aktivieren^)
    echo.
    pause
    exit /b 1
)

REM --- 2) .venv anlegen, falls nicht vorhanden -------------------------
if not exist "%PYEXE%" (
    echo.
    echo [Setup] Erstelle lokale Python-Umgebung in .venv ...
    %SYSPY% -m venv "%VENV%"
    if errorlevel 1 (
        echo [FEHLER] Konnte .venv nicht erstellen.
        pause
        exit /b 1
    )
    echo [Setup] Aktualisiere pip ...
    "%PYEXE%" -m pip install --upgrade pip --disable-pip-version-check >nul
    echo [Setup] Installiere benoetigte Pakete ^(dauert beim ersten Mal 1-2 Minuten^) ...
    "%PYEXE%" -m pip install --disable-pip-version-check -r "%REQ%"
    if errorlevel 1 (
        echo [FEHLER] Pip-Installation fehlgeschlagen.
        pause
        exit /b 1
    )
    echo [Setup] Fertig.
    echo.
)

REM --- 3) Pruefen ob bereits eine Instanz laeuft -----------------------
powershell -NoProfile -Command "$r=$null; try { $r = Invoke-WebRequest -Uri '%URL%' -UseBasicParsing -TimeoutSec 2 } catch {}; if ($r -and $r.Content -match 'Factor Incident Tool v2') { exit 0 } elseif ($r) { exit 2 } else { exit 1 }"
if errorlevel 2 (
    echo [FEHLER] Auf Port 8501 laeuft bereits eine andere Anwendung.
    echo Bitte zuerst die andere App schliessen und erneut starten.
    pause
    exit /b 1
)
if errorlevel 1 (
    echo [Start] Starte Factor Incident Tool v2 ...
    start "Factor Incident Tool v2" "%PYEXE%" -m streamlit run "%APP%" --server.port 8501 --server.headless true --browser.gatherUsageStats false
    REM kurze Wartezeit, damit der Server hochfaehrt
    powershell -NoProfile -Command "Start-Sleep -Seconds 3" >nul
)

start "" "%URL%"
endlocal
exit /b 0
