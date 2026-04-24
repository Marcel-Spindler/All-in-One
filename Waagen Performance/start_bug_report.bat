@echo off
setlocal
cd /d "%~dp0"

:: Verwende Python 3.12 (stabil)
set "PYTHON=py"
set "PYTHON_ARGS=-3.12"
set "PREFERRED_PORT=8505"

:: Prüfe ob Abhängigkeiten installiert sind
%PYTHON% %PYTHON_ARGS% -m pip show streamlit >nul 2>&1
if errorlevel 1 (
    echo Installiere Abhaengigkeiten...
    %PYTHON% %PYTHON_ARGS% -m pip install -r requirements.txt
    echo.
)

for /f "usebackq delims=" %%I in (`PowerShell -NoProfile -Command "$preferred = %PREFERRED_PORT%; $ports = @($preferred) + (($preferred + 1)..($preferred + 20)); foreach ($p in $ports) { try { $resp = Invoke-WebRequest -Uri ('http://127.0.0.1:' + $p) -UseBasicParsing -TimeoutSec 1; if ($resp.Content -match 'Streamlit') { 'RUNNING:' + $p; exit 0 } } catch {} $listen = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue; if (-not $listen) { 'FREE:' + $p; exit 0 } } exit 1"`) do set "PORT_RESULT=%%I"

for /f "tokens=1,2 delims=:" %%A in ("%PORT_RESULT%") do (
    set "PORT_MODE=%%A"
    set "PORT=%%B"
)

if "%PORT_MODE%"=="RUNNING" (
    echo Assembly QC Weekly Bug Report laeuft bereits auf Port %PORT%.
    start "" "http://127.0.0.1:%PORT%"
    goto :eof
)

if not defined PORT (
    echo Kein freier Port im Bereich %PREFERRED_PORT%-%PREFERRED_PORT%+20 gefunden.
    pause
    exit /b 1
)

echo Starte Assembly QC Weekly Bug Report auf Port %PORT%...
echo Browser oeffnet sich automatisch.
start "" "http://127.0.0.1:%PORT%"
%PYTHON% %PYTHON_ARGS% -m streamlit run weekly_bug_report.py --server.port %PORT%
pause
