@echo off
REM ============================================================
REM  All in One - Master Launcher
REM  Startet alle 4 Tools und das Cockpit auf dem aktuellen
REM  Rechner. Alle Setups laufen self-bootstrapping ab, wenn
REM  Python 3.10+ und Node.js 18+ installiert sind.
REM ============================================================
setlocal
cd /d "%~dp0"

echo.
echo  ===== All in One - Start =====
echo.

REM 1) PS Copilot (Hub) - muss zuerst hoch, weil andere Tools ihn pingen
if exist "PS Copilot\setup_and_start.bat" (
    echo [1/4] PS Copilot ...
    start "" /min cmd /c ""PS Copilot\setup_and_start.bat""
    timeout /t 4 /nobreak >nul
)

REM 2) Incident Tool
if exist "incident-tool anhand BoxIDs\setup_and_start.bat" (
    echo [2/4] Incident Tool ...
    start "" /min cmd /c ""incident-tool anhand BoxIDs\setup_and_start.bat""
    timeout /t 2 /nobreak >nul
)

REM 3) PDL Fast
if exist "PDL fast\setup_and_start.bat" (
    echo [3/4] PDL Fast ...
    start "" /min cmd /c ""PDL fast\setup_and_start.bat""
    timeout /t 2 /nobreak >nul
)

REM 4) Waagen Performance
if exist "Waagen Performance\setup_and_start.bat" (
    echo [4/4] Waagen Performance ...
    start "" /min cmd /c ""Waagen Performance\setup_and_start.bat""
    timeout /t 2 /nobreak >nul
)

REM Optional: Cockpit (Dashboard) anzeigen
if exist "Unified-Platform-Blueprint\dashboard.html" (
    start "" "Unified-Platform-Blueprint\dashboard.html"
)

echo.
echo  Fertig. Browser-Fenster oeffnen sich nach wenigen Sekunden.
echo.
endlocal
exit /b 0
