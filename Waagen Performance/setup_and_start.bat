@echo off
REM ============================================================
REM  Waagen Performance - Self-Bootstrapping Launcher
REM  Voraussetzung: Python 3.10+ installiert (Add Python to PATH).
REM ============================================================
setlocal EnableExtensions
set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
cd /d "%ROOT%"

set "VENV=%ROOT%\.venv"
set "PYEXE=%VENV%\Scripts\python.exe"
set "REQ=%ROOT%\requirements.txt"
set "URL=http://localhost:8505/"

set "SYSPY="
where py >nul 2>&1 && set "SYSPY=py -3"
if not defined SYSPY (
    where python >nul 2>&1 && set "SYSPY=python"
)
if not defined SYSPY (
    echo [FEHLER] Python wurde nicht gefunden. Bitte Python 3.10+ installieren.
    pause & exit /b 1
)

if not exist "%PYEXE%" (
    echo [Setup] Erstelle .venv ...
    %SYSPY% -m venv "%VENV%" || ( echo [FEHLER] venv-Erstellung fehlgeschlagen & pause & exit /b 1 )
    "%PYEXE%" -m pip install --upgrade pip --disable-pip-version-check >nul
    echo [Setup] Installiere Pakete ^(einmalig, 1-2 Minuten^) ...
    "%PYEXE%" -m pip install --disable-pip-version-check -r "%REQ%" || ( echo [FEHLER] pip install fehlgeschlagen & pause & exit /b 1 )
)

powershell -NoProfile -Command "$r=$null; try { $r = Invoke-WebRequest -Uri '%URL%' -UseBasicParsing -TimeoutSec 2 } catch {}; if ($r) { exit 0 } else { exit 1 }"
if errorlevel 1 (
    echo [Start] Starte Waagen Performance ...
    start "Waagen Performance" "%PYEXE%" -m streamlit run "%ROOT%\weekly_bug_report.py" --server.port 8505 --server.headless true --browser.gatherUsageStats false
    powershell -NoProfile -Command "Start-Sleep -Seconds 3" >nul
)

start "" "%URL%"
endlocal
exit /b 0
