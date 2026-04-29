@echo off
REM ============================================================
REM  PS Copilot - Self-Bootstrapping Setup & Start
REM  Voraussetzung auf Zielrechner: Node.js 18+ (https://nodejs.org)
REM ============================================================
setlocal EnableExtensions
set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
set "BACKEND=%ROOT%\ps-basecamp-backend"
cd /d "%BACKEND%"

REM --- Node.js pruefen ---
where node >nul 2>&1
if errorlevel 1 (
    echo.
    echo [FEHLER] Node.js wurde nicht gefunden.
    echo Bitte installieren: https://nodejs.org/  ^(LTS-Version^)
    echo.
    pause
    exit /b 1
)

REM --- .env pruefen ---
if not exist "%BACKEND%\.env" (
    if exist "%BACKEND%\.env.example" (
        echo.
        echo [Hinweis] Es wurde keine .env-Datei gefunden.
        echo Es wird .env.example als Vorlage kopiert.
        echo Bitte vor dem ersten Start GEMINI_API_KEY in der .env eintragen.
        echo.
        copy /Y "%BACKEND%\.env.example" "%BACKEND%\.env" >nul
        notepad "%BACKEND%\.env"
    ) else (
        echo.
        echo [FEHLER] Keine .env und keine .env.example vorhanden.
        echo Bitte vom Administrator einen GEMINI_API_KEY anfordern und in .env eintragen.
        pause
        exit /b 1
    )
)

REM --- node_modules installieren falls noetig ---
if not exist "%BACKEND%\node_modules" (
    echo [Setup] Installiere Node-Pakete ^(einmalig, dauert 1-3 Minuten^) ...
    call npm install
    if errorlevel 1 (
        echo [FEHLER] npm install fehlgeschlagen.
        pause
        exit /b 1
    )
)

REM --- Server starten / Browser oeffnen ---
call "%BACKEND%\start-ps-copilot.bat"
endlocal
exit /b 0
