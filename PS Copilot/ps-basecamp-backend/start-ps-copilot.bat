@echo off
setlocal

set "APP_DIR=%~dp0"
if "%APP_DIR:~-1%"=="\" set "APP_DIR=%APP_DIR:~0,-1%"
set "APP_URL=http://localhost:3020/"

cd /d "%APP_DIR%"

REM Falls Server schon laeuft: nur Browser oeffnen
powershell -NoProfile -ExecutionPolicy Bypass -Command "if (Get-NetTCPConnection -LocalPort 3020 -ErrorAction SilentlyContinue) { Start-Process '%APP_URL%'; exit 0 }"

REM Server minimiert starten
start "PS Copilot Server" /min cmd /c "cd /d "%APP_DIR%" && npm start"

REM Kurz warten und dann Cockpit oeffnen
timeout /t 3 /nobreak >nul
start "" "%APP_URL%"

endlocal
exit
