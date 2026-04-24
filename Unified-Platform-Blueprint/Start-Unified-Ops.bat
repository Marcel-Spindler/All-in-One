@echo off
setlocal
cd /d "%~dp0"
powershell.exe -ExecutionPolicy Bypass -File "%~dp0Start-Unified-Ops.ps1"
if errorlevel 1 (
	echo.
	echo Mindestens ein Service konnte nicht sauber gestartet werden.
	echo Bitte die Meldung oben pruefen.
	pause
)
exit /b %errorlevel%
