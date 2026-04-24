@echo off
setlocal
set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
cd /d "%ROOT%"

set "PYTHON=%ROOT%\.venv\Scripts\python.exe"
set "APP=%ROOT%\modern_incident_tool\app.py"
set "URL=http://localhost:8501/"

if not exist "%PYTHON%" (
	echo Fehler: %PYTHON% wurde nicht gefunden.
	pause
	exit /b 1
)

powershell -NoProfile -Command "$response = $null; try { $response = Invoke-WebRequest -Uri '%URL%' -UseBasicParsing -TimeoutSec 2 } catch {}; if ($response -and $response.Content -match 'Factor Incident Tool v2') { exit 0 } elseif ($response) { exit 2 } else { exit 1 }"
if errorlevel 2 (
	echo Fehler: Auf Port 8501 laeuft bereits eine andere App statt des Incident Tools.
	echo Bitte die alte PDL-Fast-Instanz schliessen und dieses Script erneut starten.
	pause
	exit /b 1
)
if errorlevel 1 (
	start "Factor Incident Tool v2" "%PYTHON%" -m streamlit run "%APP%" --server.port 8501 --server.headless true --browser.gatherUsageStats false
	timeout /t 2 >nul
)

start "" "%URL%"
endlocal
