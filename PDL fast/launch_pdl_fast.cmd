@echo off
setlocal
set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
cd /d "%ROOT%"
start "PDL Fast" python3.12.exe -m streamlit run app.py --server.port 8502
timeout /t 5 /nobreak >nul
start "" http://localhost:8502
endlocal
