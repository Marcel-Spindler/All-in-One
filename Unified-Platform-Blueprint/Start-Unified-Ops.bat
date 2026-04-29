@echo off
setlocal
cd /d "%~dp0"
wscript.exe "%~dp0Start-Unified-Ops.vbs"
exit /b 0
