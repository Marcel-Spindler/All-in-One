@echo off
REM Komfort-Starter: leitet auf den Bootstrapping-Launcher um,
REM damit Node-Module bei Bedarf installiert werden.
cd /d "%~dp0"
call "setup_and_start.bat"
