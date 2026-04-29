Set shell = CreateObject("WScript.Shell")
launcherPath = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
shell.Run "powershell.exe -NoProfile -ExecutionPolicy Bypass -File """ & launcherPath & "\Start-Unified-Ops.ps1""", 0, False