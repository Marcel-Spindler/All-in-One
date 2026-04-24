Set shell = CreateObject("WScript.Shell")
repoPath = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
pythonCmd = "cmd /c cd /d """ & repoPath & """ && python3.12.exe -m streamlit run app.py --server.port 8502"
shell.Run pythonCmd, 0, False
WScript.Sleep 5000
shell.Run "http://localhost:8502", 1, False
