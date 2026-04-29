param(
  [Parameter(Mandatory = $true)]
  [string]$KeyJsonPath,

  [Parameter(Mandatory = $true)]
  [string]$RootFolderId,

  [Parameter(Mandatory = $false)]
  [string]$ImpersonatedUser = "",

  [Parameter(Mandatory = $false)]
  [string]$EnvPath = ".env"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Convert-PrivateKeyToEnvValue {
  param([string]$PrivateKey)

  $normalized = $PrivateKey -replace "`r`n", "`n"
  $escaped = $normalized -replace "`n", "\\n"
  return '"' + $escaped + '"'
}

function Set-Or-AddEnvLine {
  param(
    [System.Collections.Generic.List[string]]$Lines,
    [string]$Key,
    [string]$Value
  )

  $prefix = "$Key="
  $index = -1

  for ($i = 0; $i -lt $Lines.Count; $i++) {
    if ($Lines[$i].StartsWith($prefix)) {
      $index = $i
      break
    }
  }

  $line = "$Key=$Value"
  if ($index -ge 0) {
    $Lines[$index] = $line
  } else {
    $Lines.Add($line)
  }
}

if (-not (Test-Path -LiteralPath $KeyJsonPath)) {
  throw "JSON-Key nicht gefunden: $KeyJsonPath"
}

$jsonRaw = Get-Content -LiteralPath $KeyJsonPath -Raw -Encoding UTF8
$keyObj = $jsonRaw | ConvertFrom-Json

if (-not $keyObj.client_email) {
  throw "client_email fehlt im JSON-Key."
}

if (-not $keyObj.private_key) {
  throw "private_key fehlt im JSON-Key."
}

$envFullPath = Resolve-Path -LiteralPath (Join-Path (Get-Location) $EnvPath) -ErrorAction SilentlyContinue
if (-not $envFullPath) {
  New-Item -ItemType File -Path $EnvPath -Force | Out-Null
  $envFullPath = Resolve-Path -LiteralPath (Join-Path (Get-Location) $EnvPath)
}

$envFile = $envFullPath.Path
$backup = "$envFile.bak.$((Get-Date).ToString('yyyyMMdd-HHmmss'))"
Copy-Item -LiteralPath $envFile -Destination $backup -Force

$existing = Get-Content -LiteralPath $envFile -Encoding UTF8
$lines = [System.Collections.Generic.List[string]]::new()
foreach ($line in $existing) { $lines.Add($line) }

$privateKeyValue = Convert-PrivateKeyToEnvValue -PrivateKey $keyObj.private_key

Set-Or-AddEnvLine -Lines $lines -Key "GOOGLE_SERVICE_ACCOUNT_EMAIL" -Value $keyObj.client_email
Set-Or-AddEnvLine -Lines $lines -Key "GOOGLE_SERVICE_ACCOUNT_KEY_FILE" -Value $KeyJsonPath
Set-Or-AddEnvLine -Lines $lines -Key "GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY" -Value $privateKeyValue
Set-Or-AddEnvLine -Lines $lines -Key "GOOGLE_DRIVE_ROOT_FOLDER_ID" -Value $RootFolderId
Set-Or-AddEnvLine -Lines $lines -Key "GOOGLE_IMPERSONATED_USER" -Value $ImpersonatedUser

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllLines($envFile, $lines, $utf8NoBom)

Write-Host "Fertig. .env wurde aktualisiert:" -ForegroundColor Green
Write-Host "  $envFile"
Write-Host "Backup:" -ForegroundColor Yellow
Write-Host "  $backup"
Write-Host ""
Write-Host "Gesetzte Werte:" -ForegroundColor Cyan
Write-Host "  GOOGLE_SERVICE_ACCOUNT_EMAIL=$($keyObj.client_email)"
Write-Host "  GOOGLE_SERVICE_ACCOUNT_KEY_FILE=$KeyJsonPath"
Write-Host "  GOOGLE_DRIVE_ROOT_FOLDER_ID=$RootFolderId"
if ([string]::IsNullOrWhiteSpace($ImpersonatedUser)) {
  Write-Host "  GOOGLE_IMPERSONATED_USER=(leer)"
} else {
  Write-Host "  GOOGLE_IMPERSONATED_USER=$ImpersonatedUser"
}
Write-Host ""
Write-Host "Nächster Schritt:" -ForegroundColor Magenta
Write-Host "1) Sicherstellen, dass der Drive-Hauptordner fuer die Service-Account-Mail freigegeben ist"
Write-Host "2) Server starten: npm start"
Write-Host "3) Test: http://localhost:3020/api/google/status"
