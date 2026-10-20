param(
    [string]$ContainerName = "paycore-postgres",
    [string]$DbUser = "paycore",
    [string]$SourceDb = "paycore",
    [string]$BackupPath = (Join-Path (Get-Location) "backups/paycore-$(Get-Date -Format 'yyyyMMdd-HHmmss').sql"),
    [string]$ScratchDb = "paycore_restore_verify_$(Get-Date -Format 'yyyyMMddHHmmss')"
)

$ErrorActionPreference = "Stop"
$backupFullPath = [System.IO.Path]::GetFullPath($BackupPath)
$backupDirectory = [System.IO.Path]::GetDirectoryName($backupFullPath)
[System.IO.Directory]::CreateDirectory($backupDirectory) | Out-Null
if (Test-Path -LiteralPath $backupFullPath) { throw "Refusing to overwrite existing backup: $backupFullPath" }
if ($ScratchDb -notmatch '^[a-zA-Z][a-zA-Z0-9_]{0,62}$') { throw "Invalid PostgreSQL scratch database name: $ScratchDb" }

Write-Host "==> Creating a plain SQL backup of '$SourceDb' in '$backupFullPath'..." -ForegroundColor Cyan
$dumpText = docker exec $ContainerName pg_dump -U $DbUser -d $SourceDb --no-owner --no-privileges
if ($LASTEXITCODE -ne 0) { throw "pg_dump failed with exit code $LASTEXITCODE" }
[System.IO.File]::WriteAllText($backupFullPath, ($dumpText -join [Environment]::NewLine), [System.Text.UTF8Encoding]::new($false))
if ((Get-Item -LiteralPath $backupFullPath).Length -eq 0) { throw "pg_dump produced an empty backup" }

Write-Host "==> Creating isolated restore database '$ScratchDb'..." -ForegroundColor Cyan
$exists = (docker exec $ContainerName psql -U $DbUser -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$ScratchDb'" | Out-String).Trim()
if ($LASTEXITCODE -ne 0) { throw "Could not query PostgreSQL database catalog" }
if ($exists -eq "1") { throw "Refusing to drop or overwrite existing database '$ScratchDb'" }
docker exec $ContainerName psql -U $DbUser -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $ScratchDb"
if ($LASTEXITCODE -ne 0) { throw "Could not create scratch database '$ScratchDb'" }

Write-Host "==> Restoring backup into '$ScratchDb'..." -ForegroundColor Cyan
Get-Content -LiteralPath $backupFullPath -Raw | docker exec -i $ContainerName psql -U $DbUser -d $ScratchDb -v ON_ERROR_STOP=1
if ($LASTEXITCODE -ne 0) { throw "Restore failed; scratch database '$ScratchDb' was retained for diagnosis" }

Write-Host "==> Comparing ledger transaction counts and per-transaction balance..." -ForegroundColor Cyan
$sourceCount = (docker exec $ContainerName psql -U $DbUser -d $SourceDb -tAc "SELECT COUNT(*) FROM ledger_transactions").Trim()
if ($LASTEXITCODE -ne 0) { throw "Could not inspect source ledger" }
$restoredCount = (docker exec $ContainerName psql -U $DbUser -d $ScratchDb -tAc "SELECT COUNT(*) FROM ledger_transactions").Trim()
if ($LASTEXITCODE -ne 0) { throw "Could not inspect restored ledger" }
if ($sourceCount -ne $restoredCount) { throw "Ledger transaction count changed during restore: source=$sourceCount restored=$restoredCount" }
$unbalanced = (docker exec $ContainerName psql -U $DbUser -d $ScratchDb -tAc "SELECT COUNT(*) FROM (SELECT lt.id FROM ledger_transactions lt JOIN ledger_entries le ON le.txn_id = lt.id JOIN accounts a ON a.id = le.account_id GROUP BY lt.id, a.currency HAVING SUM(CASE WHEN le.direction = 'DEBIT' THEN le.amount ELSE -le.amount END) <> 0) t").Trim()
if ($LASTEXITCODE -ne 0) { throw "Could not verify restored ledger invariants" }
if ($unbalanced -ne "0") { throw "Restored ledger contains $unbalanced unbalanced transaction/currency groups" }

Write-Host "PASS: backup restored; $restoredCount ledger transactions preserved and every transaction balances by currency." -ForegroundColor Green
Write-Host "Backup retained at: $backupFullPath"
Write-Host "Scratch restore retained at: $ScratchDb"
