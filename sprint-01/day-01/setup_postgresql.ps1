# ============================================================
# PostgreSQL 15 Setup + Hardening Script (Windows PowerShell)
# ============================================================

$ErrorActionPreference = "Stop"

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
$DB_NAME   = "dvdrental"
$APP_USER  = "appuser"
$APP_PASS  = "AppUser@2024!"
$LOG_FILE  = "setup_postgresql.log"

# Try to auto-detect PostgreSQL install path
$PG_BASE = "D:\PostgreSQL\17"
$PG_BIN  = "$PG_BASE\bin"
$PG_DATA = "$PG_BASE\data"

# ------------------------------------------------------------
# LOGGING FUNCTION
# ------------------------------------------------------------
function Log {
    param ([string]$msg)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$timestamp] $msg"
    Write-Host $line
    Add-Content -Path $LOG_FILE -Value $line
}

# ------------------------------------------------------------
# CHECK POSTGRESQL
# ------------------------------------------------------------
Log "Checking PostgreSQL installation..."

if (!(Test-Path "$PG_BIN\psql.exe")) {
    Log "ERROR: PostgreSQL not found at $PG_BIN"
    exit 1
}

$env:Path += ";$PG_BIN"

Log "PostgreSQL found"

# ------------------------------------------------------------
# CREATE ROLE + DATABASE
# ------------------------------------------------------------
Log "Creating role and database..."

$sqlSetup = @"
DO \$\$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = '$APP_USER') THEN
        ALTER ROLE $APP_USER WITH PASSWORD '$APP_PASS';
    ELSE
        CREATE USER $APP_USER WITH LOGIN PASSWORD '$APP_PASS';
    END IF;
END
\$\$;

ALTER ROLE $APP_USER SET statement_timeout = '60000';

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS $DB_NAME;
CREATE DATABASE $DB_NAME;
GRANT CONNECT ON DATABASE $DB_NAME TO $APP_USER;
"@

$sqlSetupFile = "$env:TEMP\pg_setup.sql"
$sqlSetup | Out-File -Encoding ASCII $sqlSetupFile

psql -U postgres -f $sqlSetupFile | Tee-Object -FilePath $LOG_FILE -Append

# ------------------------------------------------------------
# SCHEMA PERMISSIONS
# ------------------------------------------------------------
Log "Applying schema permissions..."

$sqlPerms = @"
\c $DB_NAME

GRANT USAGE ON SCHEMA public TO $APP_USER;

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA public TO $APP_USER;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA public TO $APP_USER;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $APP_USER;
"@

$sqlPermsFile = "$env:TEMP\pg_perms.sql"
$sqlPerms | Out-File -Encoding ASCII $sqlPermsFile

psql -U postgres -f $sqlPermsFile | Tee-Object -FilePath $LOG_FILE -Append

# ------------------------------------------------------------
# HARDEN postgresql.conf
# ------------------------------------------------------------
$pgConf = "$PG_DATA\postgresql.conf"

if (Test-Path $pgConf) {
    Log "Hardening postgresql.conf..."

    Copy-Item $pgConf "$pgConf.bak_$(Get-Date -Format yyyyMMdd)"

    function Set-Config($key, $value) {
        $content = Get-Content $pgConf
        if ($content -match "^$key") {
            $content = $content -replace "^#?$key\s*=.*", "$key = $value"
        } else {
            $content += "$key = $value"
        }
        $content | Set-Content $pgConf
    }

    Set-Config "max_connections" "100"
    Set-Config "shared_buffers" "256MB"
    Set-Config "log_connections" "on"
    Set-Config "log_disconnections" "on"
    Set-Config "statement_timeout" "60000"
    Set-Config "lock_timeout" "10000"

    Log "postgresql.conf updated"
} else {
    Log "WARNING: postgresql.conf not found"
}

# ------------------------------------------------------------
# CONFIGURE pg_hba.conf
# ------------------------------------------------------------
$hbaConf = "$PG_DATA\pg_hba.conf"

if (Test-Path $hbaConf) {
    Log "Updating pg_hba.conf..."

    Copy-Item $hbaConf "$hbaConf.bak_$(Get-Date -Format yyyyMMdd)"

    $entry = "host    $DB_NAME    $APP_USER    127.0.0.1/32    scram-sha-256"

    if (-not (Select-String -Path $hbaConf -Pattern $APP_USER)) {
        Add-Content $hbaConf "`n# appuser access"
        Add-Content $hbaConf $entry
        Log "pg_hba entry added"
    }
}

# ------------------------------------------------------------
# RESTART SERVICE
# ------------------------------------------------------------
Log "Restarting PostgreSQL service..."

Get-Service *postgres* | Restart-Service

Start-Sleep -Seconds 3

# ------------------------------------------------------------
# DOWNLOAD SAMPLE DB
# ------------------------------------------------------------
$dumpDir = "$env:TEMP\dvdrental_setup"
New-Item -ItemType Directory -Force -Path $dumpDir | Out-Null
Set-Location $dumpDir

if (!(Test-Path "dvdrental.tar")) {
    Log "Downloading sample database..."

    Invoke-WebRequest `
        -Uri "https://neon.com/postgresqltutorial/dvdrental.zip" `
        -OutFile "dvdrental.zip"

    Expand-Archive dvdrental.zip -Force
}

# ------------------------------------------------------------
# RESTORE DATABASE
# ------------------------------------------------------------
Log "Restoring database..."

pg_restore -U postgres -d $DB_NAME --no-owner --no-privileges dvdrental.tar `
| Tee-Object -FilePath $LOG_FILE -Append

# ------------------------------------------------------------
# VERIFY
# ------------------------------------------------------------
Log "Verifying appuser access..."

$env:PGPASSWORD = $APP_PASS

psql -h 127.0.0.1 -U $APP_USER -d $DB_NAME `
-c "SELECT COUNT(*) FROM film;" `
| Tee-Object -FilePath $LOG_FILE -Append

# ------------------------------------------------------------
# CREATE .env FILE
# ------------------------------------------------------------
$envDir = "$HOME\python-de-journey"
$envFile = "$envDir\.env"

if (!(Test-Path $envDir)) {
    New-Item -ItemType Directory -Path $envDir | Out-Null
}

if (!(Test-Path $envFile)) {
@"
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=$DB_NAME
DB_USER=$APP_USER
DB_PASSWORD=$APP_PASS
"@ | Out-File $envFile

    Log ".env file created"
}

# ------------------------------------------------------------
# DONE
# ------------------------------------------------------------
Log "======================================================"
Log "PostgreSQL Setup Complete"
Log "Database: $DB_NAME"
Log "User: $APP_USER"
Log "======================================================"