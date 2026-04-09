#!/usr/bin/env bash
# =============================================================================
# setup_postgresql.sh — Day 01 Sprint 01
# PostgreSQL 15 install + appuser + dvdrental DB + hardening
# Usage: bash setup_postgresql.sh
# =============================================================================

set -euo pipefail
LOG_FILE="setup_postgresql.log"

log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
fail() { log "ERROR: $*"; exit 1; }
hr()   { log "------------------------------------------------------"; }

log "======================================================"
log "  PostgreSQL 15 Setup + Hardening Script"
log "======================================================"

# -------------------------------------------------------
# 0. Configuration — CHANGE THESE BEFORE RUNNING
# -------------------------------------------------------
DB_NAME="dvdrental"
APP_USER="appuser"
APP_PASS="AppUser@2024!"   # ⚠️  Change this in production

log "Target DB:   $DB_NAME"
log "App User:    $APP_USER"

# -------------------------------------------------------
# 1. Install PostgreSQL 15
# -------------------------------------------------------
OS="$(uname -s)"

install_pg_linux() {
    log "Installing PostgreSQL 15 on Linux..."
    sudo apt-get update -qq
    sudo apt-get install -y \
        postgresql-15 \
        postgresql-contrib-15 \
        postgresql-client-15 \
        2>&1 | tee -a "$LOG_FILE"
    sudo systemctl start postgresql
    sudo systemctl enable postgresql
    log "PostgreSQL 15 installed and started"
}

install_pg_mac() {
    log "Installing PostgreSQL 15 on macOS..."
    brew install postgresql@15
    brew services start postgresql@15
    echo 'export PATH="/usr/local/opt/postgresql@15/bin:$PATH"' >> ~/.zshrc
    export PATH="/usr/local/opt/postgresql@15/bin:$PATH"
    log "PostgreSQL 15 installed on macOS"
}

case "$OS" in
    Linux*)  install_pg_linux ;;
    Darwin*) install_pg_mac   ;;
    *)       log "WARNING: Manual PostgreSQL install required on $OS" ;;
esac

# Verify
PG_VERSION=$(psql --version 2>&1 || true)
log "psql version: $PG_VERSION"

# -------------------------------------------------------
# 2. Create appuser + dvdrental DB via SQL
# -------------------------------------------------------
hr
log "Creating appuser and dvdrental database..."

# Write the SQL setup to a temp file
SQL_SETUP=$(mktemp /tmp/pg_setup_XXXXXX.sql)
cat > "$SQL_SETUP" << SQLEOF
-- ==========================================================
-- PostgreSQL Setup: appuser + dvdrental database
-- Run as postgres superuser
-- ==========================================================

-- 1. Drop and recreate appuser (idempotent)
DO \$\$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = '${APP_USER}') THEN
        RAISE NOTICE 'Role ${APP_USER} already exists, updating password...';
        ALTER ROLE ${APP_USER} WITH PASSWORD '${APP_PASS}';
    ELSE
        CREATE USER ${APP_USER} WITH
            LOGIN
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            NOINHERIT
            CONNECTION LIMIT 20
            PASSWORD '${APP_PASS}';
        RAISE NOTICE 'Role ${APP_USER} created.';
    END IF;
END
\$\$;

-- 2. Harden appuser timeouts
ALTER ROLE ${APP_USER} SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE ${APP_USER} SET statement_timeout = '60000';
ALTER ROLE ${APP_USER} SET lock_timeout = '10s';
ALTER ROLE ${APP_USER} SET search_path = public;

-- 3. Drop and recreate dvdrental DB (WARNING: drops if exists)
-- Comment out DROP if you want to keep existing data
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${DB_NAME}' AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS ${DB_NAME};

CREATE DATABASE ${DB_NAME}
    WITH OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TEMPLATE = template0
    CONNECTION LIMIT = 50;

COMMENT ON DATABASE ${DB_NAME} IS
    'DVD Rental sample database — Python DE Journey 90-day program';

-- 4. Grant appuser CONNECT to dvdrental
GRANT CONNECT ON DATABASE ${DB_NAME} TO ${APP_USER};

\echo 'Database ${DB_NAME} created and CONNECT granted to ${APP_USER}'
SQLEOF

# Execute setup SQL as postgres
if [ "$OS" = "Linux" ]; then
    sudo -u postgres psql -f "$SQL_SETUP" -v ON_ERROR_STOP=1 \
        2>&1 | tee -a "$LOG_FILE"
else
    psql -U postgres -f "$SQL_SETUP" -v ON_ERROR_STOP=1 \
        2>&1 | tee -a "$LOG_FILE"
fi

rm -f "$SQL_SETUP"
log "appuser and dvdrental database created"

# -------------------------------------------------------
# 3. Schema-level permissions (must run AFTER connecting to dvdrental)
# -------------------------------------------------------
hr
log "Setting schema-level permissions for appuser..."

SQL_PERMS=$(mktemp /tmp/pg_perms_XXXXXX.sql)
cat > "$SQL_PERMS" << SQLEOF
-- Connect to dvdrental and grant schema permissions
\c ${DB_NAME}

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO ${APP_USER};

-- Current objects
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA public TO ${APP_USER};
GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA public TO ${APP_USER};
GRANT EXECUTE
    ON ALL FUNCTIONS IN SCHEMA public TO ${APP_USER};

-- Future objects (ALTER DEFAULT PRIVILEGES for postgres role)
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${APP_USER};
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO ${APP_USER};
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT EXECUTE ON FUNCTIONS TO ${APP_USER};

-- Explicitly REVOKE dangerous privileges
REVOKE CREATE ON SCHEMA public FROM ${APP_USER};
REVOKE ALL ON DATABASE ${DB_NAME} FROM PUBLIC;
GRANT CONNECT ON DATABASE ${DB_NAME} TO ${APP_USER};

\echo 'Schema permissions set for ${APP_USER} on ${DB_NAME}'
SQLEOF

if [ "$OS" = "Linux" ]; then
    sudo -u postgres psql -f "$SQL_PERMS" -v ON_ERROR_STOP=1 \
        2>&1 | tee -a "$LOG_FILE"
else
    psql -U postgres -f "$SQL_PERMS" -v ON_ERROR_STOP=1 \
        2>&1 | tee -a "$LOG_FILE"
fi

rm -f "$SQL_PERMS"
log "Schema permissions granted"

# -------------------------------------------------------
# 4. Harden postgresql.conf
# -------------------------------------------------------
hr
log "Hardening postgresql.conf..."

if [ "$OS" = "Linux" ]; then
    PG_CONF="/etc/postgresql/15/main/postgresql.conf"
    if [ -f "$PG_CONF" ]; then
        sudo cp "$PG_CONF" "${PG_CONF}.bak.$(date +%Y%m%d)"

        # Apply settings using sed (idempotent-ish)
        set_pg_conf() {
            local key="$1" val="$2"
            sudo sed -i "s/^#*${key}\s*=.*/${key} = ${val}/" "$PG_CONF"
            # If key not found, append it
            grep -q "^${key}" "$PG_CONF" || echo "${key} = ${val}" | sudo tee -a "$PG_CONF"
        }

        set_pg_conf "max_connections"                    "100"
        set_pg_conf "shared_buffers"                     "'256MB'"
        set_pg_conf "effective_cache_size"               "'512MB'"
        set_pg_conf "idle_in_transaction_session_timeout" "30000"
        set_pg_conf "statement_timeout"                  "60000"
        set_pg_conf "lock_timeout"                       "10000"
        set_pg_conf "log_min_duration_statement"         "1000"
        set_pg_conf "log_connections"                    "on"
        set_pg_conf "log_disconnections"                 "on"
        set_pg_conf "log_line_prefix"                    "'%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '"
        set_pg_conf "log_checkpoints"                    "on"
        set_pg_conf "log_lock_waits"                     "on"
        set_pg_conf "deadlock_timeout"                   "'1s'"

        log "postgresql.conf hardened (backup at ${PG_CONF}.bak.*)"
    else
        log "WARNING: postgresql.conf not found at $PG_CONF"
    fi
fi

# -------------------------------------------------------
# 5. Configure pg_hba.conf
# -------------------------------------------------------
hr
log "Configuring pg_hba.conf..."

if [ "$OS" = "Linux" ]; then
    HBA_CONF="/etc/postgresql/15/main/pg_hba.conf"
    if [ -f "$HBA_CONF" ]; then
        sudo cp "$HBA_CONF" "${HBA_CONF}.bak.$(date +%Y%m%d)"

        # Ensure appuser can connect from localhost with password
        if ! sudo grep -q "host.*${DB_NAME}.*${APP_USER}" "$HBA_CONF"; then
            echo "# Python DE Journey — appuser entry" | sudo tee -a "$HBA_CONF"
            echo "host    ${DB_NAME}    ${APP_USER}    127.0.0.1/32    scram-sha-256" \
                | sudo tee -a "$HBA_CONF"
            log "pg_hba.conf entry added for appuser"
        else
            log "pg_hba.conf entry for appuser already exists"
        fi
    fi
fi

# -------------------------------------------------------
# 6. Reload PostgreSQL to apply config changes
# -------------------------------------------------------
hr
log "Reloading PostgreSQL..."

if [ "$OS" = "Linux" ]; then
    sudo systemctl reload postgresql || sudo systemctl restart postgresql
else
    brew services restart postgresql@15
fi

sleep 2
log "PostgreSQL reloaded"

# -------------------------------------------------------
# 7. Download and restore DVD Rental sample database
# -------------------------------------------------------
hr
log "Downloading DVD Rental (Pagila) sample database..."

DUMP_URL="https://www.postgresqltutorial.com/wp-content/uploads/2019/05/dvdrental.zip"
DUMP_DIR="/tmp/dvdrental_setup"
mkdir -p "$DUMP_DIR"
cd "$DUMP_DIR"

if [ ! -f "dvdrental.tar" ]; then
    log "Downloading from postgresqltutorial.com..."
    curl -L "$DUMP_URL" -o dvdrental.zip 2>&1 | tee -a "$LOG_FILE"
    unzip -o dvdrental.zip
    log "DVD Rental dump downloaded and extracted"
else
    log "dvdrental.tar already present, skipping download"
fi

# Restore
log "Restoring dvdrental database..."
if [ "$OS" = "Linux" ]; then
    sudo -u postgres pg_restore \
        -d dvdrental \
        --no-owner \
        --no-privileges \
        -v \
        dvdrental.tar \
        2>&1 | tee -a "$LOG_FILE"
else
    pg_restore \
        -U postgres \
        -d dvdrental \
        --no-owner \
        --no-privileges \
        -v \
        dvdrental.tar \
        2>&1 | tee -a "$LOG_FILE"
fi

log "DVD Rental database restored"
cd - > /dev/null

# -------------------------------------------------------
# 8. Verify appuser can query the database
# -------------------------------------------------------
hr
log "Verifying appuser access..."

PGPASSWORD="$APP_PASS" psql \
    -h 127.0.0.1 \
    -U "$APP_USER" \
    -d "$DB_NAME" \
    -c "SELECT COUNT(*) AS film_count FROM film;" \
    2>&1 | tee -a "$LOG_FILE" || \
    log "WARNING: appuser verification query failed — check pg_hba.conf"

# -------------------------------------------------------
# 9. Print table inventory
# -------------------------------------------------------
hr
log "Table inventory in $DB_NAME:"

if [ "$OS" = "Linux" ]; then
    sudo -u postgres psql -d "$DB_NAME" \
        -c "SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(quote_ident(tablename)::text)) AS size FROM pg_tables WHERE schemaname='public' ORDER BY tablename;" \
        2>&1 | tee -a "$LOG_FILE"
fi

# -------------------------------------------------------
# 10. Create .env file template
# -------------------------------------------------------
hr
ENV_FILE="${HOME}/python-de-journey/.env"
if [ ! -f "$ENV_FILE" ]; then
    log "Creating .env file..."
    cat > "$ENV_FILE" << ENVEOF
# ============================================================
# Database Configuration — python-de-journey
# ⚠️  NEVER commit this file to git
# ============================================================
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=${DB_NAME}
DB_USER=${APP_USER}
DB_PASSWORD=${APP_PASS}

# Connection Pool Settings
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=2
DB_POOL_RECYCLE=1800
DB_POOL_PRE_PING=true

# App Settings
APP_ENV=development
LOG_LEVEL=INFO
ENVEOF
    log ".env file created at $ENV_FILE"
else
    log ".env already exists — skipping"
fi

# -------------------------------------------------------
# 11. Summary
# -------------------------------------------------------
log "======================================================"
log "  PostgreSQL Setup Complete"
log "======================================================"
log "Database:   $DB_NAME"
log "App User:   $APP_USER"
log "Host:       127.0.0.1:5432"
log "Auth:       scram-sha-256 (pg_hba.conf)"
log "Log:        $LOG_FILE"
log ""
log "Connection string:"
log "  postgresql://${APP_USER}:***@127.0.0.1:5432/${DB_NAME}"
log ""
log "Test with:"
log "  PGPASSWORD='${APP_PASS}' psql -h 127.0.0.1 -U ${APP_USER} -d ${DB_NAME}"
log "======================================================"
