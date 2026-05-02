#!/bin/bash
# PostgreSQL Restore Script for Zhamlik
# Usage: ./restore.sh <backup_file>

set -e

# Check if backup file is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <backup_file>"
    echo ""
    echo "Available backups:"
    ls -lh /opt/backups/postgresql/daily/*.sql.gz 2>/dev/null | tail -5
    ls -lh /opt/backups/postgresql/weekly/*.sql.gz 2>/dev/null | tail -5
    ls -lh /opt/backups/postgresql/monthly/*.sql.gz 2>/dev/null | tail -5
    exit 1
fi

BACKUP_FILE="$1"

# Check if backup file exists
if [ ! -f "$BACKUP_FILE" ]; then
    echo "ERROR: Backup file not found: $BACKUP_FILE"
    exit 1
fi

# Database configuration
DB_NAME="zhamlik_db"
DB_USER="zhamlik"
DB_HOST="localhost"
DB_PASS="zhamlik_secure_password_2024"
LOG_FILE="/var/log/zhamlik/restore.log"

# Create log directory
mkdir -p /var/log/zhamlik

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=== Restore started ==="
log "Backup file: $BACKUP_FILE"

# Confirm restore
read -p "This will REPLACE the current database. Are you sure? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    log "Restore cancelled by user"
    exit 0
fi

# Stop services to prevent concurrent access
log "Stopping zhamlik services..."
systemctl stop zhamlik zhamlik-crypto zhamlik-mobile

# Drop existing database (optional - comment out if you want to keep existing data)
log "Dropping existing database..."
sudo -u postgres psql -c "DROP DATABASE IF EXISTS ${DB_NAME}_restore;"

# Create new database
log "Creating new database..."
sudo -u postgres psql -c "CREATE DATABASE ${DB_NAME}_restore OWNER $DB_USER;"

# Set PGPASSWORD for pg_restore
export PGPASSWORD="$DB_PASS"

# Restore backup
log "Restoring database from backup..."
gunzip -c "$BACKUP_FILE" | psql -h "$DB_HOST" -U "$DB_USER" "${DB_NAME}_restore"

unset PGPASSWORD

# Verify restore
log "Verifying restore..."
RESTORED_SIZE=$(sudo -u postgres psql -d "${DB_NAME}_restore" -t -c "SELECT pg_size_pretty(pg_database_size('${DB_NAME}_restore'));" | xargs)
log "Restored database size: $RESTORED_SIZE"

# Instructions for switching to restored database
log "=== Restore completed ==="
log ""
log "To switch to the restored database:"
log "  1. Stop services: systemctl stop zhamlik zhamlik-crypto zhamlik-mobile"
log "  2. Rename databases:"
log "     sudo -u postgres psql -c 'ALTER DATABASE $DB_NAME RENAME TO ${DB_NAME}_backup_$(date +%Y%m%d);'"
log "     sudo -u postgres psql -c 'ALTER DATABASE ${DB_NAME}_restore RENAME TO $DB_NAME;'"
log "  3. Start services: systemctl start zhamlik zhamlik-crypto zhamlik-mobile"
log ""
log "To rollback, simply rename the databases back."

# Start services again
log "Starting zhamlik services..."
systemctl start zhamlik zhamlik-crypto zhamlik-mobile
