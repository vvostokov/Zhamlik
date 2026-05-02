#!/bin/bash
# PostgreSQL Backup Script for Zhamlik
# Usage: ./backup.sh [daily|weekly|monthly]

set -e

# Configuration
BACKUP_TYPE=${1:-daily}
BACKUP_BASE="/opt/backups/postgresql"
RETENTION_DAYS=30
RETENTION_WEEKS=12
RETENTION_MONTHS=12

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="$BACKUP_BASE/$BACKUP_TYPE"
LOG_FILE="/var/log/zhamlik/backup.log"

# Database configuration
DB_NAME="zhamlik_db"
DB_USER="zhamlik"
DB_HOST="localhost"
DB_PASS="zhamlik_secure_password_2024"

# Create directories
mkdir -p "$BACKUP_DIR"
mkdir -p /var/log/zhamlik

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=== Backup started: $BACKUP_TYPE ==="
log "Database: $DB_NAME"

# Set PGPASSWORD for pg_dump
export PGPASSWORD="$DB_PASS"

# Perform backup
BACKUP_FILE="$BACKUP_DIR/zhamlik_db_${BACKUP_TYPE}_${DATE}.sql.gz"
log "Creating backup: $BACKUP_FILE"

pg_dump -h "$DB_HOST" -U "$DB_USER" "$DB_NAME" | gzip > "$BACKUP_FILE"

# Check if backup was successful
if [ -f "$BACKUP_FILE" ] && [ -s "$BACKUP_FILE" ]; then
    BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    log "Backup completed successfully. Size: $BACKUP_SIZE"
else
    log "ERROR: Backup failed!"
    exit 1
fi

# Clean old backups based on type
log "Cleaning old $BACKUP_TYPE backups..."

case $BACKUP_TYPE in
    daily)
        # Keep daily backups for RETENTION_DAYS days
        find "$BACKUP_DIR" -name "zhamlik_db_daily_*.sql.gz" -mtime +$RETENTION_DAYS -delete
        log "Kept daily backups for $RETENTION_DAYS days"
        ;;
    weekly)
        # Keep weekly backups for RETENTION_WEEKS weeks
        find "$BACKUP_DIR" -name "zhamlik_db_weekly_*.sql.gz" -mtime +$((RETENTION_WEEKS * 7)) -delete
        log "Kept weekly backups for $RETENTION_WEEKS weeks"
        ;;
    monthly)
        # Keep monthly backups for RETENTION_MONTHS months (approx 30 days each)
        find "$BACKUP_DIR" -name "zhamlik_db_monthly_*.sql.gz" -mtime +$((RETENTION_MONTHS * 30)) -delete
        log "Kept monthly backups for $RETENTION_MONTHS months"
        ;;
esac

# List current backups
log "Current $BACKUP_TYPE backups:"
ls -lh "$BACKUP_DIR"/zhamlik_db_${BACKUP_TYPE}_*.sql.gz 2>/dev/null | tail -5 | tee -a "$LOG_FILE"

unset PGPASSWORD
log "=== Backup completed ==="
