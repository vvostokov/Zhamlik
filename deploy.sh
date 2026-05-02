#!/bin/bash
# Zhamlik Deployment Script
# Usage: ./deploy.sh [branch]

set -e

BRANCH=${1:-master}
APP_DIR="/opt/zhamlik"
BACKUP_BASE="/opt/backups/zhamlik"
BACKUP_DIR="$BACKUP_BASE/$(date +%Y%m%d_%H%M%S)"
LOG_FILE="/var/log/zhamlik/deploy.log"

# Create directories
mkdir -p "$BACKUP_BASE"
mkdir -p /var/log/zhamlik

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=== Deployment started ==="
log "Branch: $BRANCH"

# Backup current version
log "Creating backup..."
cp -r "$APP_DIR" "$BACKUP_DIR"

# Preserve local configuration
log "Preserving local configuration..."
cp "$APP_DIR/.env" /tmp/zhamlik.env.backup 2>/dev/null || true

# Pull latest changes
log "Pulling latest changes from git..."
cd "$APP_DIR"
git fetch origin
git reset --hard origin/"$BRANCH"

# Restore local configuration
log "Restoring local configuration..."
mv /tmp/zhamlik.env.backup "$APP_DIR/.env" 2>/dev/null || true

# Install/update dependencies
log "Updating Python dependencies..."
source "$APP_DIR/venv/bin/activate"
pip install -r requirements.txt --quiet

# Run migrations
log "Running database migrations..."
cd "$APP_DIR"
FLASK_APP=app.py flask db upgrade 2>&1 | tee -a "$LOG_FILE" || log "Migration warning: check logs"

# Collect static files if needed
log "Restarting gunicorn services..."
systemctl restart zhamlik 2>&1 | tee -a "$LOG_FILE" || true

# Wait for service to start
sleep 3

# Check service status
if systemctl is-active --quiet zhamlik; then
    log "Service restarted successfully"
else
    log "WARNING: Service may not be running properly"
    systemctl status zhamlik | tail -5 | tee -a "$LOG_FILE"
fi

log "=== Deployment completed ==="
log "Backup: $BACKUP_DIR"
