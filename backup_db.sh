#!/bin/bash
# Daily Postgres backup for digitalhub, gzip'd and rotated. Run via
# digitalhub-backup.timer -- see /etc/systemd/system/digitalhub-backup.{service,timer}.
set -euo pipefail

BACKUP_DIR="/home/ubuntu/digitalhub/backups"
RETENTION_DAYS=14
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
DUMP_FILE="$BACKUP_DIR/digitalhub-$TIMESTAMP.sql.gz"

mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"

set -a
source /home/ubuntu/digitalhub/.env
set +a

pg_dump "$DATABASE_URL" | gzip > "$DUMP_FILE.tmp"
mv "$DUMP_FILE.tmp" "$DUMP_FILE"
chmod 600 "$DUMP_FILE"

# Prune anything older than the retention window.
find "$BACKUP_DIR" -name 'digitalhub-*.sql.gz' -mtime "+$RETENTION_DAYS" -delete

logger -t digitalhub-backup "backup complete: $DUMP_FILE ($(du -h "$DUMP_FILE" | cut -f1))"
