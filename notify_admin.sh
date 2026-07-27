#!/bin/bash
# Sends a Telegram message to every admin in ADMIN_TELEGRAM_IDS, straight
# via the Bot API (not through the running bot process) so it still works
# when the thing being reported on is "the bot process is dead." Used by
# watchdog.sh (hang detection) and digitalhub-crash-notify.service (OnFailure).
#
# Usage: notify_admin.sh <message> [cooldown_key] [cooldown_secs]
# The optional cooldown collapses repeated alerts for the same incident
# (e.g. a crash loop restarting every few seconds) into one ping per window.
set -u

ENV_FILE="/home/ubuntu/digitalhub/.env"
[ -f "$ENV_FILE" ] || exit 0
set -a
source "$ENV_FILE"
set +a

[ -n "${TELEGRAM_BOT_TOKEN:-}" ] || exit 0
[ -n "${ADMIN_TELEGRAM_IDS:-}" ] || exit 0

MSG="${1:-Digital Hub Bot: unspecified alert}"
COOLDOWN_KEY="${2:-}"
COOLDOWN_SECS="${3:-0}"

if [ -n "$COOLDOWN_KEY" ] && [ "$COOLDOWN_SECS" -gt 0 ]; then
    STATE_FILE="/home/ubuntu/digitalhub/.alert_state_${COOLDOWN_KEY}"
    now=$(date +%s)
    last=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
    if [ $((now - last)) -lt "$COOLDOWN_SECS" ] 2>/dev/null; then
        exit 0
    fi
    echo "$now" > "$STATE_FILE"
fi

IFS=',' read -ra IDS <<< "$ADMIN_TELEGRAM_IDS"
for id in "${IDS[@]}"; do
    id="$(echo "$id" | xargs)"
    [ -n "$id" ] || continue
    curl -fsS -m 10 -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        --data-urlencode "chat_id=${id}" \
        --data-urlencode "text=${MSG}" \
        --data-urlencode "parse_mode=HTML" >/dev/null 2>&1
done
