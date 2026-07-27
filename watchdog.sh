#!/bin/bash
# Restarts the digitalhub service if it stops answering health checks --
# covers a hung-but-alive process, which plain `Restart=always` in the
# systemd unit does not (that only fires when the process actually exits).
set -u

URL="http://127.0.0.1:8080/api/healthz"
TIMEOUT=5
GRACE_SECS=45

if curl -fsS -m "$TIMEOUT" "$URL" >/dev/null 2>&1; then
    exit 0
fi

# Don't fight a service that's still legitimately starting up -- DB
# connect + Telegram webhook registration alone takes a few seconds, and
# this timer fires every minute regardless of when the service last
# (re)started. Without this check, a restart (from this script, a deploy,
# or a real crash) landing in the same minute as the next timer tick would
# see a not-yet-listening port and trigger a second, unnecessary restart
# -- observed happening in practice during a manual restart.
started_at=$(systemctl show digitalhub.service -p ActiveEnterTimestampMonotonic --value)
now=$(cut -d' ' -f1 /proc/uptime | cut -d. -f1)
now_us=$((now * 1000000))
if [ -n "$started_at" ] && [ "$started_at" -gt 0 ] 2>/dev/null; then
    elapsed=$(( (now_us - started_at) / 1000000 ))
    if [ "$elapsed" -lt "$GRACE_SECS" ]; then
        logger -t digitalhub-watchdog "healthz check failed but service started ${elapsed}s ago (<${GRACE_SECS}s grace) -- skipping"
        exit 0
    fi
fi

logger -t digitalhub-watchdog "healthz check failed, restarting digitalhub.service"
# Cooldown so a stuck-in-a-loop hang (checked every minute) doesn't turn
# into a ping per minute -- one alert per 10-minute window is enough to
# know it's happening without drowning the admin chat.
sudo -u ubuntu /bin/bash /home/ubuntu/digitalhub/notify_admin.sh \
    "⚠️ <b>Digital Hub Bot</b> stopped responding to health checks — restarting it now." \
    healthz 600
systemctl restart digitalhub.service
