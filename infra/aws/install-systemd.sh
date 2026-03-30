#!/usr/bin/env bash
# Run as root after the repo lives at /opt/kalshi-pipeline and .env is in place.
set -euo pipefail
ROOT=/opt/kalshi-pipeline
UNIT_DIR="$ROOT/infra/aws/systemd"
if [[ ! -d "$UNIT_DIR" ]]; then
  echo "Expected $UNIT_DIR — clone the repo to $ROOT first."
  exit 1
fi
install -d -m 755 /etc/kalshi
if [[ ! -f /etc/kalshi/forward-exec.env ]]; then
  cp "$ROOT/infra/aws/forward-exec.env.example" /etc/kalshi/forward-exec.env
  chmod 644 /etc/kalshi/forward-exec.env
  echo "Created /etc/kalshi/forward-exec.env (edit UPDATE_FORWARD_EXTRA_ARGS if needed)."
fi
cp "$UNIT_DIR/kalshi-forward.service" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-forward.timer" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-health.service" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-health.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now kalshi-forward.timer
systemctl enable --now kalshi-health.timer
echo "Enabled timers. Check: systemctl list-timers | grep kalshi"
echo "Logs: journalctl -u kalshi-forward.service -f"
