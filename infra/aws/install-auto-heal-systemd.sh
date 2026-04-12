#!/usr/bin/env bash
# Optional: incremental orphan self-heal. Safe default is AUTO_HEAL_ORPHANS_MAX=0 (no API calls).
# Run as root after the repo is at /opt/kalshi-pipeline and .env exists.
set -euo pipefail
ROOT=/opt/kalshi-pipeline
UNIT_DIR="$ROOT/infra/aws/systemd"
if [[ ! -d "$UNIT_DIR" ]]; then
  echo "Expected $UNIT_DIR — clone the repo to $ROOT first."
  exit 1
fi
install -d -m 755 /etc/kalshi
if [[ ! -f /etc/kalshi/auto-heal.env ]]; then
  cp "$ROOT/infra/aws/auto-heal.env.example" /etc/kalshi/auto-heal.env
  chmod 644 /etc/kalshi/auto-heal.env
  echo "Created /etc/kalshi/auto-heal.env — edit AUTO_HEAL_ORPHANS_MAX to enable (e.g. 50)."
fi
cp "$UNIT_DIR/kalshi-auto-heal.service" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-auto-heal.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now kalshi-auto-heal.timer
echo "Enabled kalshi-auto-heal.timer (weekly). Logs: journalctl -u kalshi-auto-heal.service -n 200"
echo "While AUTO_HEAL_ORPHANS_MAX=0 the service is a no-op each run."
