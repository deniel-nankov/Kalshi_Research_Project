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
if [[ ! -f /etc/kalshi/observability.env ]]; then
  cp "$ROOT/infra/aws/observability.env.example" /etc/kalshi/observability.env
  chmod 644 /etc/kalshi/observability.env
  echo "Created /etc/kalshi/observability.env (Tier 2: CLOUDWATCH_NAMESPACE, optional S3_TIER2_URI)."
fi
if [[ ! -f /etc/kalshi/ops-console.env ]]; then
  cp "$ROOT/infra/aws/ops-console.env.example" /etc/kalshi/ops-console.env
  chmod 644 /etc/kalshi/ops-console.env
  echo "Created /etc/kalshi/ops-console.env (OPS_CONSOLE_INTERVAL for kalshi-ops-console.service)."
fi
chmod +x "$ROOT/infra/aws/run-update-forward.sh" 2>/dev/null || true
chmod +x "$ROOT/infra/aws/test-pipeline.sh" 2>/dev/null || true
chmod +x "$ROOT/infra/aws/run-ops-console-daemon.sh" 2>/dev/null || true
cp "$UNIT_DIR/kalshi-forward.service" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-forward.timer" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-health.service" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-health.timer" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-observability.service" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-observability.timer" /etc/systemd/system/
cp "$UNIT_DIR/kalshi-ops-console.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now kalshi-forward.timer
systemctl enable --now kalshi-health.timer
systemctl enable --now kalshi-observability.timer
systemctl enable --now kalshi-ops-console.service
echo "Enabled timers + kalshi-ops-console.service. Check: systemctl list-timers | grep kalshi"
echo "Ops snapshot: journalctl -u kalshi-ops-console.service -n 50"
echo "Logs: journalctl -u kalshi-forward.service -f"
echo "Before relying on timers: sudo -u ubuntu bash $ROOT/infra/aws/test-pipeline.sh"
