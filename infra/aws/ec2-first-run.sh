#!/usr/bin/env bash
#
# Run once on a fresh Ubuntu EC2 instance (as root: sudo bash ec2-first-run.sh).
#
# Optional env:
#   GIT_URL      — default https://github.com/deniel-nankov/Kalshi_Research_Project.git (override for a fork)
#   GIT_BRANCH   — default main
#   RUN_DIAGNOSE — set to 1 to run server-data-perfection.sh diagnose after install
#   RUN_REPAIR   — set to 1 to run repair (dedupe); implies a maintenance window
#
set -euo pipefail

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "Run as root: sudo bash $0"
  exit 1
fi

GIT_URL="${GIT_URL:-https://github.com/deniel-nankov/Kalshi_Research_Project.git}"

BRANCH="${GIT_BRANCH:-main}"
ROOT=/opt/kalshi-pipeline

if [[ -d "$ROOT/.git" ]]; then
  echo "Updating existing repo at $ROOT"
  sudo -u ubuntu git -C "$ROOT" fetch origin
  sudo -u ubuntu git -C "$ROOT" checkout "$BRANCH"
  sudo -u ubuntu git -C "$ROOT" pull origin "$BRANCH"
else
  echo "Cloning into $ROOT"
  install -d -m 755 /opt
  rm -rf "$ROOT"
  sudo -u ubuntu git clone --branch "$BRANCH" "$GIT_URL" "$ROOT"
fi

echo "Installing system packages + uv"
bash "$ROOT/infra/aws/bootstrap.sh"

echo "Python dependencies (uv sync)"
sudo -u ubuntu bash -lc "cd \"$ROOT\" && /usr/local/bin/uv sync"

if [[ ! -f "$ROOT/.env" ]]; then
  echo ""
  echo ">>> Create $ROOT/.env with KALSHI_API_KEY_ID and KALSHI_API_PRIVATE_KEY, then:"
  echo "    sudo chmod 600 $ROOT/.env"
  echo "    sudo bash $ROOT/infra/aws/install-systemd.sh"
  echo "    Optional: sudo bash $ROOT/infra/aws/server-data-perfection.sh diagnose"
  exit 0
fi

echo "Installing systemd timers"
bash "$ROOT/infra/aws/install-systemd.sh"

if [[ "${RUN_DIAGNOSE:-0}" == "1" ]]; then
  bash "$ROOT/infra/aws/server-data-perfection.sh" diagnose || true
fi

if [[ "${RUN_REPAIR:-0}" == "1" ]]; then
  bash "$ROOT/infra/aws/server-data-perfection.sh" repair || true
fi

echo ""
echo "Done. Timers: systemctl list-timers | grep kalshi"
echo "Logs: journalctl -u kalshi-forward.service -f"
