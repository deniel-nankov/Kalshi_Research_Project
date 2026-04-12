#!/usr/bin/env bash
# Run on a fresh EC2 instance as root (e.g. sudo bash bootstrap.sh).
# Installs uv system-wide, deps for Python wheels, and prepares /opt/kalshi-pipeline.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git curl ca-certificates build-essential awscli

export UV_INSTALL_DIR=/usr/local/bin
curl -LsSf https://astral.sh/uv/install.sh | sh

install -d -m 755 /opt/kalshi-pipeline
# Clone or rsync your repo into /opt/kalshi-pipeline (owned by ubuntu).
chown ubuntu:ubuntu /opt/kalshi-pipeline

echo "Next (as ubuntu):"
echo "  cd /opt && sudo -u ubuntu git clone <YOUR_REPO_URL> kalshi-pipeline"
echo "  cd /opt/kalshi-pipeline && sudo -u ubuntu uv sync"
echo "  sudo -u ubuntu install -m 600 your-local-.env /opt/kalshi-pipeline/.env   # KALSHI_API_KEY_ID + KALSHI_API_PRIVATE_KEY"
echo "  sudo bash infra/aws/install-systemd.sh"
echo "Data quality (after data is in place): sudo bash infra/aws/server-data-perfection.sh diagnose"
echo "Optional one-time historical backfill (long): sudo -u ubuntu bash -lc 'cd /opt/kalshi-pipeline && uv run python scripts/download_historical.py'"
