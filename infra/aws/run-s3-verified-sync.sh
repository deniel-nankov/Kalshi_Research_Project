#!/usr/bin/env bash
# Invoked by systemd kalshi-s3-verified-sync.service. Loads repo .env then /etc/kalshi/s3-verified-sync.env.
set -euo pipefail
ROOT=/opt/kalshi-pipeline
cd "$ROOT"
set -a
[[ -f "$ROOT/.env" ]] && . "$ROOT/.env"
[[ -f /etc/kalshi/s3-verified-sync.env ]] && . /etc/kalshi/s3-verified-sync.env
set +a

if [[ "${ENABLE_KALSHI_S3_VERIFIED_SYNC:-0}" != "1" ]]; then
  echo "kalshi-s3-verified-sync: skipped (set ENABLE_KALSHI_S3_VERIFIED_SYNC=1 in /etc/kalshi/s3-verified-sync.env)"
  exit 0
fi
if [[ -z "${S3_KALSHI_URI:-}" ]]; then
  echo "kalshi-s3-verified-sync: ENABLE_KALSHI_S3_VERIFIED_SYNC=1 but S3_KALSHI_URI is empty — fix /etc/kalshi/s3-verified-sync.env" >&2
  exit 1
fi

export S3_KALSHI_URI
export PATH="/usr/local/bin:${PATH:-/usr/bin:/bin}"
export RUN_TIER2_PUBLISH_AFTER_SYNC="${RUN_TIER2_PUBLISH_AFTER_SYNC:-0}"
exec bash scripts/sync_verified_dataset_to_s3.sh
