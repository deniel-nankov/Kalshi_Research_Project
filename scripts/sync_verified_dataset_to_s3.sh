#!/usr/bin/env bash
# Upload ONLY after the institutional gate passes (strict health, forward audit, etc.).
# Does NOT upload if validate_data_health or any gate step fails.
#
# Prerequisites:
#   - Real Parquet under data/kalshi (not Git LFS pointers); EC2 IAM (or aws configure) can write to S3
#   - export S3_KALSHI_URI='s3://your-bucket/prefix/kalshi/'   # trailing slash on prefix is fine
#
# Usage (repo root):
#   ./scripts/sync_verified_dataset_to_s3.sh
#
# Optional:
#   SKIP_ORPHAN_AUDIT=1 ./scripts/sync_verified_dataset_to_s3.sh   # faster gate (skips fix_orphan_tickers --dry-run)
#   APPLY_REPAIR=1 ./scripts/sync_verified_dataset_to_s3.sh       # run dedupe apply before gate (maintenance)
#   RUN_TIER2_PUBLISH_AFTER_SYNC=1  # after successful s3 sync, run publish_tier2_observability.py (loads /etc/kalshi/observability.env if present)
#
# S3 bucket hardening (run once per bucket; replace BUCKET and REGION):
#   aws s3api put-bucket-versioning --bucket BUCKET --versioning-configuration Status=Enabled
#   aws s3api put-public-access-block --bucket BUCKET --public-access-block-configuration \
#     BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
#
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="/usr/local/bin:${PATH:-/usr/bin:/bin}"
: "${S3_KALSHI_URI:?Set S3_KALSHI_URI (see scripts/sync_kalshi_data_to_s3.sh)}"

command -v aws >/dev/null 2>&1 || { echo "aws CLI not found (install infra/aws/install-aws-cli-v2.sh or add to PATH)" >&2; exit 1; }

if [[ "${SKIP_ORPHAN_AUDIT:-0}" == "1" ]]; then
  export ORPHAN_AUDIT=0
else
  export ORPHAN_AUDIT="${ORPHAN_AUDIT:-1}"
fi

echo "=== Institutional gate (upload runs only if this exits 0) ==="
bash scripts/institutional_data_release.sh

echo ""
echo "=== Gate passed: syncing data/kalshi -> $S3_KALSHI_URI ==="
CONFIRM_SYNC=1 bash scripts/sync_kalshi_data_to_s3.sh

echo ""
echo "OK: verified dataset synced to S3."

if [[ "${RUN_TIER2_PUBLISH_AFTER_SYNC:-0}" == "1" ]]; then
  echo ""
  echo "=== Tier 2 (CloudWatch + optional S3 snapshot via observability.env) ==="
  if [[ -f /etc/kalshi/observability.env ]]; then
    set -a
    # shellcheck disable=SC1091
    . /etc/kalshi/observability.env
    set +a
  fi
  UV_BIN="${UV:-}"
  if [[ -z "$UV_BIN" ]]; then
    if [[ -x /usr/local/bin/uv ]]; then
      UV_BIN=/usr/local/bin/uv
    else
      UV_BIN=uv
    fi
  fi
  command -v "$UV_BIN" >/dev/null 2>&1 || {
    echo "Tier 2: uv not found; install uv or set UV=/path/to/uv" >&2
    exit 1
  }
  "$UV_BIN" run python scripts/publish_tier2_observability.py
  echo "OK: Tier 2 publish finished."
fi
