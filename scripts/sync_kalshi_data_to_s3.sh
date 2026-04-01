#!/usr/bin/env bash
# Sync data/kalshi/ to S3 using an access point or bucket URI.
#
# Prerequisite: run institutional_data_release.sh (with APPLY_REPAIR=1 if needed) until it passes.
#
# Configure AWS CLI (aws configure, or IAM role on EC2), then:
#
#   export S3_KALSHI_URI="s3://arn:aws:s3:us-east-1:123456789012:accesspoint/your-ap-name/optional-prefix/"
#
# Access point form (note trailing slash on prefix):
#   s3://arn:aws:s3:REGION:ACCOUNT_ID:accesspoint/ACCESS_POINT_NAME/kalshi/
#
# Or classic bucket:
#   export S3_KALSHI_URI="s3://my-bucket/path/to/kalshi/"
#
#   ./scripts/sync_kalshi_data_to_s3.sh              # dry-run
#   CONFIRM_SYNC=1 ./scripts/sync_kalshi_data_to_s3.sh   # real upload
#
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/data/kalshi"
: "${S3_KALSHI_URI:?Set S3_KALSHI_URI to your S3 access point or bucket URL (see script header)}"

command -v aws >/dev/null 2>&1 || { echo "aws CLI not found" >&2; exit 1; }
[[ -d "$SRC" ]] || { echo "Missing $SRC" >&2; exit 1; }

AWS_ARGS=(--only-show-errors)
[[ -n "${S3_STORAGE_CLASS:-}" ]] && AWS_ARGS+=(--storage-class "$S3_STORAGE_CLASS")

if [[ "${CONFIRM_SYNC:-}" != "1" ]]; then
  echo "DRY-RUN (no uploads). To upload for real: CONFIRM_SYNC=1 $0"
  aws s3 sync "$SRC" "$S3_KALSHI_URI" "${AWS_ARGS[@]}" --dryrun
  exit 0
fi

echo "Syncing $SRC -> $S3_KALSHI_URI"
# Exclude ingestion lock; include state/ JSON for checkpoints and health reports
aws s3 sync "$SRC" "$S3_KALSHI_URI" "${AWS_ARGS[@]}" \
  --exclude "state/forward_ingestion.lock"

echo "Done. Enable S3 versioning on the bucket for safer rollback."
