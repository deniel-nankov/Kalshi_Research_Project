#!/usr/bin/env bash
#
# Pre-deployment dry run - no forward Parquet writes, no S3 uploads, no systemctl changes.
# Safe to run on a laptop or on EC2 before install-systemd / CONFIRM_SYNC=1.
#
# From repo root (or any cwd):
#   ./infra/aws/deploy_dry_run.sh
#
# Optional:
#   SKIP_SLOW=1 ./infra/aws/deploy_dry_run.sh     # skip orphan --dry-run (can be slow)
#   STRICT_RELEASE=1 ./infra/aws/deploy_dry_run.sh  # also run institutional_data_release.sh (strict; slow)
#   DEPLOY_CHECK_SYSTEMD=1 ./infra/aws/deploy_dry_run.sh  # systemd-analyze verify on unit files (Linux)
#
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
UV="${UV:-uv}"

step() {
  echo ""
  echo "======================================================================"
  echo " DEPLOY DRY-RUN: $1"
  echo "======================================================================"
}

fail() {
  echo "DEPLOY DRY-RUN FAILED: $*" >&2
  exit 1
}

command -v "$UV" >/dev/null 2>&1 || fail "uv not found; install or set UV=/path/to/uv"

step "1/8 Shell syntax (infra + maintenance scripts)"
for f in "$ROOT/infra/aws"/*.sh; do
  [[ -f "$f" ]] || continue
  bash -n "$f" || fail "bash -n $f"
done
bash -n "$ROOT/scripts/institutional_maintenance.sh" || fail "bash -n institutional_maintenance.sh"
bash -n "$ROOT/scripts/institutional_data_release.sh" || fail "bash -n institutional_data_release.sh"
bash -n "$ROOT/scripts/sync_kalshi_data_to_s3.sh" || fail "bash -n sync_kalshi_data_to_s3.sh"
echo "OK: bash -n on deploy-related shell scripts"

if [[ "${DEPLOY_CHECK_SYSTEMD:-0}" == "1" ]] && command -v systemd-analyze >/dev/null 2>&1; then
  step "1b systemd unit file verify (optional)"
  for u in "$ROOT/infra/aws/systemd"/*.service "$ROOT/infra/aws/systemd"/*.timer; do
    [[ -f "$u" ]] || continue
    systemd-analyze verify "$u" || fail "systemd-analyze verify $u"
  done
  echo "OK: systemd-analyze verify"
fi

step "2/8 Preflight - Parquet readable (strict)"
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  [[ -f "$ROOT/.env" ]] && . "$ROOT/.env"
  set +a
  "$UV" run python scripts/preflight_ec2_pipeline.py --strict || fail "preflight (with .env)"
else
  "$UV" run python scripts/preflight_ec2_pipeline.py --strict --skip-api-keys || fail "preflight (no .env)"
fi

step "3/8 Forward ingest - update_forward.py --dry-run (needs .env + API)"
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  . "$ROOT/.env"
  [[ -f /etc/kalshi/forward-exec.env ]] && . /etc/kalshi/forward-exec.env
  set +a
  if [[ -n "${UPDATE_FORWARD_EXTRA_ARGS:-}" ]]; then
    # shellcheck disable=SC2086
    "$UV" run python scripts/update_forward.py $UPDATE_FORWARD_EXTRA_ARGS --dry-run
  else
    "$UV" run python scripts/update_forward.py --dry-run
  fi
else
  echo "SKIP: no .env - cannot dry-run Kalshi API (copy .env for full deploy rehearsal)"
fi

step "4/8 Data health - validate_data_health.py (strict = exit 1 only on FAIL)"
mkdir -p "$ROOT/data/kalshi/state"
"$UV" run python scripts/validate_data_health.py \
  --output "$ROOT/data/kalshi/state/health_report_deploy_dry_run.json" \
  --strict \
  || fail "validate_data_health has FAIL items"

step "5/8 Forward layout audit - validate_forward_pipeline.py --skip-run"
"$UV" run python scripts/validate_forward_pipeline.py --skip-run || fail "validate_forward_pipeline"

step "6/8 Duplicate / repair preflight - run_institutional_data_repair.py (no --apply)"
"$UV" run python scripts/run_institutional_data_repair.py || fail "run_institutional_data_repair preflight"

if [[ "${SKIP_SLOW:-0}" != "1" ]]; then
  step "7/8 Orphan audit - fix_orphan_tickers.py --dry-run (set SKIP_SLOW=1 to skip)"
  "$UV" run python scripts/fix_orphan_tickers.py --dry-run
else
  step "7/8 Orphan audit - SKIPPED (SKIP_SLOW=1)"
fi

step "8/8 AWS Tier 2 + S3 sync (both dry)"
"$UV" run python scripts/publish_tier2_observability.py --dry-run
if [[ -n "${S3_KALSHI_URI:-}" ]]; then
  if command -v aws >/dev/null 2>&1; then
    "$ROOT/scripts/sync_kalshi_data_to_s3.sh" || fail "S3 sync dry-run (aws s3 sync --dryrun)"
  else
    echo "SKIP: aws CLI not found - cannot run S3 sync dry-run"
  fi
else
  echo "S3_KALSHI_URI unset - sync script not invoked (export it to exercise aws s3 sync --dryrun)."
fi

if [[ "${STRICT_RELEASE:-0}" == "1" ]]; then
  step "EXTRA institutional_data_release.sh (strict gate; no APPLY_REPAIR unless you set it)"
  ORPHAN_AUDIT=1 bash scripts/institutional_data_release.sh || fail "institutional_data_release"
fi

echo ""
echo "======================================================================"
echo " DEPLOY DRY-RUN: completed OK (no production writes, no S3 upload, no systemctl)."
echo "======================================================================"
echo "Artifacts: data/kalshi/state/health_report_deploy_dry_run.json"
echo "Next: sudo bash infra/aws/install-systemd.sh  OR  CONFIRM_SYNC=1 ./scripts/sync_kalshi_data_to_s3.sh"
echo "Full release gate (optional): STRICT_RELEASE=1 ./infra/aws/deploy_dry_run.sh"
