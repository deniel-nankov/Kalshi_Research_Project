#!/usr/bin/env bash
# Full institutional validation (and optional dedupe) before publishing data to S3 or archiving.
#
# Run from repo root (or any cwd — script cds to repo root):
#   ./scripts/institutional_data_release.sh              # validate only, no writes except health JSON
#   APPLY_REPAIR=1 ./scripts/institutional_data_release.sh   # run forward dedupe + re-validate
#   ORPHAN_AUDIT=1 ./scripts/institutional_data_release.sh   # add fix_orphan_tickers dry-run at end
#
# On a server, stop writers first:
#   sudo systemctl stop kalshi-forward.timer kalshi-health.timer
#
# Requires: uv, real Parquet under data/kalshi (not Git LFS pointers), .env for API only if orphans need API
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
UV="${UV:-uv}"

step() {
  echo ""
  echo "══════════════════════════════════════════════════════════════════════"
  echo " $1"
  echo "══════════════════════════════════════════════════════════════════════"
}

fail() {
  echo "RELEASE ABORTED: $*" >&2
  exit 1
}

command -v "$UV" >/dev/null 2>&1 || fail "uv not found; install or set UV=/path/to/uv"

step "1/5 Preflight — readable Parquet, no LFS stubs (strict; API keys optional)"
"$UV" run python scripts/preflight_ec2_pipeline.py --strict --skip-api-keys

step "2/5 Institutional health report (all checks; JSON artifact; strict)"
mkdir -p data/kalshi/state
"$UV" run python scripts/validate_data_health.py \
  --output data/kalshi/state/health_report_pre_release.json \
  --strict \
  || fail "validate_data_health: fix FAIL items (see docs/HEALTH_REPORT_WARNINGS_EXAMINED.md)"

step "3/5 Forward pipeline audit (overlap / duplicates across layout)"
"$UV" run python scripts/validate_forward_pipeline.py --skip-run

step "4/5 Duplicate estimates (preflight — no file changes)"
"$UV" run python scripts/run_institutional_data_repair.py

if [[ "${APPLY_REPAIR:-}" == "1" ]]; then
  step "4b APPLY forward dedupe (rewrites Parquet; backups created)"
  "$UV" run python scripts/run_institutional_data_repair.py --apply
  step "4c Re-validate after dedupe"
  "$UV" run python scripts/validate_forward_pipeline.py --skip-run
  "$UV" run python scripts/validate_data_health.py \
    --output data/kalshi/state/health_report_post_dedupe.json \
    --strict \
    || fail "Post-dedupe health still has FAIL"
else
  echo ""
  echo "Skipping dedupe APPLY (set APPLY_REPAIR=1 to run run_institutional_data_repair.py --apply)."
fi

if [[ "${ORPHAN_AUDIT:-}" == "1" ]]; then
  step "5/5 Orphan ticker audit (dry-run; may be slow)"
  "$UV" run python scripts/fix_orphan_tickers.py --dry-run
else
  step "5/5 Orphan audit skipped (set ORPHAN_AUDIT=1 for fix_orphan_tickers --dry-run)"
fi

step "Dataset stats snapshot"
"$UV" run python scripts/data_stats.py | tee data/kalshi/state/dataset_stats_release.txt

echo ""
echo "INSTITUTIONAL RELEASE GATE: completed OK."
echo "Artifacts: data/kalshi/state/health_report_*.json, dataset_stats_release.txt"
echo "Optional: CANONICAL_MARKETS=1 is NOT auto-run (fix_forward_markets_dedupe --exclude-historical); see docs/DATA_REPAIR.md"
echo "Next: ./scripts/sync_kalshi_data_to_s3.sh (set S3_KALSHI_URI)"
