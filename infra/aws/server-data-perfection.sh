#!/usr/bin/env bash
#
# Server runbook: diagnostics, dedupe, optional canonical market repair, gap/orphan hints.
# Repo path defaults to /opt/kalshi-pipeline (override with KALSHI_PIPELINE_ROOT).
#
# Usage (as root on EC2):
#   sudo KALSHI_PIPELINE_ROOT=/opt/kalshi-pipeline bash infra/aws/server-data-perfection.sh diagnose
#   sudo bash infra/aws/server-data-perfection.sh repair          # stop timer → dedupe → validators → start timer
#   sudo bash infra/aws/server-data-perfection.sh repair-markets  # maintenance: canonical forward_markets + drop hist overlap
#   sudo bash infra/aws/server-data-perfection.sh orphans-audit   # dry-run orphan tickers (can be slow)
#
# Prerequisites: ubuntu user, .env with Kalshi keys, uv at /usr/local/bin/uv (see bootstrap.sh).
#
set -euo pipefail

ROOT="${KALSHI_PIPELINE_ROOT:-/opt/kalshi-pipeline}"
UV="${UV_BIN:-/usr/local/bin/uv}"
RUN_USER="${KALSHI_RUN_USER:-ubuntu}"
TIMER=kalshi-forward.timer

run_py() {
  local title=$1
  shift
  echo "=== ${title} ==="
  sudo -u "$RUN_USER" UV_BIN="$UV" env -C "$ROOT" bash -c '
    set -a
    [ -f .env ] && . ./.env
    set +a
    exec "$UV_BIN" run python "$@"
  ' bash "$@"
  echo ""
}

timer_stop() {
  if systemctl is-enabled "$TIMER" &>/dev/null; then
    systemctl stop "$TIMER" || true
    echo "Stopped $TIMER"
  fi
}

timer_start() {
  if systemctl is-enabled "$TIMER" &>/dev/null; then
    systemctl start "$TIMER" || true
    echo "Started $TIMER"
  fi
}

cmd=${1:-}
case "$cmd" in
  diagnose)
    run_py "Dataset stats (counts, range)" "scripts/data_stats.py"
    run_py "Preflight — duplicate estimates (no writes)" "scripts/run_institutional_data_repair.py"
    run_py "Full health report + JSON" "scripts/validate_data_health.py --output data/kalshi/state/health_report.json"
    echo "Review output above. FAIL must be fixed; WARN often matters (e.g. duplicate market keys, boundary gaps)."
    echo "Next: sudo bash infra/aws/server-data-perfection.sh repair"
    ;;
  repair)
    timer_stop
    run_py "Dedupe forward trades + markets + validate" "scripts/run_institutional_data_repair.py --apply"
    timer_start
    echo "Re-run: diagnose — aim for no FAIL; resolve WARN where you care (see docs/DATA_REPAIR.md)."
    ;;
  repair-markets)
    echo "Replaces forward_markets tree (backup created). Use during a maintenance window."
    echo "Stops $TIMER for safety. Press Enter to continue, Ctrl-C to abort (non-interactive: SKIP_CONFIRM=1)."
    if [[ "${SKIP_CONFIRM:-}" != "1" ]]; then
      read -r
    fi
    timer_stop
    run_py "Canonical forward markets (+ optional historical key overlap removal)" "scripts/fix_forward_markets_dedupe.py --yes --exclude-historical"
    run_py "Forward pipeline audit" "scripts/validate_forward_pipeline.py --skip-run"
    run_py "Full health" "scripts/validate_data_health.py --output data/kalshi/state/health_report.json"
    timer_start
    ;;
  orphans-audit)
    run_py "Orphan tickers (trades without market rows) — dry-run" "scripts/fix_orphan_tickers.py --dry-run"
    echo "To backfill orphans over many runs, see docs/DATA_REPAIR.md (fix_orphan_tickers --checkpoint --max)."
    ;;
  *)
    echo "Usage: $0 {diagnose|repair|repair-markets|orphans-audit}"
    exit 1
    ;;
esac
