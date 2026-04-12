#!/usr/bin/env bash
#
# Planned maintenance: dry-run (safe) or execute (stops writers, dedupe, restarts).
#
# Dry-run (no systemctl, no Parquet repair; runs full institutional validation + orphan audit):
#   ./scripts/institutional_maintenance.sh --dry-run
#
# Execute maintenance window (requires root for systemctl; stops kalshi timers, APPLY_REPAIR, restart):
#   sudo CONFIRM_MAINTENANCE=I_ACCEPT_DOWNTIME ./scripts/institutional_maintenance.sh --execute
#
# Optional canonical forward_markets repair after dedupe (long; see docs/DATA_REPAIR.md):
#   sudo CONFIRM_MAINTENANCE=I_ACCEPT_DOWNTIME RUN_MARKETS_CANONICAL=1 ./scripts/institutional_maintenance.sh --execute
#
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
UV="${UV:-uv}"

DRY=0
EXEC=0
for a in "$@"; do
  case "$a" in
    --dry-run) DRY=1 ;;
    --execute) EXEC=1 ;;
    *) echo "Unknown arg: $a (use --dry-run or --execute)" >&2; exit 2 ;;
  esac
done

if [[ "$DRY" -eq 1 && "$EXEC" -eq 1 ]]; then
  echo "Use only one of --dry-run or --execute" >&2
  exit 2
fi
if [[ "$DRY" -eq 0 && "$EXEC" -eq 0 ]]; then
  echo "Specify --dry-run or --execute" >&2
  exit 2
fi

TIMERS=(kalshi-forward.timer kalshi-health.timer kalshi-observability.timer kalshi-auto-heal.timer)
OPS_SERVICE=kalshi-ops-console.service

step() {
  echo ""
  echo "======================================================================"
  echo " $1"
  echo "======================================================================"
}

restart_timers() {
  for t in "${TIMERS[@]}"; do
    systemctl start "$t" 2>/dev/null || true
  done
}

restart_ops_console() {
  systemctl start "$OPS_SERVICE" 2>/dev/null || true
}

restart_all() {
  restart_timers
  restart_ops_console
}

if [[ "$DRY" -eq 1 ]]; then
  step "MAINTENANCE DRY-RUN - no services stopped; no APPLY_REPAIR"
  echo "Would run later in --execute:"
  echo "  systemctl stop $OPS_SERVICE"
  for t in "${TIMERS[@]}"; do
    echo "  systemctl stop $t"
  done
  ORPHAN_AUDIT=1 bash scripts/institutional_data_release.sh
  echo ""
  echo "Would run after maintenance body:"
  for t in "${TIMERS[@]}"; do
    echo "  systemctl start $t"
  done
  echo "  systemctl start $OPS_SERVICE"
  echo ""
  echo "DRY-RUN OK. Review output; when ready, schedule a window and run:"
  echo "  sudo CONFIRM_MAINTENANCE=I_ACCEPT_DOWNTIME $0 --execute"
  exit 0
fi

# --execute
if [[ "${CONFIRM_MAINTENANCE:-}" != "I_ACCEPT_DOWNTIME" ]]; then
  echo "Refusing execute: set CONFIRM_MAINTENANCE=I_ACCEPT_DOWNTIME" >&2
  exit 1
fi
if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "Execute mode needs root for systemctl (sudo)." >&2
  exit 1
fi

stopped_any=0
cleanup() {
  if [[ "$stopped_any" -eq 1 ]]; then
    echo ""
    echo ">>> cleanup: restarting kalshi timers + ops console (best-effort)"
    restart_all || true
  fi
}
trap cleanup EXIT

step "Stopping kalshi ops console + timers"
if systemctl stop "$OPS_SERVICE" 2>/dev/null; then
  stopped_any=1
fi
for t in "${TIMERS[@]}"; do
  if systemctl stop "$t" 2>/dev/null; then
    stopped_any=1
  fi
done

step "Institutional release with APPLY_REPAIR=1 + orphan audit"
export APPLY_REPAIR=1
export ORPHAN_AUDIT=1
bash scripts/institutional_data_release.sh

if [[ "${RUN_MARKETS_CANONICAL:-0}" == "1" ]]; then
  step "Canonical forward_markets (fix_forward_markets_dedupe --exclude-historical)"
  "$UV" run python scripts/fix_forward_markets_dedupe.py --yes --exclude-historical
  "$UV" run python scripts/validate_forward_pipeline.py --skip-run
  "$UV" run python scripts/validate_data_health.py \
    --output data/kalshi/state/health_report_post_canonical.json \
    --strict
fi

stopped_any=0
trap - EXIT
step "Starting kalshi timers + ops console"
restart_all

echo ""
echo "MAINTENANCE EXECUTE: completed."
