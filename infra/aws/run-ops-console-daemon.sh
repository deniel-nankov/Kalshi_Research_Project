#!/usr/bin/env bash
# Started by systemd kalshi-ops-console.service; keeps ops_snapshot.json fresh for Tier 2 S3.
set -euo pipefail
ROOT=/opt/kalshi-pipeline
cd "$ROOT"
set -a
[[ -f /etc/kalshi/ops-console.env ]] && . /etc/kalshi/ops-console.env
set +a
INTERVAL="${OPS_CONSOLE_INTERVAL:-300}"
ARGS=(--daemon --interval "$INTERVAL")
[[ -n "${OPS_CONSOLE_LOG_FILE:-}" ]] && ARGS+=(--log-file "$OPS_CONSOLE_LOG_FILE")
exec /usr/local/bin/uv run python scripts/institutional_ops_console.py "${ARGS[@]}"
