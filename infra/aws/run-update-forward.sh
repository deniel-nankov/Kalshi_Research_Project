#!/usr/bin/env bash
# Invoked by systemd (kalshi-forward.service). Loads .env then optional CLI flags
# from /etc/kalshi/forward-exec.env (no EnvironmentFile= in the unit — systemd
# was misparsing ${UPDATE_FORWARD_EXTRA_ARGS} and breaking starts).
set -euo pipefail
ROOT=/opt/kalshi-pipeline
cd "$ROOT"
set -a
[[ -f "$ROOT/.env" ]] && . "$ROOT/.env"
[[ -f /etc/kalshi/forward-exec.env ]] && . /etc/kalshi/forward-exec.env
set +a
if [[ -n "${UPDATE_FORWARD_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2086
  exec /usr/local/bin/uv run python scripts/update_forward.py $UPDATE_FORWARD_EXTRA_ARGS
else
  exec /usr/local/bin/uv run python scripts/update_forward.py
fi
