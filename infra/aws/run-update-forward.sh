#!/usr/bin/env bash
# Invoked by systemd (kalshi-forward.service). Loads project .env; optional
# UPDATE_FORWARD_EXTRA_ARGS from /etc/kalshi/forward-exec.env (inherited env).
set -euo pipefail
ROOT=/opt/kalshi-pipeline
cd "$ROOT"
set -a
[[ -f "$ROOT/.env" ]] && . "$ROOT/.env"
set +a
# shellcheck disable=SC2086
exec /usr/local/bin/uv run python scripts/update_forward.py ${UPDATE_FORWARD_EXTRA_ARGS-}
