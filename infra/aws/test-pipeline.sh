#!/usr/bin/env bash
# Run on EC2 after clone + uv sync + .env (as ubuntu, from repo root or any cwd).
# 1) Preflight Parquet + env  2) Dry-run forward (no writes)  3) Print next step.
set -euo pipefail
ROOT=/opt/kalshi-pipeline
cd "$ROOT"
set -a
[[ -f "$ROOT/.env" ]] && . "$ROOT/.env"
[[ -f /etc/kalshi/forward-exec.env ]] && . /etc/kalshi/forward-exec.env
set +a

echo "=== Step 1/2: preflight_ec2_pipeline.py (strict) ==="
/usr/local/bin/uv run python scripts/preflight_ec2_pipeline.py --strict

echo ""
echo "=== Step 2/2: update_forward.py --dry-run (no checkpoint/parquet writes) ==="
if [[ -n "${UPDATE_FORWARD_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2086
  /usr/local/bin/uv run python scripts/update_forward.py $UPDATE_FORWARD_EXTRA_ARGS --dry-run
else
  /usr/local/bin/uv run python scripts/update_forward.py --dry-run
fi

echo ""
echo "=== PREFLIGHT OK ==="
echo "Full pre-deploy rehearsal (no S3 / no timer changes): bash $ROOT/infra/aws/deploy_dry_run.sh"
echo "Enable production timers (if not already): sudo bash $ROOT/infra/aws/install-systemd.sh"
echo "Then: sudo systemctl start kalshi-forward.service"
