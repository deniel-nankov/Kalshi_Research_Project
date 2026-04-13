#!/usr/bin/env bash
# Install Astral uv to /usr/local/bin. Run as root (same as bootstrap.sh).
set -euo pipefail
export UV_INSTALL_DIR=/usr/local/bin
curl -LsSf https://astral.sh/uv/install.sh | sh
command -v /usr/local/bin/uv >/dev/null
echo "OK: $(/usr/local/bin/uv --version)"
