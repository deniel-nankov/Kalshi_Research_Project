#!/usr/bin/env bash
# Install AWS CLI v2 to /usr/local/bin (official bundle). Run as root.
# Use when `apt install awscli` has no candidate (common on minimal Noble images).
set -euo pipefail

if command -v aws >/dev/null 2>&1; then
  echo "aws already present: $(command -v aws) ($(aws --version 2>&1))"
  exit 0
fi

export DEBIAN_FRONTEND=noninteractive
apt-get install -y curl unzip

arch="$(uname -m)"
case "$arch" in
  x86_64) zipname=awscli-exe-linux-x86_64.zip ;;
  aarch64) zipname=awscli-exe-linux-aarch64.zip ;;
  *) echo "Unsupported architecture: $arch" >&2; exit 1 ;;
esac

tmp="$(mktemp -d)"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT

curl -fsSL "https://awscli.amazonaws.com/${zipname}" -o "$tmp/awscliv2.zip"
unzip -q "$tmp/awscliv2.zip" -d "$tmp"
"$tmp/aws/install" --update

trap - EXIT
cleanup

command -v aws >/dev/null
echo "Installed: $(command -v aws) ($(aws --version 2>&1))"
