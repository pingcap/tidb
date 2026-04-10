#!/usr/bin/env bash
set -euo pipefail

cores="$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN || echo unknown)"
loadavg="$(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || uptime || echo unknown)"

echo "[bazel-test] cores=${cores}"
echo "[bazel-test] loadavg=${loadavg}"

exec "$@"
