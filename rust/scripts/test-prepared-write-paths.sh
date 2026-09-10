#!/usr/bin/env bash
set -euo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-realtikv-prepared-write.sh"
# Exercise the production tag and guard without invoking cleanup or TiUP.
eval "$(sed -n '/^TAG=/p' "${script}" | head -1)"
eval "$(sed -n '/^validate_owned_paths() {$/,/^}$/p' "${script}")"
TIUP_HOME=/tmp/prepared-write-path-test
SELF_TEST_ROOT=
TAG_DIR="${TIUP_HOME}/data/${TAG}"
validate_owned_paths || { echo 'production tag rejected' >&2; exit 1; }

TAG_DIR="${TIUP_HOME}/data/unrelated"
if validate_owned_paths; then
  echo 'foreign directory accepted' >&2
  exit 1
fi
TAG='realtikv-prepared-write-123/../../unrelated'
TAG_DIR="${TIUP_HOME}/data/${TAG}"
if validate_owned_paths; then
  echo 'path traversal accepted' >&2
  exit 1
fi
echo 'PASS: own tag accepted; foreign and traversal paths rejected'
