#!/usr/bin/env bash
set -euo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-realtikv-optimistic-2pc.sh"
eval "$(sed -n '/^TAG=/p' "${script}" | head -1)"
eval "$(sed -n '/^validate_owned_paths() {$/,/^}$/p' "${script}")"
TIUP_HOME=/tmp/optimistic-2pc-path-test
SELF_TEST_ROOT=
TAG_DIR="${TIUP_HOME}/data/${TAG}"
PHASE_DIR="${TMPDIR:-/tmp}/${TAG}-phases"
validate_owned_paths || { echo 'production tag rejected' >&2; exit 1; }
PHASE_DIR="${TMPDIR:-/tmp}/unrelated"
if validate_owned_paths; then
  echo 'foreign phase directory accepted' >&2
  exit 1
fi
PHASE_DIR="${TMPDIR:-/tmp}/${TAG}-phases"
TAG_DIR="${TIUP_HOME}/data/unrelated"
if validate_owned_paths; then
  echo 'foreign data directory accepted' >&2
  exit 1
fi
TAG='realtikv-optimistic-2pc-123/../../unrelated'
TAG_DIR="${TIUP_HOME}/data/${TAG}"
PHASE_DIR="${TMPDIR:-/tmp}/${TAG}-phases"
if validate_owned_paths; then
  echo 'path traversal accepted' >&2
  exit 1
fi
echo 'PASS: own tag accepted; foreign and traversal paths rejected'
