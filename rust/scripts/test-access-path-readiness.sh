#!/usr/bin/env bash
# Exercise the production startup check with a listener preceding its ready event.
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RUST_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
STARTUP_SCRIPT=${1:-run-realtikv-access-path.sh}
WORK_DIR=$(mktemp -d)
RUST_PID=""
trap 'if [[ -n "$RUST_PID" ]]; then kill "$RUST_PID" 2>/dev/null || true; wait "$RUST_PID" 2>/dev/null || true; fi; rm -rf "$WORK_DIR"' EXIT
RUST_LOG_FILE="${WORK_DIR}/node.log"
RUST_SQL_PORT=0
wait_for_port() { return 0; }
# Keep this test attached to the actual call site, including its exit behavior.
startup_check=$(awk '/^RUST_PID=\$!$/ { capture=1; next } capture && /^$/ { exit } capture { print }' "${SCRIPT_DIR}/${STARTUP_SCRIPT}")
[[ -n "$startup_check" ]]
printf '%s\n' '{"event":"mysql_tls","enabled":true}' >"$RUST_LOG_FILE"
(
  sleep 1
  printf '%s\n' '{"event":"cluster_session_node_ready"}' >>"$RUST_LOG_FILE"
  sleep 10
) &
RUST_PID=$!
( eval "$startup_check" )
grep -F '"event":"cluster_session_node_ready"' "$RUST_LOG_FILE" >/dev/null
echo 'PASS: TCP listener preceding ready does not fail startup'
kill "$RUST_PID"
wait "$RUST_PID" 2>/dev/null || true
dead_pid=$RUST_PID
RUST_PID=""
: >"$RUST_LOG_FILE"
if ( RUST_PID=$dead_pid; eval "$startup_check" ) >"${WORK_DIR}/dead.out" 2>&1; then
  echo 'FAIL: exited node accepted as ready' >&2
  exit 1
fi
grep -F 'exited before reporting ready' "${WORK_DIR}/dead.out"
# Advance Bash's clock instead of making the regression wait three minutes.
if (
  RUST_PID=$$
  sleep() { SECONDS=$((SECONDS + 180)); }
  eval "$startup_check"
) >"${WORK_DIR}/timeout.out" 2>&1; then
  echo 'FAIL: live node without a ready event accepted' >&2
  exit 1
fi
grep -F 'never reported ready within 180 seconds' "${WORK_DIR}/timeout.out"
echo 'PASS: exited and stuck nodes still fail startup'
