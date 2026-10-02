#!/usr/bin/env bash
# Exercise the DDL gate's JSON round trip through the shared catalog.
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK_DIR=$(mktemp -d)
READER_PID=
trap 'if [[ -n "$READER_PID" ]]; then kill "$READER_PID" 2>/dev/null || true; wait "$READER_PID" 2>/dev/null || true; fi; rm -rf "$WORK_DIR"' EXIT
DATABASE=campaign31
ANCHOR_TABLE=anchor
RUST_READER_PORT=0
READER_LOG="$WORK_DIR/reader.log"
rust_node() {
  return 0
}
start_rust_node() {
  printf '%s\n' "$@" > "$WORK_DIR/loaded"
  sleep 30 &
}
await_rust_ready() { return 0; }
rust_reader() {
  [[ $(wc -l < "$WORK_DIR/loaded") -eq 2 ]] || return 2
  printf '1\t{"v": 7}\n'
}
go_tidb() { printf '1\t{"v": 7}\n'; }
stop_rust_node() {
  kill "$1"
  wait "$1" 2>/dev/null || true
}
fixture=$(awk '/^rust_node -Nse \\/ { pending=$0; next } /CREATE TABLE .*\.unservable / { print pending; capture=1 } capture { print } /rust_node -Nse "DROP TABLE .*\.unservable"/ { exit }' "$SCRIPT_DIR/run-realtikv-ddl.sh")
[[ -n "$fixture" ]]
eval "$fixture"
[[ -f "$WORK_DIR/loaded" ]]
echo 'PASS: JSON DDL and reads use the shared catalog without table descriptors'
