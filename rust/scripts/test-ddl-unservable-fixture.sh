#!/usr/bin/env bash
# Exercise the DDL gate's actual fixture against an explicitly loaded catalog.
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
  case "$*" in
    *'SELECT id FROM'*) echo 'unknown table: campaign31.unservable'; return 1 ;;
    *) return 0 ;;
  esac
}
start_rust_node() {
  printf '%s\n' "$@" > "$WORK_DIR/loaded"
  sleep 30 &
}
await_rust_ready() { return 0; }
rust_reader() {
  grep -qx 'campaign31.anchor' "$WORK_DIR/loaded" || return 2
  grep -qx 'campaign31.unservable' "$WORK_DIR/loaded" || return 2
  echo 'column j has type JSON, which this node cannot decode yet'
  return 1
}
stop_rust_node() {
  kill "$1"
  wait "$1" 2>/dev/null || true
}
fixture=$(awk '/^rust_node -Nse \\/ { pending=$0; next } /CREATE TABLE .*\.unservable / { print pending; capture=1 } capture { print } /rust_node -Nse "DROP TABLE .*\.unservable"/ { exit }' "$SCRIPT_DIR/run-realtikv-ddl.sh")
[[ -n "$fixture" ]]
eval "$fixture"
[[ -f "$WORK_DIR/loaded" ]]
echo 'PASS: the refusal query targets a node that explicitly loaded the table'
