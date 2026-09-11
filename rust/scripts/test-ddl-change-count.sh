#!/usr/bin/env bash
# Fixture DDL must not mask an extra mutation by CREATE IF NOT EXISTS.
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
RUST_LOG="$WORK_DIR/node.log"
DATABASE=campaign31
MADE_DATABASE=campaign31_rust
MADE_TABLE=made_by_rust
rust_node() {
  if [[ "$*" == *'IF NOT EXISTS'* ]]; then
    echo '{"event":"catalog_change","outcome":"already_satisfied"}' >> "$RUST_LOG"
    if [[ "$EXTRA_MUTATION" == 1 ]]; then
      echo '{"event":"catalog_change","outcome":"applied"}' >> "$RUST_LOG"
    fi
  else
    echo '{"event":"catalog_change","outcome":"applied"}' >> "$RUST_LOG"
  fi
}
phase=$(awk '/^APPLIED_BEFORE=|^rust_node -e "CREATE DATABASE/ { capture=1 } capture && /^# ----/ { exit } capture { print }' "$SCRIPT_DIR/run-realtikv-ddl.sh")
[[ -n "$phase" ]]
seed_log() {
  : > "$RUST_LOG"
  for _ in 1 2 3 4; do
    echo '{"event":"catalog_change","outcome":"applied"}' >> "$RUST_LOG"
  done
}
seed_log
EXTRA_MUTATION=0
( eval "$phase" )
seed_log
EXTRA_MUTATION=1
if ( eval "$phase" ) > "$WORK_DIR/extra.out" 2>&1; then
  echo 'FAIL: IF NOT EXISTS was allowed to mutate the catalog' >&2
  exit 1
fi
grep -qF 'expected exactly two applied catalog changes' "$WORK_DIR/extra.out"
echo 'PASS: fixture changes excluded and an extra catalog mutation rejected'
