#!/usr/bin/env bash
# End-to-end smoke test runner for descending-order indexes
# (pingcap/tidb#2519).
#
# Boots a tiup playground cluster with caller-supplied TiDB and TiKV
# binaries, runs tests/desc_index_e2e.sql against it via the mysql
# client, and tears the cluster down on exit. Output is split into
# "playground.log" (TiDB / TiKV / PD logs) and "e2e.out" (SQL output
# you'll want to diff against the expectations annotated in the SQL
# file).
#
# Usage:
#   TIDB_BIN=/path/to/bin/tidb-server \
#   TIKV_BIN=/path/to/target/release/tikv-server \
#     ./tests/run_desc_index_e2e.sh
#
# Optional environment overrides:
#   SQL_FILE   path to the SQL script (default: alongside this script)
#   TIUP_TAG   playground tag (default: desc-e2e)
#   WAIT_SECS  seconds to wait for TiDB to come up (default: 120)
#   OUT_DIR    where to write playground.log + e2e.out (default: mktemp)
#
# Requires: tiup, mysql client, plus the two binaries above. The TiKV
# binary must include the descending-order coprocessor changes from
# tikv/tikv#19558.

set -euo pipefail

: "${TIDB_BIN:?set TIDB_BIN to the path of the desc-index TiDB binary}"
: "${TIKV_BIN:?set TIKV_BIN to the path of the desc-index TiKV binary}"
SQL_FILE="${SQL_FILE:-$(dirname "$0")/desc_index_e2e.sql}"
TIUP_TAG="${TIUP_TAG:-desc-e2e}"
WAIT_SECS="${WAIT_SECS:-120}"
OUT_DIR="${OUT_DIR:-$(mktemp -d -t desc-e2e-XXXXXX)}"

for f in "$TIDB_BIN" "$TIKV_BIN" "$SQL_FILE"; do
  if [[ ! -f "$f" ]]; then
    echo "missing: $f" >&2
    exit 1
  fi
done

echo ">>> output directory: $OUT_DIR"
echo ">>> tidb-server:      $TIDB_BIN"
echo ">>> tikv-server:      $TIKV_BIN"
echo ">>> sql file:         $SQL_FILE"

# Kick off tiup playground; record its PID so we can tear down cleanly.
# Flag names: --db / --kv / --pd (not --tidb/--tikv); binpath flags
# follow the same convention.
tiup playground nightly \
  --kv 1 --db 1 --pd 1 \
  --kv.binpath "$TIKV_BIN" \
  --db.binpath "$TIDB_BIN" \
  --tag "$TIUP_TAG" \
  --without-monitor \
  > "$OUT_DIR/playground.log" 2>&1 &
PLAYGROUND_PID=$!

cleanup() {
  echo
  echo ">>> tearing down playground (pid=$PLAYGROUND_PID)"
  # tiup playground installs a signal handler that closes the children.
  kill "$PLAYGROUND_PID" 2>/dev/null || true
  wait "$PLAYGROUND_PID" 2>/dev/null || true
  echo ">>> logs:    $OUT_DIR/playground.log"
  echo ">>> output:  $OUT_DIR/e2e.out"
}
trap cleanup EXIT

# Wait for TiDB to start accepting connections (port 4000).
echo ">>> waiting up to ${WAIT_SECS}s for TiDB to come up on 127.0.0.1:4000"
for _ in $(seq 1 "$WAIT_SECS"); do
  if mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo ">>> TiDB is up"
    break
  fi
  sleep 1
done

if ! mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT 1" >/dev/null 2>&1; then
  echo "TiDB did not come up within ${WAIT_SECS}s; see $OUT_DIR/playground.log" >&2
  exit 1
fi

# Run the SQL file. Use --table for human-readable output (with column
# headers), --comments so the section banners survive into the log, and
# --force so the deliberate error case in Section 3 (CLUSTERED PRIMARY
# KEY with DESC must be rejected) does not abort the rest of the script.
# Pipe via stdin rather than `-e "source $SQL_FILE"` because `source`
# runs in single-statement mode and silently ignores --force. Both
# stdout and stderr are tee'd so expected-error lines show up inline
# next to the matching SQL statement.
echo ">>> running $SQL_FILE"
mysql -h 127.0.0.1 -P 4000 -u root --table --comments --force \
  --default-character-set=utf8mb4 \
  < "$SQL_FILE" 2>&1 \
  | tee "$OUT_DIR/e2e.out"

echo
echo ">>> e2e run finished — review $OUT_DIR/e2e.out for unexpected rows."
