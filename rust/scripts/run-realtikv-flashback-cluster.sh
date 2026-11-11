#!/usr/bin/env bash

# FLASHBACK CLUSTER on a real TiKV.
#
# The node tests run the flashback job behind Go's `mockFlashbackTest`
# failpoint, which skips the region RPC loops. This starts an owned TiUP
# playground (PD + 1 TiKV, no TiDB), runs the built `tidb-server` against it
# and checks, as Go's `tests/realtikvtest/flashbacktest` does, that rows and
# schema are back at the flashback timestamp, that a timestamp before the GC
# safe point, an unsupported DDL or a system-schema DDL in the window is
# refused, and that the job restores what it closed. Then it
# tears the playground down.
#
# Needs: tiup, a mysql client that still loads mysql_native_password
# (FLASHBACK_MYSQL, default `mysql`), and `cargo build -p tidb-server`.

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SERVER=${FLASHBACK_TIDB_SERVER:-${RUST_ROOT}/target/debug/tidb-server}
MYSQL=${FLASHBACK_MYSQL:-mysql}
TIKV_VERSION=${FLASHBACK_TIKV_VERSION:-v8.5.7}
PORT_OFFSET=${FLASHBACK_PORT_OFFSET:-43000}
SQL_PORT=${FLASHBACK_SQL_PORT:-14471}
TAG="realtikv-flashback-cluster-${$}"
PD_ADDR="127.0.0.1:$((2379 + PORT_OFFSET))"
LOG_DIR=${TMPDIR:-/tmp}
PLAYGROUND_PID=
NODE_PID=

cleanup() {
  [[ -n "${NODE_PID}" ]] && kill "${NODE_PID}" 2>/dev/null && wait "${NODE_PID}" 2>/dev/null || true
  [[ -n "${PLAYGROUND_PID}" ]] && kill "${PLAYGROUND_PID}" 2>/dev/null && wait "${PLAYGROUND_PID}" 2>/dev/null || true
  tiup clean "${TAG}" --all >/dev/null 2>&1 || true
}
trap cleanup EXIT

tiup playground "${TIKV_VERSION}" --tag "${TAG}" --db 0 --tiflash 0 --kv 1 --pd 1 \
  --port-offset "${PORT_OFFSET}" --without-monitor >"${LOG_DIR}/${TAG}-playground.log" 2>&1 &
PLAYGROUND_PID=$!
for _ in $(seq 1 120); do
  curl -s -m 1 "http://${PD_ADDR}/pd/api/v1/stores" 2>/dev/null | grep -q '"state_name": "Up"' && break
  sleep 1
done

"${SERVER}" --path "${PD_ADDR}" -P "${SQL_PORT}" --socket '' >"${LOG_DIR}/${TAG}-node.log" 2>&1 &
NODE_PID=$!
sql() {
  "${MYSQL}" -h 127.0.0.1 -P "${SQL_PORT}" -u root --batch --skip-column-names -e "$1" 2>&1
}
for _ in $(seq 1 120); do
  sql "select 1" >/dev/null 2>&1 && break
  sleep 1
done

expect() {
  local got
  got=$(sql "$1" || true)
  if [[ "${got}" != "$2" ]]; then
    echo "FAIL: $1" >&2
    echo "  want: $2" >&2
    echo "  got:  ${got}" >&2
    exit 1
  fi
}
expect_error() {
  local got
  got=$(sql "$1" || true)
  if [[ "${got}" != *"$2"* ]]; then
    echo "FAIL: $1" >&2
    echo "  want an error containing: $2" >&2
    echo "  got:  ${got}" >&2
    exit 1
  fi
}

# Go `MockGC`'s safePointSQL: the safe point two days back.
SAFE_POINT=$(date -v-48H "+%Y%m%d-%H:%M:%S %z" 2>/dev/null || date -d "48 hours ago" "+%Y%m%d-%H:%M:%S %z")
sql "INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '${SAFE_POINT}', '') ON DUPLICATE KEY UPDATE variable_value = '${SAFE_POINT}'"
sql "create table test.fb(a int primary key, b int)"
sql "insert into test.fb values (1,1),(2,2)"
sleep 2
TS=$(sql "select now(6)")
sleep 2
sql "insert into test.fb values (3,3)"
sql "update test.fb set b = 20 where a = 2"
sql "create table test.fb2(a int)"
sql "insert into test.fb2 values (7)"
expect "select * from test.fb order by a" $'1\t1\n2\t20\n3\t3'
# The stores' safe ts has to pass the flashback timestamp.
sleep 8

expect_error "flashback cluster to timestamp '$(date -v-72H "+%Y-%m-%d %H:%M:%S" 2>/dev/null || date -d "72 hours ago" "+%Y-%m-%d %H:%M:%S")'" "GC safe point"
expect "select * from test.fb order by a" $'1\t1\n2\t20\n3\t3'

expect "flashback cluster to timestamp '${TS}'" ""
expect "select * from test.fb order by a" $'1\t1\n2\t2'
expect "show tables in test" "fb"
expect "select job_type, state from information_schema.ddl_jobs order by job_id desc limit 1" $'flashback cluster\tsynced'
sql "insert into test.fb values (9,9)"
expect "select * from test.fb order by a" $'1\t1\n2\t2\n9\t9'
sql "create table test.fb3(a int)"
expect "show tables in test" $'fb\nfb3'
expect "select @@global.tidb_gc_enable, @@global.tidb_super_read_only, @@global.tidb_enable_auto_analyze, @@global.tidb_ttl_job_enable" $'1\t0\t1\t1'
# Go `checkAndSetFlashbackClusterInfo`: a schema diff in the window that
# flashback does not support, or one on a system schema, refuses the job
# (the message prints the timestamp's TSO time, milliseconds).
sleep 2
TS=$(sql "select now(3)")
sleep 2
sql "create placement policy fbp followers = 1"
expect_error "flashback cluster to timestamp '${TS}'" "Detected unsupported DDL job type(create placement policy) during [${TS}000, now), can't do flashback"
sleep 2
TS=$(sql "select now(3)")
sleep 2
sql "create table mysql.fb_system(a int)"
expect_error "flashback cluster to timestamp '${TS}'" "Detected modified system table during [${TS}000, now), can't do flashback"
expect "select job_type, state from information_schema.ddl_jobs order by job_id desc limit 1" $'flashback cluster\tcancelled'
expect "select @@global.tidb_gc_enable, @@global.tidb_super_read_only, @@global.tidb_enable_auto_analyze, @@global.tidb_ttl_job_enable" $'1\t0\t1\t1'
SCHEDULE=$(curl -s "http://${PD_ADDR}/pd/api/v1/config/schedule")
for limit in merge region leader hot-region replica; do
  if echo "${SCHEDULE}" | grep -q "\"${limit}-schedule-limit\": 0,"; then
    echo "FAIL: PD's ${limit}-schedule-limit is still 0 after the flashback" >&2
    exit 1
  fi
done
echo "flashback cluster on a real TiKV: ok"
