#!/usr/bin/env bash
# The contended lost-update check: two sessions, one row, blind increments.
#
# This is the harness for a bug a mock cannot see. An autocommit UPDATE reads at
# T_read and, before this was fixed, prewrote under a LATER fresh timestamp;
# TiKV's optimistic conflict check compares a key's latest commit_ts against the
# PREWRITING transaction's start_ts, so a commit landing in (T_read, T_write)
# was not a conflict TiKV could see and the stale value overwrote it in silence.
# Nothing but real MVCC under real contention exhibits that.
#
# Every scenario is `UPDATE lu SET v = v + N WHERE <predicate>` run blind, so
# the arithmetic is the oracle: the row must end at exactly
# `statements_a * step_a + statements_b * step_b`. Anything less is a lost
# update, and the harness reports the shortfall rather than a pass/fail alone.
#
# The two controls are not optional and are always run:
#   * Go vs Go       -- the method control. If this is short, the oracle is
#                       wrong and no other row means anything.
#   * Rust alone     -- the no-race control. If this is short, writes are being
#                       dropped outside any race and the race is not the story.
#
# The ranged-predicate scenario exists because the live loss was the same size
# with a non-point predicate (2498) as with `WHERE id = 1` (2538): a fix scoped
# to the point-get shape would not be a fix.
#
# Playground discipline: one background `tiup playground` at a port offset, a
# trap that tears it down, an after-the-trap check that every owned port is
# unreachable, and the tag directory deleted.
#
#   rust/scripts/run-lost-update-check.sh
#   LOST_UPDATE_PORT_OFFSET=43000 rust/scripts/run-lost-update-check.sh
set -uo pipefail

MYSQL_CLIENT=${LOST_UPDATE_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "LOST_UPDATE_MYSQL_CLIENT must name an executable stock MySQL client" >&2
  exit 1
fi
if ! command -v tiup >/dev/null 2>&1; then
  echo "tiup is required" >&2
  exit 1
fi

# MySQL client 8.0.34+ deprecated and 9.x ships `mysql_native_password` only as
# a loadable client plugin. Both servers here offer that plugin, so point the
# client at whichever directory actually has it; without this the readiness
# probe never connects and the run reports a cluster that is in fact up.
MYSQL_PLUGIN_ARGS=()
if [[ -n "${LOST_UPDATE_MYSQL_PLUGIN_DIR:-}" ]]; then
  MYSQL_PLUGIN_ARGS=(--plugin-dir="${LOST_UPDATE_MYSQL_PLUGIN_DIR}")
else
  MYSQL_BIN_DIR=$(cd "$(dirname "$(command -v "${MYSQL_CLIENT}")")" && pwd)
  for candidate in \
    "${MYSQL_BIN_DIR}/../opt/mysql-client/lib/plugin" \
    /opt/homebrew/opt/mysql-client/lib/plugin \
    /usr/local/opt/mysql-client/lib/plugin; do
    if [[ -f "${candidate}/mysql_native_password.so" ]]; then
      MYSQL_PLUGIN_ARGS=(--plugin-dir="${candidate}")
      break
    fi
  done
fi

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CLUSTER_VERSION=${LOST_UPDATE_CLUSTER_VERSION:-v9.0.0-beta.2.pre-nightly}
TAG="lost-update-${$}-$(date +%s)"
PORT_OFFSET=${LOST_UPDATE_PORT_OFFSET:-43000}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "LOST_UPDATE_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
  exit 1
fi
PD_PORT=$((2379 + PORT_OFFSET))
GO_SQL_PORT=$((4000 + PORT_OFFSET))
TIKV_SEED_PORT=$((20160 + PORT_OFFSET))
GO_STATUS_PORT=$((10080 + PORT_OFFSET))
RUST_SQL_PORT=$((12000 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
OUT_DIR=${LOST_UPDATE_OUT_DIR:-${TMPDIR:-/tmp}/${TAG}}
mkdir -p "${OUT_DIR}"
PLAYGROUND_LOG="${OUT_DIR}/playground.log"
RUST_LOG_FILE="${OUT_DIR}/rust-node.log"
CHECK_LOG="${OUT_DIR}/lost-update.log"
PLAYGROUND_PID=
RUST_PID=
RUNTIME_DIR=
AUTH_USER=lucheck
AUTH_PASSWORD=${LOST_UPDATE_AUTH_PASSWORD:-lucheck-native-password}
STATEMENTS=${LOST_UPDATE_STATEMENTS:-300}
DB=lucheck

step() { printf '\n===== %s =====\n' "$*" | tee -a "${CHECK_LOG}"; }
note() { printf '%s\n' "$*" | tee -a "${CHECK_LOG}"; }

collect_descendant_pids() {
  local frontier=$1 descendants= child next parent
  while [[ -n "${frontier}" ]]; do
    next=
    for parent in ${frontier}; do
      while IFS= read -r child; do
        if [[ -n "${child}" ]]; then
          descendants="${descendants}${descendants:+ }${child}"
          next="${next}${next:+ }${child}"
        fi
      done < <(pgrep -P "${parent}" || true)
    done
    frontier=${next}
  done
  printf '%s\n' "${descendants}"
}

cleanup() {
  local original_status=$?
  local cleanup_failed=false
  trap - EXIT INT TERM

  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    kill "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  local owned
  owned=$( { if [[ -n "${PLAYGROUND_PID}" ]]; then collect_descendant_pids "${PLAYGROUND_PID}"; fi
            pgrep -f "${TAG_DIR}" || true; } | awk 'NF && !seen[$1]++ { print $1 }' | tr '\n' ' ')
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  tiup clean "${TAG}" --all >/dev/null 2>&1 || true
  local pid
  for pid in ${owned}; do
    kill "${pid}" 2>/dev/null || true
  done
  local settled=false _
  for _ in $(seq 1 30); do
    local alive=false
    for pid in ${owned} ${RUST_PID}; do
      if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
        alive=true
        break
      fi
    done
    if [[ "${alive}" == false ]]; then
      settled=true
      break
    fi
    sleep 1
  done
  if [[ "${settled}" != true ]]; then
    for pid in ${owned} ${RUST_PID}; do
      [[ -n "${pid}" ]] && kill -9 "${pid}" 2>/dev/null || true
    done
    sleep 2
  fi

  local port
  for port in "${RUST_SQL_PORT}" "${GO_SQL_PORT}" "${GO_STATUS_PORT}" \
    "${TIKV_SEED_PORT}" "${PD_PORT}"; do
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "lost-update cleanup failed: 127.0.0.1:${port} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
    [[ -n "${RUNTIME_DIR}" ]] && rm -rf -- "${RUNTIME_DIR}"
  fi
  echo "lost-update artifacts: ${OUT_DIR}" >&2
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}

RUST_SERVER=${LOST_UPDATE_RUST_SERVER:-}
if [[ -z "${RUST_SERVER}" ]]; then
  ( cd "${RUST_ROOT}" && CARGO_BUILD_JOBS=12 cargo build -j12 --release \
      -p tidb-server --bin tidb-server ) || exit 1
  RUST_SERVER="${RUST_ROOT}/target/release/tidb-server"
fi
if [[ ! -x "${RUST_SERVER}" ]]; then
  echo "lost-update Rust server is not executable: ${RUST_SERVER}" >&2
  exit 1
fi

for port in "${PD_PORT}" "${GO_SQL_PORT}" "${TIKV_SEED_PORT}" \
  "${GO_STATUS_PORT}" "${RUST_SQL_PORT}"; do
  if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
    echo "refusing occupied lost-update port ${port}; set LOST_UPDATE_PORT_OFFSET" >&2
    exit 1
  fi
done

trap cleanup EXIT INT TERM

RUNTIME_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}-runtime.XXXXXX")
AUTH_FILE="${RUNTIME_DIR}/auth.tsv"
AUTH_HASH_HEX=$(printf '%s' "${AUTH_PASSWORD}" \
  | openssl dgst -sha1 -binary | openssl dgst -sha1 -hex \
  | awk '{ print toupper($NF) }')
(umask 077; printf '%s\t%s\t%s\t*%s\n' \
  "${AUTH_USER}" "127.0.0.1" "mysql_native_password" "${AUTH_HASH_HEX}" >"${AUTH_FILE}")
chmod 0600 "${AUTH_FILE}"
unset AUTH_HASH_HEX

step "cluster: TiUP playground ${CLUSTER_VERSION} at offset ${PORT_OFFSET}"
TIDB_CONFIG="${RUNTIME_DIR}/tidb.toml"
printf 'lease = "2s"\n' >"${TIDB_CONFIG}"
tiup playground "${CLUSTER_VERSION}" --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 1 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  --db.config "${TIDB_CONFIG}" >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!
cluster_ready=false
for _ in $(seq 1 300); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    note "FAIL: playground exited early; see ${PLAYGROUND_LOG}"
    exit 1
  fi
  if curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/members" >/dev/null 2>&1 \
    && "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" -uroot \
      --connect-timeout=2 ${MYSQL_PLUGIN_ARGS[@]+"${MYSQL_PLUGIN_ARGS[@]}"} -Nse 'select 1' >/dev/null 2>&1; then
    cluster_ready=true
    break
  fi
  sleep 1
done
if [[ "${cluster_ready}" != true ]]; then
  note "FAIL: cluster never became ready; see ${PLAYGROUND_LOG}"
  note "last PD probe: $(curl -s --max-time 2 "http://${PD_ADDR}/pd/api/v1/members" 2>&1 | head -c 200)"
  note "last SQL probe: $("${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" -uroot \
    --connect-timeout=2 ${MYSQL_PLUGIN_ARGS[@]+"${MYSQL_PLUGIN_ARGS[@]}"} -Nse 'select 1' 2>&1 | head -c 300)"
  exit 1
fi
note "OK: PD ${PD_ADDR}, Go TiDB 127.0.0.1:${GO_SQL_PORT}"

go_sql() {
  "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" -uroot \
    --connect-timeout=5 ${MYSQL_PLUGIN_ARGS[@]+"${MYSQL_PLUGIN_ARGS[@]}"} "$@" 2>&1
}
rust_sql() {
  "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${RUST_SQL_PORT}" \
    -u"${AUTH_USER}" -p"${AUTH_PASSWORD}" --connect-timeout=5 \
    ${MYSQL_PLUGIN_ARGS[@]+"${MYSQL_PLUGIN_ARGS[@]}"} "$@" 2>&1
}

go_sql -e "DROP DATABASE IF EXISTS ${DB}; CREATE DATABASE ${DB};" >/dev/null
go_sql -D "${DB}" -e "CREATE TABLE lu (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)" >/dev/null

step "Rust node startup (--cluster-session)"
"${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
  --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
  --cluster-session --lease-ms 2000 \
  --auth-file "${AUTH_FILE}" --max-connections 32 \
  >"${RUST_LOG_FILE}" 2>&1 &
RUST_PID=$!
rust_ready=false
for _ in $(seq 1 900); do
  if ! kill -0 "${RUST_PID}" 2>/dev/null; then
    note "FAIL: Rust node exited before readiness"
    tail -60 "${RUST_LOG_FILE}" | tee -a "${CHECK_LOG}"
    exit 1
  fi
  if grep -qE '"event":"(sql_node_ready|cluster_session_node_ready)"' "${RUST_LOG_FILE}"; then
    rust_ready=true
    break
  fi
  sleep 0.1
done
if [[ "${rust_ready}" != true ]]; then
  note "FAIL: Rust node never published readiness"
  tail -60 "${RUST_LOG_FILE}" | tee -a "${CHECK_LOG}"
  exit 1
fi
note "OK: Rust node listening on 127.0.0.1:${RUST_SQL_PORT}"

# One side of a race: `STATEMENTS` blind increments, each its own autocommit
# statement, sent down one connection. Retries are NOT performed -- a retry
# would hide exactly the failure being measured, because a lost update reports
# success. Statements that the server refuses are counted and subtracted from
# the expected total, so a run in which the fix converts silent loss into a
# reported 9007 still has an exact oracle.
run_side() {
  local engine=$1 step_value=$2 predicate=$3 label=$4
  local sql_file="${OUT_DIR}/${label}.sql" out="${OUT_DIR}/${label}.out"
  : >"${sql_file}"
  local i
  for ((i = 0; i < STATEMENTS; i++)); do
    printf 'UPDATE lu SET v = v + %s WHERE %s;\n' "${step_value}" "${predicate}" >>"${sql_file}"
  done
  if [[ "${engine}" == go ]]; then
    go_sql -D "${DB}" --force <"${sql_file}" >"${out}" 2>&1
  else
    rust_sql -D "${DB}" --force <"${sql_file}" >"${out}" 2>&1
  fi
  grep -c '^ERROR' "${out}" || true
}

read_v() {
  local engine=$1
  if [[ "${engine}" == go ]]; then
    go_sql -D "${DB}" -N -B -e 'SELECT v FROM lu WHERE id = 1'
  else
    rust_sql -D "${DB}" -N -B -e 'SELECT v FROM lu WHERE id = 1'
  fi
}

RESULTS=()
FAILED=0

# `engine_a`, `engine_b`: which server each side's connection talks to.
# `predicate`: the WHERE clause both sides use.
# `sides`: 2 for a contended scenario, 1 for the uncontended control.
scenario() {
  local name=$1 engine_a=$2 engine_b=$3 predicate=$4 sides=$5
  step "${name}"
  go_sql -D "${DB}" -e 'DELETE FROM lu; INSERT INTO lu (id, v) VALUES (1, 0)' >/dev/null
  local errors_a errors_b=0 expected
  local out_a="${OUT_DIR}/${name// /_}-a.count"
  if [[ "${sides}" == 2 ]]; then
    ( run_side "${engine_a}" 5 "${predicate}" "${name// /_}-a" >"${out_a}" ) &
    local pid_a=$!
    errors_b=$(run_side "${engine_b}" 7 "${predicate}" "${name// /_}-b")
    wait "${pid_a}"
    errors_a=$(cat "${out_a}")
    expected=$(( (STATEMENTS - errors_a) * 5 + (STATEMENTS - errors_b) * 7 ))
  else
    errors_a=$(run_side "${engine_a}" 5 "${predicate}" "${name// /_}-a")
    expected=$(( (STATEMENTS - errors_a) * 5 ))
  fi
  local actual
  actual=$(read_v go | tr -d '[:space:]')
  local verdict=PASS
  if [[ "${actual}" != "${expected}" ]]; then
    verdict=LOST
    FAILED=$((FAILED + 1))
  fi
  note "${verdict}: ${name} -> ${actual} (expected ${expected}); refused: a=${errors_a} b=${errors_b}"
  RESULTS+=("${verdict}|${name}|${actual}|${expected}|${errors_a}|${errors_b}")
}

scenario "control Go vs Go (method control)"      go   go   'id = 1'  2
scenario "control Rust alone (no-race control)"   rust rust 'id = 1'  1
scenario "Rust vs Go, point predicate"            rust go   'id = 1'  2
scenario "Rust vs Rust, point predicate"          rust rust 'id = 1'  2
scenario "Rust vs Go, ranged predicate"           rust go   'id >= 1' 2

step "task #168: pessimistic lock type on the optimistic path"
note "Go @@tidb_txn_mode: $(go_sql -N -B -e 'SELECT @@tidb_txn_mode' | tr -d '\r')"
note "Go pessimistic-auto-commit: $(go_sql -N -B -e "SELECT \`value\` FROM information_schema.cluster_config WHERE \`key\` = 'pessimistic-txn.pessimistic-auto-commit'" | tr -d '\r' | tr '\n' ' ')"
note "Go tidb_retry_limit / tidb_disable_txn_auto_retry: $(go_sql -N -B -e 'SELECT @@tidb_retry_limit, @@tidb_disable_txn_auto_retry' | tr -d '\r')"
LOCK_TYPE_HITS=$(grep -c 'outside bounded recovery' "${OUT_DIR}"/*.out 2>/dev/null \
  | awk -F: '{ total += $2 } END { print total + 0 }')
note "'outside bounded recovery' occurrences across every side: ${LOCK_TYPE_HITS}"
grep -h 'outside bounded recovery' "${OUT_DIR}"/*.out 2>/dev/null | sort -u \
  | head -5 | tee -a "${CHECK_LOG}"

step "summary"
printf '%-46s %-6s %-8s %-8s\n' "scenario" "result" "actual" "expected" | tee -a "${CHECK_LOG}"
for row in "${RESULTS[@]}"; do
  IFS='|' read -r verdict name actual expected _ _ <<<"${row}"
  printf '%-46s %-6s %-8s %-8s\n' "${name}" "${verdict}" "${actual}" "${expected}" \
    | tee -a "${CHECK_LOG}"
done

if [[ "${FAILED}" -ne 0 ]]; then
  note "${FAILED} scenario(s) LOST updates"
  exit 1
fi
note "every scenario reached its exact total"
exit 0
