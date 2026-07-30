#!/usr/bin/env bash
#
# Measures how far stock `sysbench 1.0.20` gets against the Rust SQL node.
#
# This is a MEASUREMENT harness, not a pass/fail proof: every rung of the
# ladder is attempted and reported even when an earlier rung failed, because
# the precise first failure is the deliverable.
#
# Rungs, in order:
#   0. real cluster (TiUP playground: PD + TiKV + one Go tidb-server)
#   1. Rust node startup in `--cluster-session` mode
#   2. stock MySQL client handshake, auth, `SELECT 1`
#   3. `CREATE DATABASE sbtest` through the Rust node
#   4. `sysbench oltp_read_only ... prepare` (--tables=1 --table-size=1000)
#   5. correctness: COUNT(*) and a checksum-style aggregate
#   6. oltp_point_select / oltp_read_only / oltp_write_only / oltp_read_write,
#      each under `--db-ps-mode=disable` (text) and the default (binary
#      prepared statements), --threads=1 --time=10
#
# Everything the script starts is killed by the EXIT/INT/TERM trap, and the
# trap fails the run if any owned port is still reachable afterwards.

set -uo pipefail

for prerequisite in tiup cargo curl jq nc pgrep awk sed seq grep openssl sysbench; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing sysbench-ladder prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${SYSBENCH_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "SYSBENCH_MYSQL_CLIENT must name an executable stock MySQL client" >&2
  exit 1
fi
# MySQL client 8.0.34+ deprecated and 9.x ships `mysql_native_password` only as
# a loadable client plugin. The Rust node offers that plugin and nothing else,
# so point the client at whichever plugin directory actually has it.
MYSQL_PLUGIN_ARGS=()
if [[ -n "${SYSBENCH_MYSQL_PLUGIN_DIR:-}" ]]; then
  if [[ ! -f "${SYSBENCH_MYSQL_PLUGIN_DIR}/mysql_native_password.so" ]]; then
    echo "SYSBENCH_MYSQL_PLUGIN_DIR does not contain mysql_native_password.so" >&2
    exit 1
  fi
  MYSQL_PLUGIN_ARGS=(--plugin-dir="${SYSBENCH_MYSQL_PLUGIN_DIR}")
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
TAG="sysbench-ladder-${$}-$(date +%s)"
PORT_OFFSET=${SYSBENCH_PORT_OFFSET:-41000}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "SYSBENCH_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
  exit 1
fi
PD_PORT=$((2379 + PORT_OFFSET))
GO_SQL_PORT=$((4000 + PORT_OFFSET))
TIKV_SEED_PORT=$((20160 + PORT_OFFSET))
GO_STATUS_PORT=$((10080 + PORT_OFFSET))
RUST_SQL_PORT=$((12000 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
OUT_DIR=${SYSBENCH_OUT_DIR:-${TMPDIR:-/tmp}/${TAG}}
mkdir -p "${OUT_DIR}"
PLAYGROUND_LOG="${OUT_DIR}/playground.log"
RUST_LOG_FILE="${OUT_DIR}/rust-node.log"
LADDER_LOG="${OUT_DIR}/ladder.log"
PLAYGROUND_PID=
RUST_PID=
RUNTIME_DIR=
AUTH_FILE=
AUTH_USER=sbtest
AUTH_PASSWORD=${SYSBENCH_AUTH_PASSWORD:-sbtest-native-password}
SYSBENCH_DB=sbtest
TABLE_SIZE=${SYSBENCH_TABLE_SIZE:-1000}
RUN_TIME=${SYSBENCH_RUN_TIME:-10}

step() { printf '\n===== %s =====\n' "$*" | tee -a "${LADDER_LOG}"; }
note() { printf '%s\n' "$*" | tee -a "${LADDER_LOG}"; }

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
      echo "sysbench-ladder cleanup failed: 127.0.0.1:${port} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
    [[ -n "${RUNTIME_DIR}" ]] && rm -rf -- "${RUNTIME_DIR}"
  fi
  echo "sysbench-ladder artifacts: ${OUT_DIR}" >&2
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}

RUST_SERVER=${SYSBENCH_RUST_SERVER:-}
if [[ -z "${RUST_SERVER}" ]]; then
  ( cd "${RUST_ROOT}" && CARGO_BUILD_JOBS=12 cargo build -j12 --release \
      -p tidb-server --bin tidb-server ) || exit 1
  RUST_SERVER="${RUST_ROOT}/target/release/tidb-server"
fi
if [[ ! -x "${RUST_SERVER}" ]]; then
  echo "sysbench-ladder Rust server is not executable: ${RUST_SERVER}" >&2
  exit 1
fi

for port in "${PD_PORT}" "${GO_SQL_PORT}" "${TIKV_SEED_PORT}" \
  "${GO_STATUS_PORT}" "${RUST_SQL_PORT}"; do
  if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
    echo "refusing occupied sysbench-ladder port ${port}; set SYSBENCH_PORT_OFFSET" >&2
    exit 1
  fi
done

trap cleanup EXIT INT TERM

RUNTIME_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}-runtime.XXXXXX")
AUTH_FILE="${RUNTIME_DIR}/auth.tsv"
AUTH_HASH_HEX=$(printf '%s' "${AUTH_PASSWORD}" \
  | openssl dgst -sha1 -binary | openssl dgst -sha1 -hex \
  | awk '{ print toupper($NF) }')
if [[ ! "${AUTH_HASH_HEX}" =~ ^[0-9A-F]{40}$ ]]; then
  echo "could not derive the sysbench native-password stage-two hash" >&2
  exit 1
fi
(umask 077; printf '%s\t%s\t%s\t*%s\n' \
  "${AUTH_USER}" "127.0.0.1" "mysql_native_password" "${AUTH_HASH_HEX}" >"${AUTH_FILE}")
chmod 0600 "${AUTH_FILE}"
unset AUTH_HASH_HEX

step "rung 0: real cluster (TiUP playground)"
TIDB_CONFIG="${RUNTIME_DIR}/tidb.toml"
printf 'lease = "2s"\n' >"${TIDB_CONFIG}"
tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 1 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  --db.config "${TIDB_CONFIG}" >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!
cluster_ready=false
for _ in $(seq 1 300); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    note "FAIL rung 0: playground exited early; see ${PLAYGROUND_LOG}"
    exit 1
  fi
  if curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/members" >/dev/null 2>&1 \
    && "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" -uroot \
      --connect-timeout=2 "${MYSQL_PLUGIN_ARGS[@]}" -Nse 'select 1' >/dev/null 2>&1; then
    cluster_ready=true
    break
  fi
  sleep 1
done
if [[ "${cluster_ready}" != true ]]; then
  note "FAIL rung 0: cluster never became ready; see ${PLAYGROUND_LOG}"
  exit 1
fi
note "OK rung 0: PD ${PD_ADDR}, Go TiDB 127.0.0.1:${GO_SQL_PORT}"

step "rung 1: Rust node startup (--cluster-session)"
"${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
  --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
  --cluster-session --lease-ms 2000 \
  --auth-file "${AUTH_FILE}" --max-connections 32 \
  >"${RUST_LOG_FILE}" 2>&1 &
RUST_PID=$!
rust_ready=false
for _ in $(seq 1 900); do
  if ! kill -0 "${RUST_PID}" 2>/dev/null; then
    note "FAIL rung 1: Rust node exited before readiness"
    tail -60 "${RUST_LOG_FILE}" | tee -a "${LADDER_LOG}"
    exit 1
  fi
  # `--cluster-session` publishes its own readiness event name; the configured
  # read-table mode publishes `sql_node_ready`. Either one means listening.
  if grep -qE '"event":"(sql_node_ready|cluster_session_node_ready)"' "${RUST_LOG_FILE}"; then
    rust_ready=true
    break
  fi
  sleep 0.1
done
if [[ "${rust_ready}" != true ]]; then
  note "FAIL rung 1: Rust node never published readiness"
  tail -60 "${RUST_LOG_FILE}" | tee -a "${LADDER_LOG}"
  exit 1
fi
note "OK rung 1: Rust node listening on 127.0.0.1:${RUST_SQL_PORT}"

rust_sql() {
  "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${RUST_SQL_PORT}" \
    -u"${AUTH_USER}" -p"${AUTH_PASSWORD}" --connect-timeout=5 \
    "${MYSQL_PLUGIN_ARGS[@]}" "$@" 2>&1
}

step "rung 2: stock MySQL client handshake, auth, SELECT 1"
if SELECT_ONE=$(rust_sql -N -B -e 'SELECT 1'); then
  note "OK rung 2: SELECT 1 -> ${SELECT_ONE}"
else
  note "FAIL rung 2: ${SELECT_ONE}"
  exit 1
fi

step "rung 3: CREATE DATABASE ${SYSBENCH_DB} through the Rust node"
CREATE_DB_OUT=$(rust_sql -e "CREATE DATABASE IF NOT EXISTS ${SYSBENCH_DB}")
if [[ $? -eq 0 ]]; then
  note "OK rung 3: database created (or already present)"
else
  note "FAIL rung 3: ${CREATE_DB_OUT}"
  note "falling back to the Go server so later rungs still report their own failure"
  "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" -uroot \
    "${MYSQL_PLUGIN_ARGS[@]}" -e "CREATE DATABASE IF NOT EXISTS ${SYSBENCH_DB}" 2>&1 \
    | tee -a "${LADDER_LOG}"
fi

step "rung 3b: initial-handshake capability flags, Rust node vs Go TiDB"
python3 - "${RUST_SQL_PORT}" "${GO_SQL_PORT}" <<'PY' 2>&1 | tee -a "${LADDER_LOG}"
import socket, struct, sys

# CLIENT_SSL is bit 11 of the server's advertised capability flags. sysbench's
# MariaDB connector consults exactly that bit before deciding whether TLS is
# possible, so print it side by side with the Go server's.
def flags(port):
    s = socket.create_connection(("127.0.0.1", int(port)), timeout=5)
    header = s.recv(4)
    length = int.from_bytes(header[:3], "little")
    body = b""
    while len(body) < length:
        body += s.recv(length - len(body))
    s.close()
    end = body.index(b"\0", 1)
    rest = body[end + 1 + 4 + 8 + 1 :]
    lower = struct.unpack("<H", rest[:2])[0]
    upper = struct.unpack("<H", rest[2 + 1 + 2 + 2 :][:2])[0]
    return (upper << 16) | lower

for label, port in (("rust", sys.argv[1]), ("go", sys.argv[2])):
    try:
        value = flags(port)
        print(f"{label}: capabilities=0x{value:08x} CLIENT_SSL={'yes' if value & (1 << 11) else 'no'}")
    except Exception as error:  # noqa: BLE001 - diagnostic only
        print(f"{label}: capability probe failed: {error}")
PY

step "rung 3c: control - sysbench prepare against the Go TiDB on the same cluster"
"${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" -uroot \
  "${MYSQL_PLUGIN_ARGS[@]}" -e "CREATE DATABASE IF NOT EXISTS sbtest_go" >/dev/null 2>&1
CONTROL_LOG="${OUT_DIR}/control-go-prepare.log"
if sysbench oltp_read_only --db-driver=mysql --mysql-host=127.0.0.1 \
  --mysql-port="${GO_SQL_PORT}" --mysql-user=root --mysql-db=sbtest_go \
  --mysql-ssl=off --tables=1 --table-size=100 prepare \
  >"${CONTROL_LOG}" 2>&1; then
  note "OK rung 3c: sysbench prepares fine against Go TiDB, so the client is not the problem"
else
  note "NOTE rung 3c: sysbench also fails against the Go TiDB - the cause is client-side"
  tail -8 "${CONTROL_LOG}" | tee -a "${LADDER_LOG}"
fi

SYSBENCH_CONN=(
  --db-driver=mysql
  --mysql-ssl=off
  --mysql-host=127.0.0.1
  --mysql-port="${RUST_SQL_PORT}"
  --mysql-user="${AUTH_USER}"
  --mysql-password="${AUTH_PASSWORD}"
  --mysql-db="${SYSBENCH_DB}"
  --tables=1
  --table-size="${TABLE_SIZE}"
)

# sysbench's default `id INTEGER NOT NULL AUTO_INCREMENT` is a real
# possibility for a server and a real gap for this node, so both are attempted
# and reported: the default first, then sysbench's own `--auto-inc=off`, which
# declares `id INTEGER NOT NULL` and supplies every id explicitly.
AUTO_INC_ARGS=()
prepared=false
for auto_inc in on off; do
  step "rung 4 (--auto-inc=${auto_inc}): sysbench oltp_read_only prepare (--tables=1 --table-size=${TABLE_SIZE})"
  PREPARE_LOG="${OUT_DIR}/prepare-auto-inc-${auto_inc}.log"
  rust_sql -e "DROP TABLE IF EXISTS ${SYSBENCH_DB}.sbtest1" >/dev/null 2>&1
  if sysbench oltp_read_only "${SYSBENCH_CONN[@]}" --auto-inc="${auto_inc}" \
    prepare >"${PREPARE_LOG}" 2>&1; then
    note "OK rung 4 (--auto-inc=${auto_inc}): prepare succeeded"
    AUTO_INC_ARGS=(--auto-inc="${auto_inc}")
    prepared=true
    break
  fi
  note "FAIL rung 4 (--auto-inc=${auto_inc}): prepare failed; tail below"
  tail -20 "${PREPARE_LOG}" | tee -a "${LADDER_LOG}"
done
SYSBENCH_CONN+=("${AUTO_INC_ARGS[@]+"${AUTO_INC_ARGS[@]}"}")

step "rung 5: correctness of the prepared dataset"
if [[ "${prepared}" == true ]]; then
  note "COUNT(*): $(rust_sql -N -B -e "SELECT COUNT(*) FROM ${SYSBENCH_DB}.sbtest1")"
  note "checksum: $(rust_sql -N -B -e \
    "SELECT COUNT(*), SUM(id), SUM(k), MIN(id), MAX(id) FROM ${SYSBENCH_DB}.sbtest1")"
  note "go-side:  $("${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
    -uroot "${MYSQL_PLUGIN_ARGS[@]}" -N -B -e \
    "SELECT COUNT(*), SUM(id), SUM(k), MIN(id), MAX(id) FROM ${SYSBENCH_DB}.sbtest1" 2>&1)"
else
  note "SKIP rung 5: no dataset to check"
fi

step "rung 6: workload ladder (--threads=1 --time=${RUN_TIME})"
for workload in oltp_point_select oltp_read_only oltp_write_only oltp_read_write; do
  for ps_mode in disable auto; do
    label="${workload}-ps-${ps_mode}"
    log="${OUT_DIR}/${label}.log"
    note "--- ${label} ---"
    if sysbench "${workload}" "${SYSBENCH_CONN[@]}" \
      --db-ps-mode="${ps_mode}" --threads=1 --time="${RUN_TIME}" \
      --report-interval=0 run >"${log}" 2>&1; then
      grep -E "queries:|transactions:|avg:|95th percentile:|total time:" "${log}" \
        | tee -a "${LADDER_LOG}"
    else
      note "FAIL ${label}:"
      grep -m 5 -E "FATAL|ERROR" "${log}" | tee -a "${LADDER_LOG}"
    fi
  done
done

step "post-run correctness re-check"
if [[ "${prepared}" == true ]]; then
  note "rust: $(rust_sql -N -B -e \
    "SELECT COUNT(*), SUM(id), SUM(k) FROM ${SYSBENCH_DB}.sbtest1")"
  note "go:   $("${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
    -uroot "${MYSQL_PLUGIN_ARGS[@]}" -N -B -e \
    "SELECT COUNT(*), SUM(id), SUM(k) FROM ${SYSBENCH_DB}.sbtest1" 2>&1)"
fi

note "sysbench ladder finished; artifacts under ${OUT_DIR}"
