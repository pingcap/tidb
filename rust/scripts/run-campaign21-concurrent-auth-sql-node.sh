#!/usr/bin/env bash

set -euo pipefail

for prerequisite in tiup cargo curl jq nc pgrep ps awk sed seq grep sort tail mktemp mkfifo openssl chmod date kill; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing Campaign 21 prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${C21_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "C21_MYSQL_CLIENT must name an executable stock MySQL or MariaDB client" >&2
  exit 1
fi
MYSQL_PLUGIN_ARGS=()
if [[ -n "${C21_MYSQL_PLUGIN_DIR:-}" ]]; then
  if [[ ! -f "${C21_MYSQL_PLUGIN_DIR}/mysql_native_password.so" ]]; then
    echo "C21_MYSQL_PLUGIN_DIR does not contain mysql_native_password.so" >&2
    exit 1
  fi
  MYSQL_PLUGIN_ARGS=(--plugin-dir="${C21_MYSQL_PLUGIN_DIR}")
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
TAG="campaign21-concurrent-auth-sql-node-${$}-$(date +%s)"
PORT_OFFSET=${C21_PORT_OFFSET:-41000}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 44375 ]]; then
  echo "C21_PORT_OFFSET must be an unsigned integer no greater than 44375" >&2
  exit 1
fi
PD_PORT=$((2379 + PORT_OFFSET))
GO_SQL_PORT=$((4000 + PORT_OFFSET))
TIKV_SEED_PORT=$((20160 + PORT_OFFSET))
GO_STATUS_PORT=$((10080 + PORT_OFFSET))
RUST_SQL_PORT=$((12000 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
GO_SQL_ADDR="127.0.0.1:${GO_SQL_PORT}"
RUST_SQL_ADDR="127.0.0.1:${RUST_SQL_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
MYSQL_LOG="${TMPDIR:-/tmp}/${TAG}-mysql.log"
RUNTIME_DIR=
AUTH_FILE=
AUTH_USER=campaign21
AUTH_PASSWORD=${C21_AUTH_PASSWORD:-campaign21-native-password}
PLAYGROUND_PID=
RUST_PID=
OWNED_PIDS=
STORE_ADDRESSES=
CLIENT_PIDS=()
CLIENT_LOGS_ARCHIVED=false
CLIENT_COMPLETION_TIMEOUT=${C21_CLIENT_COMPLETION_TIMEOUT:-60}
PROCESS_STOP_TIMEOUT=${C21_PROCESS_STOP_TIMEOUT:-15}

for timeout_name in CLIENT_COMPLETION_TIMEOUT PROCESS_STOP_TIMEOUT; do
  timeout_value=${!timeout_name}
  if [[ ! "${timeout_value}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${timeout_name} must be a positive integer number of seconds" >&2
    exit 1
  fi
done

tag_status_rows() {
  tiup status | awk -v tag="${TAG}" \
    'NR > 2 && ($1 == tag || index($0, "/data/" tag "/")) { print }'
}

tag_owned_pids() {
  pgrep -f "${TAG_DIR}" || true
}

collect_descendant_pids() {
  local frontier=$1
  local descendants=
  local child
  local next
  local parent
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

merge_owned_pids() {
  {
    for pid in ${OWNED_PIDS}; do
      printf '%s\n' "${pid}"
    done
    if [[ -n "${PLAYGROUND_PID}" ]]; then
      collect_descendant_pids "${PLAYGROUND_PID}"
    fi
    tag_owned_pids
  } | awk 'NF && !seen[$1]++ { print $1 }' | tr '\n' ' '
}

close_hold_fds() {
  exec 10>&-
  exec 11>&-
  exec 12>&-
  exec 13>&-
  exec 14>&-
  exec 15>&-
  exec 16>&-
  exec 17>&-
  exec 18>&-
}

open_hold_fd() {
  local index=$1
  local fifo=$2
  case "${index}" in
    0) exec 10<>"${fifo}" ;;
    1) exec 11<>"${fifo}" ;;
    2) exec 12<>"${fifo}" ;;
    3) exec 13<>"${fifo}" ;;
    4) exec 14<>"${fifo}" ;;
    5) exec 15<>"${fifo}" ;;
    6) exec 16<>"${fifo}" ;;
    7) exec 17<>"${fifo}" ;;
    8) exec 18<>"${fifo}" ;;
    *) echo "invalid Campaign 21 client index ${index}" >&2; return 1 ;;
  esac
}

release_client_query() {
  local index=$1
  local query='SELECT balance AS amount, id FROM campaign20.rows;'
  case "${index}" in
    0) printf '%s\nquit\n' "${query}" >&10; exec 10>&- ;;
    1) printf '%s\nquit\n' "${query}" >&11; exec 11>&- ;;
    2) printf '%s\nquit\n' "${query}" >&12; exec 12>&- ;;
    3) printf '%s\nquit\n' "${query}" >&13; exec 13>&- ;;
    4) printf '%s\nquit\n' "${query}" >&14; exec 14>&- ;;
    5) printf '%s\nquit\n' "${query}" >&15; exec 15>&- ;;
    6) printf '%s\nquit\n' "${query}" >&16; exec 16>&- ;;
    7) printf '%s\nquit\n' "${query}" >&17; exec 17>&- ;;
    *) echo "invalid successful Campaign 21 client index ${index}" >&2; return 1 ;;
  esac
}

close_early_client_fd() {
  exec 18>&-
}

pid_is_running() {
  local pid=$1
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 1
  fi
  local state
  state=$(ps -o stat= -p "${pid}" 2>/dev/null | awk 'NR == 1 { print $1 }')
  [[ -n "${state}" && "${state}" != Z* ]]
}

wait_for_pids_until() {
  local deadline=$1
  shift
  while true; do
    local running=false
    local pid
    for pid in "$@"; do
      if [[ -n "${pid}" ]] && pid_is_running "${pid}"; then
        running=true
        break
      fi
    done
    if [[ "${running}" == false ]]; then
      return 0
    fi
    if [[ $(date +%s) -ge "${deadline}" ]]; then
      return 1
    fi
    sleep 0.1
  done
}

terminate_pid_group() {
  local label=$1
  shift
  local pid
  local running_pids=()
  for pid in "$@"; do
    if [[ -n "${pid}" ]] && pid_is_running "${pid}"; then
      running_pids+=("${pid}")
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done
  if [[ ${#running_pids[@]} -eq 0 ]]; then
    for pid in "$@"; do
      if [[ -n "${pid}" ]]; then
        wait "${pid}" 2>/dev/null || true
      fi
    done
    return 0
  fi

  local deadline=$(( $(date +%s) + PROCESS_STOP_TIMEOUT ))
  local forced=false
  if ! wait_for_pids_until "${deadline}" "${running_pids[@]}"; then
    forced=true
    for pid in "${running_pids[@]}"; do
      if pid_is_running "${pid}"; then
        kill -KILL "${pid}" 2>/dev/null || true
      fi
    done
    deadline=$(( $(date +%s) + PROCESS_STOP_TIMEOUT ))
    if ! wait_for_pids_until "${deadline}" "${running_pids[@]}"; then
      echo "Campaign 21 cleanup failed: ${label} remained alive after SIGKILL" >&2
      return 1
    fi
  fi
  for pid in "$@"; do
    if [[ -n "${pid}" ]]; then
      wait "${pid}" 2>/dev/null || true
    fi
  done
  if [[ "${forced}" == true ]]; then
    echo "Campaign 21 cleanup failed: ${label} required SIGKILL after ${PROCESS_STOP_TIMEOUT}s" >&2
    return 1
  fi
  return 0
}

archive_client_logs() {
  if [[ "${CLIENT_LOGS_ARCHIVED}" == true ]]; then
    return
  fi
  CLIENT_LOGS_ARCHIVED=true
  if [[ -z "${RUNTIME_DIR}" ]] || [[ ! -d "${RUNTIME_DIR}" ]]; then
    return
  fi
  local index
  for index in $(seq 0 8); do
    if [[ -f "${RUNTIME_DIR}/client-${index}.out" ]]; then
      printf 'client_%s_output_begin\n' "${index}" >>"${MYSQL_LOG}"
      sed -n '1,200p' "${RUNTIME_DIR}/client-${index}.out" >>"${MYSQL_LOG}"
      printf 'client_%s_output_end\n' "${index}" >>"${MYSQL_LOG}"
    fi
    if [[ -s "${RUNTIME_DIR}/client-${index}.err" ]]; then
      printf 'client_%s_stderr_begin\n' "${index}" >>"${MYSQL_LOG}"
      sed -n '1,200p' "${RUNTIME_DIR}/client-${index}.err" >>"${MYSQL_LOG}"
      printf 'client_%s_stderr_end\n' "${index}" >>"${MYSQL_LOG}"
    fi
  done
}

cleanup() {
  local original_status=$?
  local cleanup_failed=false
  trap - EXIT INT TERM

  close_hold_fds
  if [[ ${#CLIENT_PIDS[@]} -gt 0 ]] \
    && ! terminate_pid_group "stock MySQL client process" "${CLIENT_PIDS[@]}"; then
    cleanup_failed=true
  fi
  archive_client_logs

  if [[ -n "${RUST_PID}" ]] \
    && ! terminate_pid_group "Rust SQL node ${RUST_PID}" "${RUST_PID}"; then
    cleanup_failed=true
  fi
  if nc -z -w 1 127.0.0.1 "${RUST_SQL_PORT}" >/dev/null 2>&1; then
    echo "Campaign 21 cleanup failed: Rust SQL node ${RUST_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  OWNED_PIDS=$(merge_owned_pids)
  if [[ -n "${PLAYGROUND_PID}" ]] \
    && ! terminate_pid_group "TiUP playground ${PLAYGROUND_PID}" "${PLAYGROUND_PID}"; then
    cleanup_failed=true
  fi
  local registered_rows
  registered_rows=$(tag_status_rows 2>/dev/null || true)
  if [[ -n "${registered_rows}" ]] || [[ -d "${TAG_DIR}" ]]; then
    if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
      echo "Campaign 21 cleanup failed: tiup clean failed for ${TAG}" >&2
      cleanup_failed=true
    fi
  fi

  local processes_cleaned=false
  for _ in $(seq 1 30); do
    OWNED_PIDS=$(merge_owned_pids)
    local alive=false
    local pid
    for pid in ${OWNED_PIDS}; do
      if kill -0 "${pid}" 2>/dev/null; then
        alive=true
        break
      fi
    done
    local rows
    rows=$(tag_status_rows 2>/dev/null || true)
    if [[ "${alive}" == false ]] && [[ -z "${rows}" ]]; then
      processes_cleaned=true
      break
    fi
    sleep 1
  done
  if [[ "${processes_cleaned}" != true ]]; then
    echo "Campaign 21 cleanup failed: owned process or TiUP registry row remains" >&2
    cleanup_failed=true
  fi

  local address
  for address in ${STORE_ADDRESSES}; do
    local port=${address##*:}
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "Campaign 21 cleanup failed: TiKV ${address} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if nc -z -w 1 127.0.0.1 "${TIKV_SEED_PORT}" >/dev/null 2>&1; then
    echo "Campaign 21 cleanup failed: TiKV seed 127.0.0.1:${TIKV_SEED_PORT} remains reachable" >&2
    cleanup_failed=true
  fi
  if nc -z -w 1 127.0.0.1 "${GO_SQL_PORT}" >/dev/null 2>&1; then
    echo "Campaign 21 cleanup failed: Go TiDB ${GO_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi
  if nc -z -w 1 127.0.0.1 "${GO_STATUS_PORT}" >/dev/null 2>&1; then
    echo "Campaign 21 cleanup failed: Go TiDB status port remains reachable" >&2
    cleanup_failed=true
  fi
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "Campaign 21 cleanup failed: PD ${PD_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
    if [[ -e "${TAG_DIR}" ]]; then
      echo "Campaign 21 cleanup failed: owned data directory remains" >&2
      cleanup_failed=true
    fi
  fi

  if [[ -n "${RUNTIME_DIR}" ]]; then
    rm -rf -- "${RUNTIME_DIR}"
    if [[ -e "${AUTH_FILE}" ]] || [[ -e "${RUNTIME_DIR}" ]]; then
      echo "Campaign 21 cleanup failed: authentication runtime files remain" >&2
      cleanup_failed=true
    fi
  fi

  if [[ "${cleanup_failed}" == false ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG}" "${MYSQL_LOG}"
    echo "Campaign 21 cleanup proof passed: tag processes stopped, data removed, auth_file_removed=true"
  else
    echo "Campaign 21 retained logs: ${PLAYGROUND_LOG} ${RUST_LOG} ${MYSQL_LOG}" >&2
  fi
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}

cd "${RUST_ROOT}"
if [[ -z "${C21_RUST_SERVER:-}" ]]; then
  CARGO_BUILD_JOBS=12 cargo build -j12 -p tidb-server --bin tidb-server
  RUST_SERVER="${RUST_ROOT}/target/debug/tidb-server"
else
  RUST_SERVER=${C21_RUST_SERVER}
fi
if [[ ! -x "${RUST_SERVER}" ]]; then
  echo "Campaign 21 Rust server is not executable: ${RUST_SERVER}" >&2
  exit 1
fi

for port in "${PD_PORT}" "${GO_SQL_PORT}" "${TIKV_SEED_PORT}" \
  "${GO_STATUS_PORT}" "${RUST_SQL_PORT}"; do
  if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
    echo "refusing occupied Campaign 21 port ${port}; set C21_PORT_OFFSET" >&2
    exit 1
  fi
done

trap cleanup EXIT INT TERM

RUNTIME_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}-runtime.XXXXXX")
AUTH_FILE="${RUNTIME_DIR}/auth.tsv"
AUTH_HASH_HEX=$(printf '%s' "${AUTH_PASSWORD}" \
  | openssl dgst -sha1 -binary \
  | openssl dgst -sha1 -hex \
  | awk '{ print toupper($NF) }')
if [[ ! "${AUTH_HASH_HEX}" =~ ^[0-9A-F]{40}$ ]]; then
  echo "could not derive the Campaign 21 native-password stage-two hash" >&2
  exit 1
fi
(umask 077; printf '%s\t%s\t%s\t*%s\n' \
  "${AUTH_USER}" "127.0.0.1" "mysql_native_password" "${AUTH_HASH_HEX}" >"${AUTH_FILE}")
chmod 0600 "${AUTH_FILE}"
unset AUTH_HASH_HEX

tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 1 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

ready=false
PD_MEMBERS_JSON=
for _ in $(seq 1 240); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    echo "TiUP playground exited before readiness" >&2
    tail -160 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  PD_MEMBERS_JSON=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/members" 2>/dev/null) || true
  STORE_ADDRESSES=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r '.stores[] | select(.store.state_name == "Up" and ((.store.node_state_name // "Serving") == "Serving")) | .store.address' \
      2>/dev/null) || true
  if [[ -n "${PD_MEMBERS_JSON}" ]] && [[ -n "${STORE_ADDRESSES}" ]] \
    && "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
      -uroot --connect-timeout=2 "${MYSQL_PLUGIN_ARGS[@]}" -Nse 'select 1' \
      >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  echo "Go TiDB, PD, and TiKV did not become ready" >&2
  tail -160 "${PLAYGROUND_LOG}" >&2
  exit 1
fi
OWNED_PIDS=$(merge_owned_pids)
if [[ -z "${OWNED_PIDS}" ]]; then
  echo "TiUP did not publish tag-owned processes for ${TAG}" >&2
  exit 1
fi

PD_CLUSTER_ID=$(printf '%s\n' "${PD_MEMBERS_JSON}" \
  | jq -r '.header.cluster_id // .cluster_id // .id // empty')
if [[ ! "${PD_CLUSTER_ID}" =~ ^[0-9]+$ ]] || [[ "${PD_CLUSTER_ID}" =~ ^0+$ ]]; then
  echo "PD membership response omitted a nonzero cluster identity" >&2
  printf '%s\n' "${PD_MEMBERS_JSON}" >&2
  exit 1
fi

"${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
  -uroot --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" <<'SQL'
DROP DATABASE IF EXISTS campaign20;
CREATE DATABASE campaign20;
CREATE TABLE campaign20.rows (
  id BIGINT PRIMARY KEY CLUSTERED,
  balance BIGINT NOT NULL
);
INSERT INTO campaign20.rows VALUES (-7, 913), (0, -2048), (42, 77);
SQL

TABLE_ID=$("${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
  -uroot --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" -Nse \
  "select tidb_table_id from information_schema.tables where table_schema='campaign20' and table_name='rows'")
if [[ ! "${TABLE_ID}" =~ ^[0-9]+$ ]] || [[ "${TABLE_ID}" =~ ^0+$ ]]; then
  echo "Go TiDB did not resolve the Campaign 21 physical table ID" >&2
  exit 1
fi

"${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
  --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
  --database campaign20 --table rows --table-id "${TABLE_ID}" \
  --column id:1:clustered-pk \
  --column balance:2:stored-not-null \
  --auth-file "${AUTH_FILE}" --max-connections 8 >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

READY_JSON=
for _ in $(seq 1 600); do
  if ! kill -0 "${RUST_PID}" 2>/dev/null; then
    echo "Rust SQL node exited before readiness" >&2
    tail -200 "${RUST_LOG}" >&2
    exit 1
  fi
  READY_JSON=$(grep -F '"event":"sql_node_ready"' "${RUST_LOG}" | tail -1 || true)
  if [[ -n "${READY_JSON}" ]]; then
    break
  fi
  sleep 0.1
done
if [[ -z "${READY_JSON}" ]]; then
  echo "Rust SQL node did not publish readiness" >&2
  tail -200 "${RUST_LOG}" >&2
  exit 1
fi
if ! printf '%s\n' "${READY_JSON}" | jq -e \
  --arg table_id "${TABLE_ID}" --arg cluster_id "${PD_CLUSTER_ID}" \
  '(.table_id | tostring) == $table_id and (.cluster_id | tostring) == $cluster_id
   and .database == "campaign20" and .table == "rows"
   and .column_count == 2
   and .columns == ["id:1:clustered-pk", "balance:2:stored-not-null"]
   and .max_connections == 8 and .account_count == 1
   and (.authority_id | type) == "number" and .authority_id > 0
   and (.read_authority_id | type) == "number" and .read_authority_id > 0' \
  >/dev/null; then
  echo "Rust readiness did not retain the Campaign 21 auth/concurrency/read authorities" >&2
  printf '%s\n' "${READY_JSON}" >&2
  exit 1
fi
AUTHORITY_ID=$(printf '%s\n' "${READY_JSON}" | jq -r '.authority_id')
READ_AUTHORITY_ID=$(printf '%s\n' "${READY_JSON}" | jq -r '.read_authority_id')

for index in $(seq 0 7); do
  FIFO="${RUNTIME_DIR}/client-${index}.fifo"
  mkfifo "${FIFO}"
  open_hold_fd "${index}" "${FIFO}"
done

for index in $(seq 0 7); do
  FIFO="${RUNTIME_DIR}/client-${index}.fifo"
  (
    close_hold_fds
    export MYSQL_PWD="${AUTH_PASSWORD}"
    export MARIADB_PWD="${AUTH_PASSWORD}"
    exec "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${RUST_SQL_PORT}" \
      -u"${AUTH_USER}" --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" -B \
      <"${FIFO}" >"${RUNTIME_DIR}/client-${index}.out" \
      2>"${RUNTIME_DIR}/client-${index}.err"
  ) &
  CLIENT_PIDS[${index}]=$!
done

ACTIVE_EIGHT=false
for _ in $(seq 1 300); do
  ACTIVE_COUNT=$(grep -F '"event":"connection_begin"' "${RUST_LOG}" \
    | tail -1 | jq -r '.active // 0' 2>/dev/null || true)
  ALL_CLIENTS_ALIVE=true
  for client_pid in "${CLIENT_PIDS[@]}"; do
    if ! kill -0 "${client_pid}" 2>/dev/null; then
      ALL_CLIENTS_ALIVE=false
      break
    fi
  done
  if [[ "${ACTIVE_COUNT}" == 8 ]] && [[ "${ALL_CLIENTS_ALIVE}" == true ]]; then
    ACTIVE_EIGHT=true
    break
  fi
  sleep 0.1
done
if [[ "${ACTIVE_EIGHT}" != true ]]; then
  echo "Campaign 21 clients did not remain concurrently authenticated at active=8" >&2
  tail -240 "${RUST_LOG}" >&2
  exit 1
fi

for index in $(seq 0 7); do
  release_client_query "${index}"
done

CLIENT_DEADLINE=$(( $(date +%s) + CLIENT_COMPLETION_TIMEOUT ))
if ! wait_for_pids_until "${CLIENT_DEADLINE}" "${CLIENT_PIDS[@]}"; then
  echo "Campaign 21 eight-client query phase exceeded ${CLIENT_COMPLETION_TIMEOUT}s" >&2
  tail -260 "${RUST_LOG}" >&2
  exit 1
fi
for index in $(seq 0 7); do
  if ! wait "${CLIENT_PIDS[${index}]}"; then
    CLIENT_PIDS[${index}]=
    echo "Campaign 21 successful client ${index} exited unsuccessfully" >&2
    sed -n '1,200p' "${RUNTIME_DIR}/client-${index}.err" >&2
    exit 1
  fi
  CLIENT_PIDS[${index}]=
done

for index in $(seq 0 7); do
  QUERY_OUTPUT=$(sed -n '1,20p' "${RUNTIME_DIR}/client-${index}.out")
  QUERY_HEADER=$(printf '%s\n' "${QUERY_OUTPUT}" | sed -n '1p')
  if [[ "${QUERY_HEADER}" != $'amount\tid' ]]; then
    echo "Campaign 21 client ${index} did not preserve the requested header order" >&2
    exit 1
  fi
  NORMALIZED_ROWS=$(printf '%s\n' "${QUERY_OUTPUT}" | tail -n +2 \
    | sed '/^[[:space:]]*$/d' | sort -t $'\t' -k2,2n)
  if [[ "${NORMALIZED_ROWS}" != $'913\t-7\n-2048\t0\n77\t42' ]]; then
    echo "Campaign 21 client ${index} did not return the exact real-TiKV pairs" >&2
    printf 'actual:\n%s\n' "${NORMALIZED_ROWS}" >&2
    exit 1
  fi
done

EVIDENCE_READY=false
for _ in $(seq 1 300); do
  SNAPSHOT_COUNT=$(grep -c -F '"event":"query_snapshot"' "${RUST_LOG}" || true)
  TRANSPORT_COUNT=$(grep -c -F '"event":"query_transport"' "${RUST_LOG}" || true)
  QUERY_BEGIN_COUNT=$(grep -F '"event":"query_activity"' "${RUST_LOG}" \
    | grep -c -F '"phase":"begin"' || true)
  QUERY_END_COUNT=$(grep -F '"event":"query_activity"' "${RUST_LOG}" \
    | grep -c -F '"phase":"end"' || true)
  FINAL_CONNECTION_JSON=$(grep -F '"event":"connection_closed"' "${RUST_LOG}" | tail -1 || true)
  if [[ "${SNAPSHOT_COUNT}" -ge 8 ]] && [[ "${TRANSPORT_COUNT}" -ge 8 ]] \
    && [[ "${QUERY_BEGIN_COUNT}" -ge 8 ]] && [[ "${QUERY_END_COUNT}" -ge 8 ]] \
    && [[ -n "${FINAL_CONNECTION_JSON}" ]] \
    && printf '%s\n' "${FINAL_CONNECTION_JSON}" \
      | jq -e '.active == 0 and .accepted == 8 and .completed == 8' >/dev/null 2>&1; then
    EVIDENCE_READY=true
    break
  fi
  sleep 0.1
done
if [[ "${EVIDENCE_READY}" != true ]]; then
  echo "Campaign 21 query/transport/connection evidence did not converge" >&2
  tail -260 "${RUST_LOG}" >&2
  exit 1
fi

SNAPSHOTS_JSON=$(grep -F '"event":"query_snapshot"' "${RUST_LOG}" | jq -s '.')
TRANSPORTS_JSON=$(grep -F '"event":"query_transport"' "${RUST_LOG}" | jq -s '.')
QUERY_ACTIVITY_JSON=$(grep -F '"event":"query_activity"' "${RUST_LOG}" | jq -s '.')
if ! printf '%s\n' "${SNAPSHOTS_JSON}" | jq -e \
  --arg table_id "${TABLE_ID}" --arg cluster_id "${PD_CLUSTER_ID}" \
  --arg authority_id "${AUTHORITY_ID}" --arg user "${AUTH_USER}" \
  'length == 8
   and all(.[]; (.snapshot_ts | type) == "number" and .snapshot_ts > 0
     and (.table_id | tostring) == $table_id
     and (.cluster_id | tostring) == $cluster_id
     and (.authority_id | tostring) == $authority_id
     and .user == $user and .host == "127.0.0.1")
   and ([.[].connection_id] | unique | length) == 8
   and ([.[].session_id] | unique | length) == 8' >/dev/null; then
  echo "Campaign 21 snapshots did not share one nonzero cluster/authority with distinct sessions" >&2
  printf '%s\n' "${SNAPSHOTS_JSON}" >&2
  exit 1
fi
if ! printf '%s\n' "${TRANSPORTS_JSON}" | jq -e \
  --arg authority_id "${AUTHORITY_ID}" \
  'length == 8
   and all(.[]; (.authority_id | tostring) == $authority_id
     and (.located_region_ids | type) == "array" and (.located_region_ids | length) > 0
     and (.dispatched_region_ids | type) == "array" and (.dispatched_region_ids | length) > 0
     and .batch_attempts >= 1 and .unary_attempts == 0)
   and ([.[].connection_id] | unique | length) == 8
   and ([.[].session_id] | unique | length) == 8' >/dev/null; then
  echo "Campaign 21 queries did not each prove BatchCommands-only real-TiKV dispatch" >&2
  printf '%s\n' "${TRANSPORTS_JSON}" >&2
  exit 1
fi
SNAPSHOT_CONNECTION_IDS=$(printf '%s\n' "${SNAPSHOTS_JSON}" \
  | jq -r '.[].connection_id' | sort -n | tr '\n' ',')
TRANSPORT_CONNECTION_IDS=$(printf '%s\n' "${TRANSPORTS_JSON}" \
  | jq -r '.[].connection_id' | sort -n | tr '\n' ',')
if [[ "${SNAPSHOT_CONNECTION_IDS}" != "${TRANSPORT_CONNECTION_IDS}" ]]; then
  echo "Campaign 21 snapshot and transport evidence came from different connections" >&2
  exit 1
fi
if ! printf '%s\n' "${QUERY_ACTIVITY_JSON}" | jq -e \
  '([.[] | select(.phase == "begin")]) as $begins
   | ([.[] | select(.phase == "end")]) as $ends
   | length == 16
     and ($begins | length) == 8 and ($ends | length) == 8
     and ([$begins[].connection_id] | unique | length) == 8
     and ([$ends[].connection_id] | unique | length) == 8
     and (([$begins[].connection_id] | sort) == ([$ends[].connection_id] | sort))
     and (([$begins[].max_active] | max) >= 2)
     and ($ends[-1].active == 0)' >/dev/null; then
  echo "Campaign 21 did not prove overlapping queries with balanced begin/end activity" >&2
  printf '%s\n' "${QUERY_ACTIVITY_JSON}" >&2
  exit 1
fi
QUERY_ACTIVITY_CONNECTION_IDS=$(printf '%s\n' "${QUERY_ACTIVITY_JSON}" \
  | jq -r '.[] | select(.phase == "begin") | .connection_id' \
  | sort -n | tr '\n' ',')
if [[ "${SNAPSHOT_CONNECTION_IDS}" != "${QUERY_ACTIVITY_CONNECTION_IDS}" ]]; then
  echo "Campaign 21 query activity and real-TiKV evidence came from different connections" >&2
  exit 1
fi
MAX_QUERY_ACTIVE=$(printf '%s\n' "${QUERY_ACTIVITY_JSON}" \
  | jq -r '[.[] | select(.phase == "begin") | .max_active] | max')

MAX_ACTIVE=$(grep -F '"event":"connection_begin"' "${RUST_LOG}" \
  | jq -r '.active' | sort -n | tail -1)
if [[ "${MAX_ACTIVE}" != 8 ]]; then
  echo "Campaign 21 did not prove eight simultaneous connections that subsequently authenticated" >&2
  exit 1
fi
if ! printf '%s\n' "${FINAL_CONNECTION_JSON}" | jq -e \
  '.active == 0 and .accepted == .completed and .accepted == 8 and .failed == 0' \
  >/dev/null; then
  echo "Campaign 21 connection accounting did not close exactly once" >&2
  printf '%s\n' "${FINAL_CONNECTION_JSON}" >&2
  exit 1
fi

EARLY_INDEX=8
EARLY_FIFO="${RUNTIME_DIR}/client-${EARLY_INDEX}.fifo"
mkfifo "${EARLY_FIFO}"
open_hold_fd "${EARLY_INDEX}" "${EARLY_FIFO}"
(
  close_hold_fds
  export MYSQL_PWD="${AUTH_PASSWORD}"
  export MARIADB_PWD="${AUTH_PASSWORD}"
  exec "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${RUST_SQL_PORT}" \
    -u"${AUTH_USER}" --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" -B \
    <"${EARLY_FIFO}" >"${RUNTIME_DIR}/client-${EARLY_INDEX}.out" \
    2>"${RUNTIME_DIR}/client-${EARLY_INDEX}.err"
) &
CLIENT_PIDS[${EARLY_INDEX}]=$!
unset AUTH_PASSWORD

NINTH_ADMITTED=false
NINTH_ADMISSION_DEADLINE=$(( $(date +%s) + CLIENT_COMPLETION_TIMEOUT ))
while [[ $(date +%s) -lt "${NINTH_ADMISSION_DEADLINE}" ]]; do
  LAST_BEGIN=$(grep -F '"event":"connection_begin"' "${RUST_LOG}" | tail -1 || true)
  if [[ -n "${LAST_BEGIN}" ]] \
    && pid_is_running "${CLIENT_PIDS[${EARLY_INDEX}]}" \
    && printf '%s\n' "${LAST_BEGIN}" \
      | jq -e '.active == 1 and .accepted == 9' >/dev/null 2>&1; then
    NINTH_ADMITTED=true
    break
  fi
  sleep 0.1
done
if [[ "${NINTH_ADMITTED}" != true ]]; then
  echo "Campaign 21 ninth lifecycle client was not admitted after the eight-query proof" >&2
  tail -260 "${RUST_LOG}" >&2
  exit 1
fi

EARLY_PID=${CLIENT_PIDS[${EARLY_INDEX}]}
kill -TERM "${EARLY_PID}" 2>/dev/null || true
close_early_client_fd
EARLY_DEADLINE=$(( $(date +%s) + CLIENT_COMPLETION_TIMEOUT ))
if ! wait_for_pids_until "${EARLY_DEADLINE}" "${EARLY_PID}"; then
  echo "Campaign 21 ninth lifecycle client did not terminate within ${CLIENT_COMPLETION_TIMEOUT}s" >&2
  exit 1
fi
wait "${EARLY_PID}" 2>/dev/null || true
CLIENT_PIDS[${EARLY_INDEX}]=

EARLY_RELEASED=false
EARLY_RELEASE_DEADLINE=$(( $(date +%s) + CLIENT_COMPLETION_TIMEOUT ))
while [[ $(date +%s) -lt "${EARLY_RELEASE_DEADLINE}" ]]; do
  FINAL_CONNECTION_JSON=$(grep -F '"event":"connection_closed"' "${RUST_LOG}" | tail -1 || true)
  if [[ -n "${FINAL_CONNECTION_JSON}" ]] \
    && printf '%s\n' "${FINAL_CONNECTION_JSON}" \
      | jq -e '.active == 0 and .accepted == 9 and .completed == 9 and .failed == 0' \
        >/dev/null 2>&1; then
    EARLY_RELEASED=true
    break
  fi
  sleep 0.1
done
if [[ "${EARLY_RELEASED}" != true ]]; then
  echo "Rust SQL node did not release the separate ninth lifecycle client exactly once" >&2
  tail -260 "${RUST_LOG}" >&2
  exit 1
fi
archive_client_logs

SNAPSHOT_TSOS=$(printf '%s\n' "${SNAPSHOTS_JSON}" \
  | jq -r 'map(.snapshot_ts | tostring) | join(",")')
echo "Campaign 21 live concurrent authenticated SQL-node proof passed: eight concurrently connected stock clients authenticated and read exact (amount,id) pairs [(913,-7),(-2048,0),(77,42)] from real TiKV with max_query_active=${MAX_QUERY_ACTIVE}; a separate ninth lifecycle client was deliberately terminated and released exactly once; table_id=${TABLE_ID}; pd_cluster_id=${PD_CLUSTER_ID}; authority_id=${AUTHORITY_ID}; read_authority_id=${READ_AUTHORITY_ID}; snapshot_tsos=${SNAPSHOT_TSOS}; max_connection_active=${MAX_ACTIVE}; accepted=9; completed=9; active=0"
