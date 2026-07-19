#!/usr/bin/env bash

set -euo pipefail

for prerequisite in tiup cargo curl jq nc pgrep ps awk sed seq grep sort; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing Campaign 19 prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${C19_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "C19_MYSQL_CLIENT must name an executable stock MySQL client" >&2
  exit 1
fi
MYSQL_PLUGIN_ARGS=()
if [[ -n "${C19_MYSQL_PLUGIN_DIR:-}" ]]; then
  if [[ ! -f "${C19_MYSQL_PLUGIN_DIR}/mysql_native_password.so" ]]; then
    echo "C19_MYSQL_PLUGIN_DIR does not contain mysql_native_password.so" >&2
    exit 1
  fi
  MYSQL_PLUGIN_ARGS=(--plugin-dir="${C19_MYSQL_PLUGIN_DIR}")
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
TAG="campaign19-sql-node-${$}-$(date +%s)"
PORT_OFFSET=${C19_PORT_OFFSET:-39000}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "C19_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
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
PLAYGROUND_PID=
RUST_PID=
OWNED_PIDS=
STORE_ADDRESSES=

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

cleanup() {
  local original_status=$?
  local cleanup_failed=false
  trap - EXIT INT TERM

  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    kill "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  if nc -z -w 1 127.0.0.1 "${RUST_SQL_PORT}" >/dev/null 2>&1; then
    echo "Campaign 19 cleanup failed: Rust SQL node ${RUST_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  OWNED_PIDS=$(merge_owned_pids)
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "Campaign 19 cleanup failed: tiup clean failed for ${TAG}" >&2
    cleanup_failed=true
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
    echo "Campaign 19 cleanup failed: owned process or TiUP registry row remains" >&2
    cleanup_failed=true
  fi

  local address
  for address in ${STORE_ADDRESSES}; do
    local port=${address##*:}
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "Campaign 19 cleanup failed: TiKV ${address} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if nc -z -w 1 127.0.0.1 "${TIKV_SEED_PORT}" >/dev/null 2>&1; then
    echo "Campaign 19 cleanup failed: TiKV seed 127.0.0.1:${TIKV_SEED_PORT} remains reachable" >&2
    cleanup_failed=true
  fi
  if nc -z -w 1 127.0.0.1 "${GO_SQL_PORT}" >/dev/null 2>&1; then
    echo "Campaign 19 cleanup failed: Go TiDB ${GO_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi
  if nc -z -w 1 127.0.0.1 "${GO_STATUS_PORT}" >/dev/null 2>&1; then
    echo "Campaign 19 cleanup failed: Go TiDB status port remains reachable" >&2
    cleanup_failed=true
  fi
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "Campaign 19 cleanup failed: PD ${PD_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
    if [[ -e "${TAG_DIR}" ]]; then
      echo "Campaign 19 cleanup failed: owned data directory remains" >&2
      cleanup_failed=true
    fi
  fi
  if [[ "${cleanup_failed}" == false ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG}" "${MYSQL_LOG}"
  else
    echo "Campaign 19 retained logs: ${PLAYGROUND_LOG} ${RUST_LOG} ${MYSQL_LOG}" >&2
  fi
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}

cd "${RUST_ROOT}"
if [[ -z "${C19_RUST_SERVER:-}" ]]; then
  CARGO_BUILD_JOBS=12 cargo build -j12 -p tidb-server --bin tidb-server
  RUST_SERVER="${RUST_ROOT}/target/debug/tidb-server"
else
  RUST_SERVER=${C19_RUST_SERVER}
fi
if [[ ! -x "${RUST_SERVER}" ]]; then
  echo "Campaign 19 Rust server is not executable: ${RUST_SERVER}" >&2
  exit 1
fi

for port in "${PD_PORT}" "${GO_SQL_PORT}" "${TIKV_SEED_PORT}" \
  "${GO_STATUS_PORT}" "${RUST_SQL_PORT}"; do
  if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
    echo "refusing occupied Campaign 19 port ${port}; set C19_PORT_OFFSET" >&2
    exit 1
  fi
done

trap cleanup EXIT INT TERM

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
DROP DATABASE IF EXISTS campaign19;
CREATE DATABASE campaign19;
CREATE TABLE campaign19.rows (id BIGINT PRIMARY KEY CLUSTERED);
INSERT INTO campaign19.rows VALUES (-7), (0), (42);
SQL

TABLE_ID=$("${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
  -uroot --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" -Nse \
  "select tidb_table_id from information_schema.tables where table_schema='campaign19' and table_name='rows'")
if [[ ! "${TABLE_ID}" =~ ^[0-9]+$ ]] || [[ "${TABLE_ID}" =~ ^0+$ ]]; then
  echo "Go TiDB did not resolve the Campaign 19 physical table ID" >&2
  exit 1
fi

"${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
  --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
  --database campaign19 --table rows --table-id "${TABLE_ID}" \
  --column id --column-id 1 >"${RUST_LOG}" 2>&1 &
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
   and .database == "campaign19" and .table == "rows"
   and .column == "id" and .column_id == 1' \
  >/dev/null; then
  echo "Rust readiness did not retain the Go-resolved table ID and real PD cluster identity" >&2
  printf '%s\n' "${READY_JSON}" >&2
  exit 1
fi

if ! QUERY_OUTPUT=$("${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 \
  -P "${RUST_SQL_PORT}" -uroot --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" -N -B \
  -e 'SELECT id FROM campaign19.rows' 2>"${MYSQL_LOG}"); then
  echo "stock MySQL client query against the Rust SQL node failed" >&2
  cat "${MYSQL_LOG}" >&2
  tail -200 "${RUST_LOG}" >&2
  exit 1
fi
printf 'query_output_begin\n%s\nquery_output_end\n' "${QUERY_OUTPUT}" >>"${MYSQL_LOG}"
NORMALIZED_ROWS=$(printf '%s\n' "${QUERY_OUTPUT}" | sed '/^[[:space:]]*$/d' | sort -n)
if [[ "${NORMALIZED_ROWS}" != $'-7\n0\n42' ]]; then
  echo "Rust SQL node did not return exactly the seeded real-TiKV rows" >&2
  printf 'expected:\n-7\n0\n42\nactual:\n%s\n' "${NORMALIZED_ROWS}" >&2
  tail -200 "${RUST_LOG}" >&2
  exit 1
fi

SNAPSHOT_JSON=
TRANSPORT_JSON=
CONNECTION_JSON=
for _ in $(seq 1 100); do
  SNAPSHOT_JSON=$(grep -F '"event":"query_snapshot"' "${RUST_LOG}" | tail -1 || true)
  TRANSPORT_JSON=$(grep -F '"event":"query_transport"' "${RUST_LOG}" | tail -1 || true)
  CONNECTION_JSON=$(grep -F '"event":"connection_closed"' "${RUST_LOG}" | tail -1 || true)
  if [[ -n "${SNAPSHOT_JSON}" && -n "${TRANSPORT_JSON}" && -n "${CONNECTION_JSON}" ]]; then
    break
  fi
  sleep 0.1
done
if ! printf '%s\n' "${SNAPSHOT_JSON}" | jq -e \
  --arg table_id "${TABLE_ID}" --arg cluster_id "${PD_CLUSTER_ID}" \
  '(.snapshot_ts | type) == "number" and .snapshot_ts > 0
   and (.table_id | tostring) == $table_id
   and (.cluster_id | tostring) == $cluster_id' \
  >/dev/null; then
  echo "Rust query did not publish a nonzero real PD TSO for the configured table" >&2
  printf '%s\n' "${SNAPSHOT_JSON}" >&2
  exit 1
fi
if ! printf '%s\n' "${TRANSPORT_JSON}" | jq -e \
  '(.dispatched_region_ids | type) == "array" and (.dispatched_region_ids | length) > 0 and .batch_attempts >= 1' \
  >/dev/null; then
  echo "Rust query did not prove an actual BatchCommands-first dispatch over a PD-resolved region" >&2
  printf '%s\n' "${TRANSPORT_JSON}" >&2
  exit 1
fi
if ! printf '%s\n' "${CONNECTION_JSON}" | jq -e \
  '.active == 0 and .accepted >= 1 and .completed >= 1' >/dev/null; then
  echo "Rust connection lifecycle did not release the stock MySQL client to zero active connections" >&2
  printf '%s\n' "${CONNECTION_JSON}" >&2
  exit 1
fi

SNAPSHOT_TS=$(printf '%s\n' "${SNAPSHOT_JSON}" | jq -r '.snapshot_ts')
REGION_IDS=$(printf '%s\n' "${TRANSPORT_JSON}" | jq -r '.dispatched_region_ids | join(",")')
BATCH_ATTEMPTS=$(printf '%s\n' "${TRANSPORT_JSON}" | jq -r '.batch_attempts')
echo "Campaign 19 live SQL-node proof passed: stock MySQL client read [-7,0,42] from real TiKV; table_id=${TABLE_ID}; pd_cluster_id=${PD_CLUSTER_ID}; snapshot_ts=${SNAPSHOT_TS}; region_ids=${REGION_IDS}; batch_attempts=${BATCH_ATTEMPTS}; active_connections=0"
