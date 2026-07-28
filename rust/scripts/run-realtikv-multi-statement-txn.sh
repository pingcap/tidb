#!/usr/bin/env bash
#
# Live proof that one connection holds a real transaction open across several
# statements against a real TiKV cluster.
#
# A real Go tidb-server (the playground's own) creates the table and its rows.
# The Rust node is told only the PD address and the table's NAME. Two
# independent client connections then prove, on the wire, what a
# multi-statement transaction actually is:
#
#   * pessimistic: A opens a transaction, UPDATEs a row and reads its own
#     uncommitted value back; B still sees the old one; B's
#     `SELECT ... FOR UPDATE NOWAIT` on that row is refused with 3572 while A
#     holds the lock, and B's transaction survives that refusal; once A
#     COMMITs, B reads the new value and can take the lock itself.
#   * optimistic: two transactions write one row without locking. The first
#     COMMIT wins, the second is refused with 9007, and the winner's value is
#     the durable one.

set -euo pipefail

for prerequisite in tiup cargo curl jq nc pgrep ps awk sed seq grep sort openssl python3; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing multi-statement-txn prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${MULTI_TXN_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "MULTI_TXN_MYSQL_CLIENT must name an executable stock MySQL client" >&2
  exit 1
fi
MYSQL_PLUGIN_ARGS=()
if [[ -n "${MULTI_TXN_MYSQL_PLUGIN_DIR:-}" ]]; then
  if [[ ! -f "${MULTI_TXN_MYSQL_PLUGIN_DIR}/mysql_native_password.so" ]]; then
    echo "MULTI_TXN_MYSQL_PLUGIN_DIR does not contain mysql_native_password.so" >&2
    exit 1
  fi
  MYSQL_PLUGIN_ARGS=(--plugin-dir="${MULTI_TXN_MYSQL_PLUGIN_DIR}")
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
TAG="campaign26-multi-statement-txn-${$}-$(date +%s)"
PORT_OFFSET=${MULTI_TXN_PORT_OFFSET:-41500}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "MULTI_TXN_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
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
CLIENT_LOG="${TMPDIR:-/tmp}/${TAG}-client.log"
RUNTIME_DIR=
AUTH_FILE=
AUTH_USER=campaign26
AUTH_PASSWORD=${MULTI_TXN_AUTH_PASSWORD:-campaign26-native-password}
DATABASE=campaign26
SERVED_TABLE=txn_rows
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
    echo "multi-statement-txn cleanup failed: Rust SQL node ${RUST_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  OWNED_PIDS=$(merge_owned_pids)
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "multi-statement-txn cleanup failed: tiup clean failed for ${TAG}" >&2
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
    echo "multi-statement-txn cleanup failed: owned process or TiUP registry row remains" >&2
    cleanup_failed=true
  fi

  local address
  for address in ${STORE_ADDRESSES}; do
    local port=${address##*:}
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "multi-statement-txn cleanup failed: TiKV ${address} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  for port in "${TIKV_SEED_PORT}" "${GO_SQL_PORT}" "${GO_STATUS_PORT}"; do
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "multi-statement-txn cleanup failed: port ${port} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "multi-statement-txn cleanup failed: PD ${PD_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
    if [[ -n "${RUNTIME_DIR}" ]]; then
      rm -rf -- "${RUNTIME_DIR}"
    fi
    if [[ -e "${TAG_DIR}" ]] || { [[ -n "${RUNTIME_DIR}" ]] && [[ -e "${RUNTIME_DIR}" ]]; }; then
      echo "multi-statement-txn cleanup failed: owned data directory remains" >&2
      cleanup_failed=true
    fi
  fi
  if [[ "${cleanup_failed}" == false ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG}" "${MYSQL_LOG}" "${CLIENT_LOG}"
    echo "multi-statement-txn cleanup proof passed"
  else
    echo "multi-statement-txn retained logs: ${PLAYGROUND_LOG} ${RUST_LOG} ${MYSQL_LOG} ${CLIENT_LOG}" >&2
  fi
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}

go_tidb() {
  "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
    -uroot --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" "$@"
}

rust_node() {
  "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${RUST_SQL_PORT}" \
    -u"${AUTH_USER}" -p"${AUTH_PASSWORD}" --connect-timeout=5 \
    "${MYSQL_PLUGIN_ARGS[@]}" "$@"
}

cd "${RUST_ROOT}"
if [[ -z "${MULTI_TXN_RUST_SERVER:-}" ]]; then
  CARGO_BUILD_JOBS=12 cargo build -j12 -p tidb-server --bin tidb-server
  RUST_SERVER="${RUST_ROOT}/target/debug/tidb-server"
else
  RUST_SERVER=${MULTI_TXN_RUST_SERVER}
fi
if [[ ! -x "${RUST_SERVER}" ]]; then
  echo "multi-statement-txn Rust server is not executable: ${RUST_SERVER}" >&2
  exit 1
fi

for port in "${PD_PORT}" "${GO_SQL_PORT}" "${TIKV_SEED_PORT}" \
  "${GO_STATUS_PORT}" "${RUST_SQL_PORT}"; do
  if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
    echo "refusing occupied multi-statement-txn port ${port}; set MULTI_TXN_PORT_OFFSET" >&2
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
  echo "could not derive the multi-statement-txn native-password stage-two hash" >&2
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
    && go_tidb -Nse 'select 1' >/dev/null 2>&1; then
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
  exit 1
fi


# The schema and every starting row are created by the playground's REAL Go
# tidb-server. The Rust node never runs a DDL and is never told an ID.
go_tidb <<SQL
DROP DATABASE IF EXISTS ${DATABASE};
CREATE DATABASE ${DATABASE};
CREATE TABLE ${DATABASE}.${SERVED_TABLE} (
  id BIGINT PRIMARY KEY CLUSTERED,
  balance BIGINT NOT NULL
);
INSERT INTO ${DATABASE}.${SERVED_TABLE} VALUES (1, 100), (2, 200);
SQL

TABLE_ID=$(go_tidb -Nse \
  "select tidb_table_id from information_schema.tables where table_schema='${DATABASE}' and table_name='${SERVED_TABLE}'")
if [[ ! "${TABLE_ID}" =~ ^[0-9]+$ ]] || [[ "${TABLE_ID}" =~ ^0+$ ]]; then
  echo "Go TiDB did not resolve the physical table ID of the created table" >&2
  exit 1
fi

# Only the PD address, the table NAME, and the accounts file.
"${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
  --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
  --load-table "${DATABASE}.${SERVED_TABLE}" \
  --auth-file "${AUTH_FILE}" --max-connections 8 \
  >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

READY_JSON=
for _ in $(seq 1 900); do
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
  '(.tables | length) == 1
   and (.tables[0].table_id | tostring) == $table_id
   and (.cluster_id | tostring) == $cluster_id' \
  >/dev/null; then
  echo "Rust readiness did not carry the cluster-loaded table identity" >&2
  printf '%s\n' "${READY_JSON}" >&2
  exit 1
fi

txn_client() {
  python3 "${RUST_ROOT}/scripts/multi-statement-txn-client.py" "$@" \
    --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
    --user "${AUTH_USER}" --password "${AUTH_PASSWORD}" \
    --database "${DATABASE}" --table "${SERVED_TABLE}"
}

# One pessimistic transaction: its own writes, another connection's isolation
# from them, the row lock it holds, and everything the COMMIT releases.
if ! txn_client pessimistic --row-id 1 --new-balance 4242 | tee "${CLIENT_LOG}"; then
  echo "the pessimistic multi-statement transaction proof failed" >&2
  tail -200 "${RUST_LOG}" >&2
  exit 1
fi
if ! grep -q '"event":"passed","command":"pessimistic"' "${CLIENT_LOG}"; then
  echo "the pessimistic proof did not report a pass" >&2
  exit 1
fi

# The committed value is public to the CREATING Go TiDB as well, which is the
# proof that the Rust node published a real transaction rather than answering
# from its own memory.
GO_BALANCE=$(go_tidb -Nse "SELECT balance FROM ${DATABASE}.${SERVED_TABLE} WHERE id = 1")
if [[ "${GO_BALANCE}" != "4242" ]]; then
  echo "the real Go TiDB does not see the Rust transaction's committed row: ${GO_BALANCE}" >&2
  exit 1
fi

# Two optimistic transactions racing for one row: exactly one wins.
if ! txn_client optimistic --row-id 2 --first-balance 777 --second-balance 888 \
  | tee -a "${CLIENT_LOG}"; then
  echo "the optimistic multi-statement transaction proof failed" >&2
  tail -200 "${RUST_LOG}" >&2
  exit 1
fi
if ! grep -q '"event":"passed","command":"optimistic"' "${CLIENT_LOG}"; then
  echo "the optimistic proof did not report a pass" >&2
  exit 1
fi
GO_BALANCE=$(go_tidb -Nse "SELECT balance FROM ${DATABASE}.${SERVED_TABLE} WHERE id = 2")
if [[ "${GO_BALANCE}" != "777" ]]; then
  echo "the losing optimistic transaction's value reached storage: ${GO_BALANCE}" >&2
  exit 1
fi

echo "multi-statement-txn live proof passed: on ${DATABASE}.${SERVED_TABLE} (table_id=${TABLE_ID}) a \
pessimistic transaction read its own uncommitted UPDATE while another connection saw the old row and \
was refused the lock with 3572, its COMMIT published balance=4242 to the creating Go TiDB, and of two \
optimistic transactions writing one row the first COMMIT won while the second was refused with 9007; \
pd_cluster_id=${PD_CLUSTER_ID}"
