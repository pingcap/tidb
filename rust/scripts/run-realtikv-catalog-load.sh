#!/usr/bin/env bash
#
# Live proof that the Rust SQL node can read a schema it never created.
#
# A real Go tidb-server (the playground's own) creates the database, the table,
# and the rows. The Rust node is told only the PD address and the table's NAME;
# it discovers the table ID, the column IDs, and every column type by reading
# the cluster's stored catalog out of TiKV's `m` meta namespace, then serves a
# SELECT over the rows the real TiDB inserted.
#
# A second table with a column type this node cannot decode proves the other
# half of the contract: it is listed at startup with the exact refusal reason
# and refused by name at query time rather than silently hidden.

set -euo pipefail

for prerequisite in tiup cargo curl jq nc pgrep ps awk sed seq grep sort openssl; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing catalog-load prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${CATALOG_LOAD_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "CATALOG_LOAD_MYSQL_CLIENT must name an executable stock MySQL client" >&2
  exit 1
fi
MYSQL_PLUGIN_ARGS=()
if [[ -n "${CATALOG_LOAD_MYSQL_PLUGIN_DIR:-}" ]]; then
  if [[ ! -f "${CATALOG_LOAD_MYSQL_PLUGIN_DIR}/mysql_native_password.so" ]]; then
    echo "CATALOG_LOAD_MYSQL_PLUGIN_DIR does not contain mysql_native_password.so" >&2
    exit 1
  fi
  MYSQL_PLUGIN_ARGS=(--plugin-dir="${CATALOG_LOAD_MYSQL_PLUGIN_DIR}")
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
TAG="campaign26-catalog-load-${$}-$(date +%s)"
PORT_OFFSET=${CATALOG_LOAD_PORT_OFFSET:-41000}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "CATALOG_LOAD_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
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
AUTH_USER=campaign26
AUTH_PASSWORD=${CATALOG_LOAD_AUTH_PASSWORD:-campaign26-native-password}
DATABASE=campaign26
SERVED_TABLE=catalog_rows
REFUSED_TABLE=refused_rows
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
    echo "catalog-load cleanup failed: Rust SQL node ${RUST_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  OWNED_PIDS=$(merge_owned_pids)
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "catalog-load cleanup failed: tiup clean failed for ${TAG}" >&2
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
    echo "catalog-load cleanup failed: owned process or TiUP registry row remains" >&2
    cleanup_failed=true
  fi

  local address
  for address in ${STORE_ADDRESSES}; do
    local port=${address##*:}
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "catalog-load cleanup failed: TiKV ${address} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  for port in "${TIKV_SEED_PORT}" "${GO_SQL_PORT}" "${GO_STATUS_PORT}"; do
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "catalog-load cleanup failed: port ${port} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "catalog-load cleanup failed: PD ${PD_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
    if [[ -n "${RUNTIME_DIR}" ]]; then
      rm -rf -- "${RUNTIME_DIR}"
    fi
    if [[ -e "${TAG_DIR}" ]] || { [[ -n "${RUNTIME_DIR}" ]] && [[ -e "${RUNTIME_DIR}" ]]; }; then
      echo "catalog-load cleanup failed: owned data directory remains" >&2
      cleanup_failed=true
    fi
  fi
  if [[ "${cleanup_failed}" == false ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG}" "${MYSQL_LOG}"
    echo "catalog-load cleanup proof passed"
  else
    echo "catalog-load retained logs: ${PLAYGROUND_LOG} ${RUST_LOG} ${MYSQL_LOG}" >&2
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
if [[ -z "${CATALOG_LOAD_RUST_SERVER:-}" ]]; then
  CARGO_BUILD_JOBS=12 cargo build -j12 -p tidb-server --bin tidb-server
  RUST_SERVER="${RUST_ROOT}/target/debug/tidb-server"
else
  RUST_SERVER=${CATALOG_LOAD_RUST_SERVER}
fi
if [[ ! -x "${RUST_SERVER}" ]]; then
  echo "catalog-load Rust server is not executable: ${RUST_SERVER}" >&2
  exit 1
fi

for port in "${PD_PORT}" "${GO_SQL_PORT}" "${TIKV_SEED_PORT}" \
  "${GO_STATUS_PORT}" "${RUST_SQL_PORT}"; do
  if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
    echo "refusing occupied catalog-load port ${port}; set CATALOG_LOAD_PORT_OFFSET" >&2
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
  echo "could not derive the catalog-load native-password stage-two hash" >&2
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

# Every schema object and every row below is created by the playground's REAL
# Go tidb-server. The Rust node never runs a DDL and is never told an ID.
go_tidb <<SQL
DROP DATABASE IF EXISTS ${DATABASE};
CREATE DATABASE ${DATABASE};
CREATE TABLE ${DATABASE}.${SERVED_TABLE} (
  id BIGINT PRIMARY KEY CLUSTERED,
  balance BIGINT NOT NULL,
  counter BIGINT UNSIGNED NOT NULL,
  score DOUBLE NOT NULL,
  label CHAR(16) NOT NULL
);
INSERT INTO ${DATABASE}.${SERVED_TABLE} VALUES
  (-7, 913, 18446744073709551615, 1.5, 'alpha'),
  (0, -2048, 0, -0.25, 'beta'),
  (42, 77, 4294967296, 3.75, 'gamma');
CREATE TABLE ${DATABASE}.${REFUSED_TABLE} (
  id BIGINT PRIMARY KEY CLUSTERED,
  note VARCHAR(64) NOT NULL
);
INSERT INTO ${DATABASE}.${REFUSED_TABLE} VALUES (1, 'unreadable');
SQL

TABLE_ID=$(go_tidb -Nse \
  "select tidb_table_id from information_schema.tables where table_schema='${DATABASE}' and table_name='${SERVED_TABLE}'")
if [[ ! "${TABLE_ID}" =~ ^[0-9]+$ ]] || [[ "${TABLE_ID}" =~ ^0+$ ]]; then
  echo "Go TiDB did not resolve the physical table ID of the created table" >&2
  exit 1
fi
# Row order is not part of this proof (the bounded read path has no ORDER BY
# yet), so both sides are compared as sorted sets.
GO_ROWS=$(go_tidb -N -B -e \
  "SELECT id, balance, counter, score, label FROM ${DATABASE}.${SERVED_TABLE}" | sort)

# Only the PD address, the table NAMES, and the accounts file. No table ID, no
# column IDs, no column types.
"${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
  --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
  --load-table "${DATABASE}.${SERVED_TABLE}" \
  --load-table "${DATABASE}.${REFUSED_TABLE}" \
  --auth-file "${AUTH_FILE}" --max-connections 4 \
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

# The proof that the schema was LOADED: the node published the physical table
# ID and the exact column shape that only the cluster's stored catalog knows.
if ! printf '%s\n' "${READY_JSON}" | jq -e \
  --arg table_id "${TABLE_ID}" --arg cluster_id "${PD_CLUSTER_ID}" \
  --arg database "${DATABASE}" --arg table "${SERVED_TABLE}" \
  '(.tables | length) == 1
   and (.tables[0].table_id | tostring) == $table_id
   and .tables[0].database == $database and .tables[0].table == $table
   and (.tables[0].columns | length) == 5
   and (.tables[0].columns[0] | endswith(":clustered-pk"))
   and (.tables[0].columns[0] | startswith("id:"))
   and (.tables[0].columns[1] | endswith(":stored-not-null"))
   and (.tables[0].columns[2] | endswith(":stored-unsigned-bigint-not-null"))
   and (.tables[0].columns[3] | endswith(":stored-double-not-null"))
   and (.tables[0].columns[4] | endswith(":stored-char-not-null:16"))
   and (.cluster_id | tostring) == $cluster_id' \
  >/dev/null; then
  echo "Rust readiness did not carry the cluster-loaded table identity and column shape" >&2
  printf '%s\n' "${READY_JSON}" >&2
  exit 1
fi

# The other half: the unreadable table is listed with its exact reason.
if ! printf '%s\n' "${READY_JSON}" | jq -e \
  --arg name "${DATABASE}.${REFUSED_TABLE}" \
  '(.refused_tables | length) == 1
   and .refused_tables[0].table == $name
   and (.refused_tables[0].reason | test("`note`"))
   and (.refused_tables[0].reason | contains("VARCHAR(64)"))' \
  >/dev/null; then
  echo "Rust readiness did not list the unreadable table with a precise reason" >&2
  printf '%s\n' "${READY_JSON}" >&2
  exit 1
fi

if ! RUST_ROWS=$(rust_node -N -B \
  -e "SELECT id, balance, counter, score, label FROM ${DATABASE}.${SERVED_TABLE}" \
  2>"${MYSQL_LOG}" | sort); then
  echo "stock MySQL client query against the Rust SQL node failed" >&2
  cat "${MYSQL_LOG}" >&2
  tail -200 "${RUST_LOG}" >&2
  exit 1
fi
if [[ "${RUST_ROWS}" != "${GO_ROWS}" ]]; then
  echo "Rust SQL node did not return the rows the real TiDB inserted" >&2
  printf 'go tidb:\n%s\nrust node:\n%s\n' "${GO_ROWS}" "${RUST_ROWS}" >&2
  tail -200 "${RUST_LOG}" >&2
  exit 1
fi

REFUSAL_OUTPUT=$(rust_node -N -B \
  -e "SELECT id FROM ${DATABASE}.${REFUSED_TABLE}" 2>&1 || true)
if ! printf '%s\n' "${REFUSAL_OUTPUT}" | grep -q 'note'; then
  echo "Rust SQL node did not refuse the unreadable table by naming its column" >&2
  printf '%s\n' "${REFUSAL_OUTPUT}" >&2
  exit 1
fi
if ! printf '%s\n' "${REFUSAL_OUTPUT}" | grep -q 'VARCHAR(64)'; then
  echo "Rust SQL node refusal did not name the unreadable column type" >&2
  printf '%s\n' "${REFUSAL_OUTPUT}" >&2
  exit 1
fi

ROW_COUNT=$(printf '%s\n' "${RUST_ROWS}" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')
echo "catalog-load live proof passed: the Rust node loaded ${DATABASE}.${SERVED_TABLE} (table_id=${TABLE_ID}, 5 columns) from the cluster catalog it never wrote, served ${ROW_COUNT} rows identical to the creating Go TiDB, and refused ${DATABASE}.${REFUSED_TABLE} by naming column \`note\` VARCHAR(64); pd_cluster_id=${PD_CLUSTER_ID}"
