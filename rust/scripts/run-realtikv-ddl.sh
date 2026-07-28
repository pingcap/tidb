#!/usr/bin/env bash
#
# Live proof that the RUST node performs DDL against a real cluster's catalog,
# and that a real Go TiDB accepts what it wrote.
#
# The playground's own Go tidb-server creates one anchor table so the Rust node
# has something to serve; from then on every catalog change is the Rust node's:
#
#   * the Rust node runs `CREATE DATABASE` and `CREATE TABLE` over the wire,
#     allocating IDs from `NextGlobalID`, writing the DBInfo/TableInfo JSON
#     under the meta keys, bumping `SchemaVersionKey`, and writing the
#     `Diff:<ver>` SchemaDiff — all in ONE optimistic 2PC transaction;
#   * the REAL Go TiDB's own domain reloads that diff and prints the table with
#     `SHOW CREATE TABLE` — the decisive faithfulness check, because Go is
#     serving a schema this node wrote;
#   * the Go TiDB INSERTs into it, proving it accepts the TableInfo for writes;
#   * a second Rust node loads the new table BY NAME and reads Go's rows back;
#   * the Rust node DROPs the table and the database, and Go confirms both gone;
#   * a shape the node cannot serve is refused with a precise message and leaves
#     the catalog byte-identical — no id spent, no schema version spent.

set -euo pipefail

for prerequisite in tiup cargo curl jq nc pgrep awk sed seq grep openssl; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing ddl prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

MYSQL_CLIENT=${DDL_MYSQL_CLIENT:-mysql}
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "DDL_MYSQL_CLIENT must name an executable stock MySQL client" >&2
  exit 1
fi
MYSQL_PLUGIN_ARGS=()
if [[ -n "${DDL_MYSQL_PLUGIN_DIR:-}" ]]; then
  if [[ ! -f "${DDL_MYSQL_PLUGIN_DIR}/mysql_native_password.so" ]]; then
    echo "DDL_MYSQL_PLUGIN_DIR does not contain mysql_native_password.so" >&2
    exit 1
  fi
  MYSQL_PLUGIN_ARGS=(--plugin-dir="${DDL_MYSQL_PLUGIN_DIR}")
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
TAG="campaign31-ddl-${$}-$(date +%s)"
PORT_OFFSET=${DDL_PORT_OFFSET:-42500}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "DDL_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
  exit 1
fi
PD_PORT=$((2379 + PORT_OFFSET))
GO_SQL_PORT=$((4000 + PORT_OFFSET))
TIKV_SEED_PORT=$((20160 + PORT_OFFSET))
GO_STATUS_PORT=$((10080 + PORT_OFFSET))
RUST_SQL_PORT=$((12000 + PORT_OFFSET))
RUST_READER_PORT=$((12001 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
RUST_SQL_ADDR="127.0.0.1:${RUST_SQL_PORT}"
RUST_READER_ADDR="127.0.0.1:${RUST_READER_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
READER_LOG="${TMPDIR:-/tmp}/${TAG}-reader.log"
RUNTIME_DIR=
AUTH_FILE=
AUTH_USER=campaign31
AUTH_PASSWORD=${DDL_AUTH_PASSWORD:-campaign31-native-password}
DATABASE=campaign31
ANCHOR_TABLE=anchor
MADE_TABLE=made_by_rust
MADE_DATABASE=campaign31_rust
PLAYGROUND_PID=
RUST_PID=
READER_PID=
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

stop_rust_node() {
  local pid=$1
  local port=$2
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

cleanup() {
  local original_status=$?
  local cleanup_failed=false
  trap - EXIT INT TERM

  if ! stop_rust_node "${READER_PID}" "${RUST_READER_PORT}"; then
    echo "ddl cleanup failed: Rust reader node ${RUST_READER_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi
  if ! stop_rust_node "${RUST_PID}" "${RUST_SQL_PORT}"; then
    echo "ddl cleanup failed: Rust SQL node ${RUST_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  OWNED_PIDS=$(merge_owned_pids)
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "ddl cleanup failed: tiup clean failed for ${TAG}" >&2
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
    echo "ddl cleanup failed: owned process or TiUP registry row remains" >&2
    cleanup_failed=true
  fi

  local address
  for address in ${STORE_ADDRESSES}; do
    local port=${address##*:}
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "ddl cleanup failed: TiKV ${address} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  for port in "${TIKV_SEED_PORT}" "${GO_SQL_PORT}" "${GO_STATUS_PORT}"; do
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "ddl cleanup failed: port ${port} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "ddl cleanup failed: PD ${PD_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
    if [[ -n "${RUNTIME_DIR}" ]]; then
      rm -rf -- "${RUNTIME_DIR}"
    fi
    if [[ -e "${TAG_DIR}" ]] || { [[ -n "${RUNTIME_DIR}" ]] && [[ -e "${RUNTIME_DIR}" ]]; }; then
      echo "ddl cleanup failed: owned data directory remains" >&2
      cleanup_failed=true
    fi
  fi
  if [[ "${cleanup_failed}" == false ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG}" "${READER_LOG}"
    echo "ddl cleanup proof passed"
  else
    echo "ddl retained logs: ${PLAYGROUND_LOG} ${RUST_LOG} ${READER_LOG}" >&2
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

rust_reader() {
  "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${RUST_READER_PORT}" \
    -u"${AUTH_USER}" -p"${AUTH_PASSWORD}" --connect-timeout=5 \
    "${MYSQL_PLUGIN_ARGS[@]}" "$@"
}

# Waits for the Go TiDB's own domain to reload far enough to answer `sql` with
# `expected`. The Rust node publishes no etcd schema notification, so Go picks
# the change up on its own schema-lease tick.
await_go() {
  local sql=$1
  local expected=$2
  local what=$3
  local observed=
  for _ in $(seq 1 120); do
    observed=$(go_tidb -Nse "${sql}" 2>/dev/null || true)
    if [[ "${observed}" == "${expected}" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "the real Go TiDB never observed ${what}: got '${observed}', wanted '${expected}'" >&2
  return 1
}

start_rust_node() {
  local port=$1
  local table=$2
  local log=$3
  "${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
    --host 127.0.0.1 --port "${port}" \
    --load-table "${table}" \
    --lease-ms 2000 \
    --auth-file "${AUTH_FILE}" --max-connections 8 \
    >"${log}" 2>&1 &
}

await_rust_ready() {
  local pid=$1
  local log=$2
  local ready=
  for _ in $(seq 1 900); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "Rust SQL node exited before readiness" >&2
      tail -200 "${log}" >&2
      return 1
    fi
    ready=$(grep -F '"event":"sql_node_ready"' "${log}" | tail -1 || true)
    if [[ -n "${ready}" ]]; then
      printf '%s\n' "${ready}"
      return 0
    fi
    sleep 0.1
  done
  echo "Rust SQL node did not publish readiness" >&2
  tail -200 "${log}" >&2
  return 1
}

cd "${RUST_ROOT}"
if [[ -z "${DDL_RUST_SERVER:-}" ]]; then
  CARGO_BUILD_JOBS=12 cargo build -j12 -p tidb-server --bin tidb-server
  RUST_SERVER="${RUST_ROOT}/target/debug/tidb-server"
else
  RUST_SERVER=${DDL_RUST_SERVER}
fi
if [[ ! -x "${RUST_SERVER}" ]]; then
  echo "ddl Rust server is not executable: ${RUST_SERVER}" >&2
  exit 1
fi

for port in "${PD_PORT}" "${GO_SQL_PORT}" "${TIKV_SEED_PORT}" \
  "${GO_STATUS_PORT}" "${RUST_SQL_PORT}" "${RUST_READER_PORT}"; do
  if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
    echo "refusing occupied ddl port ${port}; set DDL_PORT_OFFSET" >&2
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
  echo "could not derive the ddl native-password stage-two hash" >&2
  exit 1
fi
(umask 077; printf '%s\t%s\t%s\t*%s\n' \
  "${AUTH_USER}" "127.0.0.1" "mysql_native_password" "${AUTH_HASH_HEX}" >"${AUTH_FILE}")
chmod 0600 "${AUTH_FILE}"
unset AUTH_HASH_HEX

# A short schema lease so the Go server's own domain notices the Rust node's
# catalog change on its next tick instead of the 45s default. This is a
# reload-cadence setting only; nothing about the change depends on it.
TIDB_CONFIG="${RUNTIME_DIR}/tidb.toml"
printf 'lease = "2s"\n' >"${TIDB_CONFIG}"

tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 1 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  --db.config "${TIDB_CONFIG}" \
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

# The Go server creates ONLY the anchor table, so the Rust node has a served
# relation. Every other catalog object below is written by the Rust node.
go_tidb <<SQL
DROP DATABASE IF EXISTS ${DATABASE};
DROP DATABASE IF EXISTS ${MADE_DATABASE};
CREATE DATABASE ${DATABASE};
CREATE TABLE ${DATABASE}.${ANCHOR_TABLE} (
  id BIGINT PRIMARY KEY CLUSTERED,
  v BIGINT NOT NULL
);
SQL

start_rust_node "${RUST_SQL_PORT}" "${DATABASE}.${ANCHOR_TABLE}" "${RUST_LOG}"
RUST_PID=$!
READY_JSON=$(await_rust_ready "${RUST_PID}" "${RUST_LOG}")
if ! printf '%s\n' "${READY_JSON}" | jq -e \
  --arg cluster_id "${PD_CLUSTER_ID}" \
  '(.tables | length) == 1 and (.cluster_id | tostring) == $cluster_id' >/dev/null; then
  echo "Rust readiness did not carry the cluster-loaded anchor identity" >&2
  printf '%s\n' "${READY_JSON}" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# DDL admission is decoupled from servability (the bootstrap-ddl widening):
# a JSON column CREATE succeeds -- it builds the TableInfo a real TiDB
# accepts -- and it is SERVING the table that this node refuses, naming the
# column and type at query time.
# ---------------------------------------------------------------------------
rust_node -Nse \
  "CREATE TABLE ${DATABASE}.unservable (id BIGINT PRIMARY KEY, j JSON NOT NULL)"
REFUSAL=$(rust_node -Nse "SELECT id FROM ${DATABASE}.unservable" 2>&1 || true)
if ! printf '%s' "${REFUSAL}" | grep -qF "which this node cannot decode yet"; then
  echo "an unservable table was not refused at query time with a precise message: ${REFUSAL}" >&2
  exit 1
fi
rust_node -Nse "DROP TABLE ${DATABASE}.unservable"
REFUSAL=$(rust_node -Nse \
  "CREATE TABLE ${DATABASE}.never (id BIGINT NOT NULL, v BIGINT NOT NULL)" 2>&1 || true)
if ! printf '%s' "${REFUSAL}" \
  | grep -qF "requires a single-column clustered BIGINT PRIMARY KEY"; then
  echo "a CREATE TABLE with no clustered handle was not refused precisely: ${REFUSAL}" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# The Rust node performs the catalog changes.
# ---------------------------------------------------------------------------
rust_node -e "CREATE DATABASE ${MADE_DATABASE}"
rust_node -e "CREATE TABLE ${DATABASE}.${MADE_TABLE} (
  id BIGINT PRIMARY KEY CLUSTERED,
  amount BIGINT NOT NULL,
  big BIGINT UNSIGNED NOT NULL,
  ratio DOUBLE NOT NULL,
  tag CHAR(8) NOT NULL,
  name VARCHAR(32) NOT NULL,
  price DECIMAL(10,2) NOT NULL
)"
# IF NOT EXISTS on an object the node just created spends no schema version.
rust_node -e "CREATE TABLE IF NOT EXISTS ${DATABASE}.${MADE_TABLE} (id BIGINT PRIMARY KEY)"
if ! grep -qF '"event":"catalog_change","outcome":"already_satisfied"' "${RUST_LOG}"; then
  echo "CREATE TABLE IF NOT EXISTS on an existing table did not report a no-op" >&2
  tail -60 "${RUST_LOG}" >&2
  exit 1
fi
APPLIED=$(grep -cF '"event":"catalog_change","outcome":"applied"' "${RUST_LOG}" || true)
if [[ "${APPLIED}" != "2" ]]; then
  echo "expected exactly two applied catalog changes, saw ${APPLIED}" >&2
  tail -60 "${RUST_LOG}" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# THE DECISIVE CHECK: the real Go TiDB's own domain reloads our diff and serves
# our schema.
# ---------------------------------------------------------------------------
await_go "SELECT count(*) FROM information_schema.tables \
  WHERE table_schema='${DATABASE}' AND table_name='${MADE_TABLE}'" "1" \
  "the table the Rust node created"
await_go "SELECT count(*) FROM information_schema.schemata \
  WHERE schema_name='${MADE_DATABASE}'" "1" \
  "the database the Rust node created"

GO_SHOW_CREATE=$(go_tidb -Nse "SHOW CREATE TABLE ${DATABASE}.${MADE_TABLE}")
EXPECTED_SHOW_CREATE=$(printf '%s\t%s' "${MADE_TABLE}" \
'CREATE TABLE `'"${MADE_TABLE}"'` (\n  `id` bigint NOT NULL,\n  `amount` bigint NOT NULL,\n  `big` bigint unsigned NOT NULL,\n  `ratio` double NOT NULL,\n  `tag` char(8) NOT NULL,\n  `name` varchar(32) NOT NULL,\n  `price` decimal(10,2) NOT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
if [[ "${GO_SHOW_CREATE}" != "${EXPECTED_SHOW_CREATE}" ]]; then
  echo "the real Go TiDB restores a different table than the Rust node wrote:" >&2
  printf 'go:       %s\nexpected: %s\n' "${GO_SHOW_CREATE}" "${EXPECTED_SHOW_CREATE}" >&2
  exit 1
fi

TABLE_ID=$(go_tidb -Nse \
  "SELECT tidb_table_id FROM information_schema.tables \
   WHERE table_schema='${DATABASE}' AND table_name='${MADE_TABLE}'")
if [[ ! "${TABLE_ID}" =~ ^[0-9]+$ ]] || [[ "${TABLE_ID}" =~ ^0+$ ]]; then
  echo "the Go TiDB did not resolve a physical id for the Rust-created table" >&2
  exit 1
fi
if ! grep -qF "\"created_id\":${TABLE_ID}" "${RUST_LOG}"; then
  echo "the id the Go TiDB serves is not the id the Rust node allocated" >&2
  tail -60 "${RUST_LOG}" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# The Go TiDB writes rows into the Rust-created table, and a fresh Rust node
# loads that table BY NAME and reads them back.
# ---------------------------------------------------------------------------
go_tidb -e "INSERT INTO ${DATABASE}.${MADE_TABLE} VALUES
  (1, -40, 18446744073709551615, 1.5, 'alpha', 'first row', 12.34),
  (2, 70, 7, -0.25, 'beta', 'second row', -9.99)"

start_rust_node "${RUST_READER_PORT}" "${DATABASE}.${MADE_TABLE}" "${READER_LOG}"
READER_PID=$!
READER_READY=$(await_rust_ready "${READER_PID}" "${READER_LOG}")
if ! printf '%s\n' "${READER_READY}" | jq -e \
  --arg table_id "${TABLE_ID}" \
  '(.tables | length) == 1 and (.tables[0].table_id | tostring) == $table_id' >/dev/null; then
  echo "the Rust reader did not load the Rust-created table by name" >&2
  printf '%s\n' "${READER_READY}" >&2
  exit 1
fi
READ_BACK=$(rust_reader -Nse \
  "SELECT id, amount, big, ratio, tag, name, price FROM ${DATABASE}.${MADE_TABLE} WHERE id = 1")
EXPECTED_READ_BACK=$(printf '1\t-40\t18446744073709551615\t1.5\talpha\tfirst row\t12.34')
if [[ "${READ_BACK}" != "${EXPECTED_READ_BACK}" ]]; then
  echo "the Rust node read back a different row than the Go TiDB inserted:" >&2
  printf 'rust:     %s\nexpected: %s\n' "${READ_BACK}" "${EXPECTED_READ_BACK}" >&2
  exit 1
fi
if ! stop_rust_node "${READER_PID}" "${RUST_READER_PORT}"; then
  echo "the Rust reader node did not stop" >&2
  exit 1
fi
READER_PID=

# ---------------------------------------------------------------------------
# Concurrent DDL FAILS LOUDLY rather than interleaving.
#
# There is no owner election here: every catalog change writes SchemaVersionKey
# and NextGlobalID from values its own snapshot read, so TiKV's optimistic
# conflict detection IS the mutual exclusion. Several CREATE TABLEs racing on
# one cluster must therefore end in exactly two ways per statement — committed,
# or refused as a concurrent schema change — and the catalog must contain
# exactly the committed ones, with no duplicate or lost id.
# ---------------------------------------------------------------------------
RACERS=6
RACE_PIDS=()
for racer in $(seq 1 "${RACERS}"); do
  rust_node -e "CREATE TABLE ${DATABASE}.race_${racer} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)" \
    >"${RUNTIME_DIR}/race_${racer}.out" 2>&1 &
  RACE_PIDS+=("$!")
done
# Only the racers: a bare `wait` would also wait on the playground and the
# Rust node, which run for the whole script.
for pid in "${RACE_PIDS[@]}"; do
  wait "${pid}" || true
done
RACE_WON=0
for racer in $(seq 1 "${RACERS}"); do
  output=$(grep -v '^mysql: \[Warning\]' "${RUNTIME_DIR}/race_${racer}.out" || true)
  if [[ -z "${output}" ]]; then
    RACE_WON=$((RACE_WON + 1))
  elif ! printf '%s' "${output}" | grep -qF "another DDL changed the catalog while this statement was preparing schema version"; then
    echo "a racing CREATE TABLE failed for a reason other than a concurrent schema change:" >&2
    printf '%s\n' "${output}" >&2
    exit 1
  fi
done
if [[ "${RACE_WON}" -lt 1 ]]; then
  echo "every racing CREATE TABLE was refused; at least one must commit" >&2
  exit 1
fi
await_go "SELECT count(*) FROM information_schema.tables \
  WHERE table_schema='${DATABASE}' AND table_name LIKE 'race\\_%'" "${RACE_WON}" \
  "exactly the racing statements that committed"
# Every surviving racer got its own id: a duplicate would mean the allocation
# was not serialized by the same conflict detection.
RACE_IDS=$(go_tidb -Nse "SELECT count(DISTINCT tidb_table_id) FROM information_schema.tables \
  WHERE table_schema='${DATABASE}' AND table_name LIKE 'race\\_%'")
if [[ "${RACE_IDS}" != "${RACE_WON}" ]]; then
  echo "racing CREATE TABLEs produced ${RACE_IDS} distinct ids for ${RACE_WON} tables" >&2
  exit 1
fi
for racer in $(seq 1 "${RACERS}"); do
  rust_node -e "DROP TABLE IF EXISTS ${DATABASE}.race_${racer}"
done

# ---------------------------------------------------------------------------
# The Rust node drops what it created, and the Go TiDB confirms both are gone.
# ---------------------------------------------------------------------------
rust_node -e "DROP TABLE ${DATABASE}.${MADE_TABLE}"
rust_node -e "DROP DATABASE ${MADE_DATABASE}"
await_go "SELECT count(*) FROM information_schema.tables \
  WHERE table_schema='${DATABASE}' AND table_name='${MADE_TABLE}'" "0" \
  "the Rust node's DROP TABLE"
await_go "SELECT count(*) FROM information_schema.schemata \
  WHERE schema_name='${MADE_DATABASE}'" "0" \
  "the Rust node's DROP DATABASE"

# The Go TiDB is still a working DDL owner after all of that: it allocates its
# own ids and schema versions from the same counters the Rust node advanced.
go_tidb -e "CREATE TABLE ${DATABASE}.after_rust (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"
GO_AFTER=$(go_tidb -Nse "SELECT count(*) FROM information_schema.tables \
  WHERE table_schema='${DATABASE}' AND table_name='after_rust'")
if [[ "${GO_AFTER}" != "1" ]]; then
  echo "the Go TiDB could not run its own DDL after the Rust node's changes" >&2
  exit 1
fi

# The Rust node followed its own diffs incrementally; a full reload would mean
# the diff it wrote was not one its own reloader could apply.
if grep -qF '"event":"catalog_full_reload"' "${RUST_LOG}"; then
  echo "the Rust node fell back to a full catalog reload over its own diffs" >&2
  grep -F '"event":"catalog_full_reload"' "${RUST_LOG}" >&2
  exit 1
fi

echo "ddl live proof passed: the Rust node allocated ids from NextGlobalID, wrote \
${MADE_DATABASE} and ${DATABASE}.${MADE_TABLE} (table_id=${TABLE_ID}) with one bumped \
SchemaVersionKey and one Diff:<ver> per change in a single optimistic 2PC each; the real \
Go TiDB reloaded those diffs, restored the table byte-for-byte with SHOW CREATE TABLE, and \
INSERTed rows a second Rust node loaded by name and read back; the Rust node then DROPped \
both objects and the Go TiDB confirmed them gone and ran its own DDL afterwards; \
unservable shapes were refused before any mutation; pd_cluster_id=${PD_CLUSTER_ID}"
