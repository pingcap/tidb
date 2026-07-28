#!/usr/bin/env bash
#
# Live proof that catalog changes are ANNOUNCED, not merely written.
#
# Both directions are measured against a real cluster whose reload ticks are
# deliberately far too slow to explain the result:
#
#   * the Go TiDB runs with its DEFAULT schema lease (45s, so its own reload
#     ticker fires every ~22s). It must see a table the Rust node created in
#     well under that, which is only possible if the Rust node PUT the new
#     schema version to `/tidb/ddl/global_schema_version`
#     (`OwnerUpdateGlobalVersion`'s key) and Go's own etcd watch fired;
#   * a second Rust node — the catalog-following `--cluster-session` shape,
#     which is the one that owns a reload thread — runs with a ten-minute
#     lease, so its reload thread ticks every five minutes. It must reload
#     within seconds of the Go TiDB's own CREATE TABLE, and its reload must
#     report `"trigger":"watch"`: its tick could not have run at all in that
#     window.
#
# The etcd surface is PD's own: PD embeds a real etcd server and answers
# `etcdserverpb.KV`/`etcdserverpb.Watch` on the very port the PD client
# already dials, so no extra process or address is involved.

set -euo pipefail

for prerequisite in tiup cargo curl jq nc pgrep awk sed seq grep openssl; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing ddl-notify prerequisite: ${prerequisite}" >&2
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
TAG="ddl-notify-${$}-$(date +%s)"
PORT_OFFSET=${DDL_NOTIFY_PORT_OFFSET:-43500}
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 45375 ]]; then
  echo "DDL_NOTIFY_PORT_OFFSET must be an unsigned integer no greater than 45375" >&2
  exit 1
fi
PD_PORT=$((2379 + PORT_OFFSET))
GO_SQL_PORT=$((4000 + PORT_OFFSET))
TIKV_SEED_PORT=$((20160 + PORT_OFFSET))
GO_STATUS_PORT=$((10080 + PORT_OFFSET))
RUST_SQL_PORT=$((12000 + PORT_OFFSET))
RUST_FOLLOWER_PORT=$((12001 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
RUST_SQL_ADDR="127.0.0.1:${RUST_SQL_PORT}"
RUST_FOLLOWER_ADDR="127.0.0.1:${RUST_FOLLOWER_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG_FILE="${TMPDIR:-/tmp}/${TAG}-rust.log"
FOLLOWER_LOG_FILE="${TMPDIR:-/tmp}/${TAG}-follower.log"
RUNTIME_DIR=
AUTH_FILE=
AUTH_USER=notify
AUTH_PASSWORD=${DDL_NOTIFY_AUTH_PASSWORD:-notify-native-password}
DATABASE=ddl_notify
ANCHOR_TABLE=anchor
RUST_TABLE=made_by_rust
GO_TABLE=made_by_go
PLAYGROUND_PID=
RUST_PID=
FOLLOWER_PID=
OWNED_PIDS=
STORE_ADDRESSES=

# The Rust node's own reload tick, chosen so it cannot fire inside a
# measurement window: ten minutes of lease is five minutes between passes.
RUST_LEASE_MS=600000
# Everything below this is "the notification worked"; the slowest tick that
# could compete is the Go server's ~22s, and the Rust node's is 300s.
PROMPT_LIMIT_TENTHS=50
POLL_TENTHS=1200

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

  if ! stop_rust_node "${FOLLOWER_PID}" "${RUST_FOLLOWER_PORT}"; then
    echo "ddl-notify cleanup failed: Rust follower ${RUST_FOLLOWER_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi
  if ! stop_rust_node "${RUST_PID}" "${RUST_SQL_PORT}"; then
    echo "ddl-notify cleanup failed: Rust node ${RUST_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  OWNED_PIDS=$(merge_owned_pids)
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "ddl-notify cleanup failed: tiup clean failed for ${TAG}" >&2
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
    echo "ddl-notify cleanup failed: owned process or TiUP registry row remains" >&2
    cleanup_failed=true
  fi

  local address
  for address in ${STORE_ADDRESSES}; do
    local port=${address##*:}
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "ddl-notify cleanup failed: TiKV ${address} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  for port in "${TIKV_SEED_PORT}" "${GO_SQL_PORT}" "${GO_STATUS_PORT}"; do
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "ddl-notify cleanup failed: port ${port} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "ddl-notify cleanup failed: PD ${PD_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
    if [[ -n "${RUNTIME_DIR}" ]]; then
      rm -rf -- "${RUNTIME_DIR}"
    fi
    if [[ -e "${TAG_DIR}" ]] || { [[ -n "${RUNTIME_DIR}" ]] && [[ -e "${RUNTIME_DIR}" ]]; }; then
      echo "ddl-notify cleanup failed: owned data directory remains" >&2
      cleanup_failed=true
    fi
  fi
  if [[ "${cleanup_failed}" == false ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG_FILE}" "${FOLLOWER_LOG_FILE}"
    echo "ddl-notify cleanup proof passed"
  else
    echo "ddl-notify retained logs: ${PLAYGROUND_LOG} ${RUST_LOG_FILE} ${FOLLOWER_LOG_FILE}" >&2
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

rust_follower() {
  "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${RUST_FOLLOWER_PORT}" \
    -u"${AUTH_USER}" -p"${AUTH_PASSWORD}" --connect-timeout=5 \
    "${MYSQL_PLUGIN_ARGS[@]}" "$@"
}

# Polls at 0.1s and reports how many tenths of a second it took, so the answer
# is a latency and not just a boolean.
await_tenths() {
  local probe=$1
  local elapsed=0
  while [[ "${elapsed}" -lt "${POLL_TENTHS}" ]]; do
    if eval "${probe}" >/dev/null 2>&1; then
      printf '%s\n' "${elapsed}"
      return 0
    fi
    sleep 0.1
    elapsed=$((elapsed + 1))
  done
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
  echo "ddl-notify Rust server is not executable: ${RUST_SERVER}" >&2
  exit 1
fi

for port in "${PD_PORT}" "${GO_SQL_PORT}" "${TIKV_SEED_PORT}" \
  "${GO_STATUS_PORT}" "${RUST_SQL_PORT}" "${RUST_FOLLOWER_PORT}"; do
  if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
    echo "refusing occupied ddl-notify port ${port}; set DDL_NOTIFY_PORT_OFFSET" >&2
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
  echo "could not derive the ddl-notify native-password stage-two hash" >&2
  exit 1
fi
(umask 077; printf '%s\t%s\t%s\t*%s\n' \
  "${AUTH_USER}" "127.0.0.1" "mysql_native_password" "${AUTH_HASH_HEX}" >"${AUTH_FILE}")
chmod 0600 "${AUTH_FILE}"
unset AUTH_HASH_HEX

# NO --db.config: the Go server keeps its DEFAULT 45s schema lease on purpose.
# Everything this script measures on the Go side has to beat that lease.
tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 1 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

ready=false
for _ in $(seq 1 240); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    echo "TiUP playground exited before readiness" >&2
    tail -160 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  STORE_ADDRESSES=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r '.stores[] | select(.store.state_name == "Up") | .store.address' 2>/dev/null) || true
  if [[ -n "${STORE_ADDRESSES}" ]] && go_tidb -Nse 'select 1' >/dev/null 2>&1; then
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

go_tidb <<SQL
DROP DATABASE IF EXISTS ${DATABASE};
CREATE DATABASE ${DATABASE};
CREATE TABLE ${DATABASE}.${ANCHOR_TABLE} (
  id BIGINT PRIMARY KEY CLUSTERED,
  v BIGINT NOT NULL
);
DROP USER IF EXISTS '${AUTH_USER}'@'%';
CREATE USER '${AUTH_USER}'@'%' IDENTIFIED WITH mysql_native_password BY '${AUTH_PASSWORD}';
GRANT ALL PRIVILEGES ON *.* TO '${AUTH_USER}'@'%';
FLUSH PRIVILEGES;
SQL

"${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
  --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
  --load-table "${DATABASE}.${ANCHOR_TABLE}" \
  --lease-ms "${RUST_LEASE_MS}" \
  --auth-file "${AUTH_FILE}" --max-connections 8 \
  >"${RUST_LOG_FILE}" 2>&1 &
RUST_PID=$!

READY_JSON=
for _ in $(seq 1 900); do
  if ! kill -0 "${RUST_PID}" 2>/dev/null; then
    echo "Rust SQL node exited before readiness" >&2
    tail -200 "${RUST_LOG_FILE}" >&2
    exit 1
  fi
  READY_JSON=$(grep -F '"event":"sql_node_ready"' "${RUST_LOG_FILE}" | tail -1 || true)
  if [[ -n "${READY_JSON}" ]]; then
    break
  fi
  sleep 0.1
done
if [[ -z "${READY_JSON}" ]]; then
  echo "Rust SQL node did not publish readiness" >&2
  tail -200 "${RUST_LOG_FILE}" >&2
  exit 1
fi
if grep -qF '"event":"schema_version_notifier_unavailable"' "${RUST_LOG_FILE}" \
  || grep -qF '"event":"schema_version_watch_unavailable"' "${RUST_LOG_FILE}"; then
  echo "the Rust node could not reach PD's embedded etcd; nothing below would prove anything" >&2
  grep -F 'unavailable' "${RUST_LOG_FILE}" >&2
  exit 1
fi

# The catalog-following node. `--load-table` serves one table from a snapshot
# it never revisits; `--cluster-session` is the shape that owns the reload
# thread the etcd watch nudges, so it is the one direction 2 measures. Its
# accounts come from the cluster's own `mysql.user`, written just above.
"${RUST_SERVER}" --path "${PD_ADDR}" \
  --port "${RUST_FOLLOWER_PORT}" \
  --cluster-session --load-privileges \
  --lease-ms "${RUST_LEASE_MS}" \
  >"${FOLLOWER_LOG_FILE}" 2>&1 &
FOLLOWER_PID=$!

FOLLOWER_READY=
for _ in $(seq 1 900); do
  if ! kill -0 "${FOLLOWER_PID}" 2>/dev/null; then
    echo "Rust follower node exited before readiness" >&2
    tail -200 "${FOLLOWER_LOG_FILE}" >&2
    exit 1
  fi
  FOLLOWER_READY=$(grep -F '"event":"cluster_session_node_ready"' "${FOLLOWER_LOG_FILE}" | tail -1 || true)
  if [[ -n "${FOLLOWER_READY}" ]]; then
    break
  fi
  sleep 0.1
done
if [[ -z "${FOLLOWER_READY}" ]]; then
  echo "Rust follower node did not publish readiness" >&2
  tail -200 "${FOLLOWER_LOG_FILE}" >&2
  exit 1
fi
if grep -qF '"event":"schema_version_watch_unavailable"' "${FOLLOWER_LOG_FILE}"; then
  echo "the Rust follower could not watch PD's embedded etcd" >&2
  grep -F 'unavailable' "${FOLLOWER_LOG_FILE}" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Direction 1: our DDL -> the Go TiDB notices, far inside its own 45s lease.
# ---------------------------------------------------------------------------
rust_node -e "CREATE TABLE ${DATABASE}.${RUST_TABLE} (
  id BIGINT PRIMARY KEY CLUSTERED,
  v BIGINT NOT NULL
)"
if ! grep -qF '"event":"schema_version_notified"' "${RUST_LOG_FILE}"; then
  echo "the Rust node committed its catalog change without announcing the version" >&2
  tail -60 "${RUST_LOG_FILE}" >&2
  exit 1
fi

GO_NOTICED_TENTHS=$(await_tenths "[[ \$(${MYSQL_CLIENT} --protocol=tcp -h 127.0.0.1 \
  -P ${GO_SQL_PORT} -uroot --connect-timeout=5 ${MYSQL_PLUGIN_ARGS[*]} -Nse \
  \"SELECT count(*) FROM information_schema.tables WHERE table_schema='${DATABASE}' \
  AND table_name='${RUST_TABLE}'\") == 1 ]]") || {
  echo "the Go TiDB never observed the Rust node's CREATE TABLE" >&2
  exit 1
}
if [[ "${GO_NOTICED_TENTHS}" -gt "${PROMPT_LIMIT_TENTHS}" ]]; then
  echo "the Go TiDB took ${GO_NOTICED_TENTHS} tenths of a second, which its own \
lease tick alone could explain; the notification did not work" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Direction 2: the Go TiDB's DDL -> our watch fires, 300s before our own tick.
# ---------------------------------------------------------------------------
RELOADS_BEFORE=$(grep -cF '"event":"catalog_reloaded"' "${FOLLOWER_LOG_FILE}" || true)
go_tidb -e "CREATE TABLE ${DATABASE}.${GO_TABLE} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"

RUST_NOTICED_TENTHS=$(await_tenths \
  "[[ \$(grep -cF '\"event\":\"catalog_reloaded\"' '${FOLLOWER_LOG_FILE}') -gt ${RELOADS_BEFORE} ]]") || {
  echo "the Rust follower never reloaded after the Go TiDB's CREATE TABLE" >&2
  tail -60 "${FOLLOWER_LOG_FILE}" >&2
  exit 1
}
if [[ "${RUST_NOTICED_TENTHS}" -gt "${PROMPT_LIMIT_TENTHS}" ]]; then
  echo "the Rust follower took ${RUST_NOTICED_TENTHS} tenths of a second to reload, \
which is not promptly enough to have come from the watch" >&2
  exit 1
fi
LAST_RELOAD=$(grep -F '"event":"catalog_reloaded"' "${FOLLOWER_LOG_FILE}" | tail -1)
if ! printf '%s' "${LAST_RELOAD}" | grep -qF '"trigger":"watch"'; then
  echo "the reload was not attributed to the etcd watch: ${LAST_RELOAD}" >&2
  exit 1
fi
if ! grep -qF '"event":"schema_version_watch_fired"' "${FOLLOWER_LOG_FILE}"; then
  echo "the Rust follower reloaded without its watch reporting an event" >&2
  exit 1
fi

# The reload was a real one: the follower now serves a catalog containing the
# table the Go TiDB created, at a schema version no tick of its own could have
# fetched.
GO_TABLE_SEEN=$(rust_follower -Nse \
  "SELECT count(*) FROM information_schema.tables WHERE table_schema='${DATABASE}' \
   AND table_name='${GO_TABLE}'" 2>/dev/null || true)
if [[ "${GO_TABLE_SEEN}" != "1" ]]; then
  echo "the Rust follower reloaded but does not serve the Go TiDB's new table: \
'${GO_TABLE_SEEN}'" >&2
  exit 1
fi

echo "ddl-notify live proof passed: the Rust node PUT its committed schema version to \
/tidb/ddl/global_schema_version and a DEFAULT-lease (45s) Go TiDB served the new table \
after ${GO_NOTICED_TENTHS} tenths of a second; in reverse, the Go TiDB's own CREATE TABLE \
woke the Rust follower's etcd watch and produced a \"trigger\":\"watch\" reload after \
${RUST_NOTICED_TENTHS} tenths of a second, against a ${RUST_LEASE_MS}ms lease whose tick \
is 300 seconds away"
