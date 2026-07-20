#!/usr/bin/env bash

# A deliberately small live proof for the configured two-relation read path.
# It uses a stock MySQL client against one authenticated Rust connection, while
# Go TiDB is only the fixture writer and TiUP topology owner. The Rust node
# must read both configured tables directly from TiKV at one PD timestamp.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RUST_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
TAG="campaign25-multi-relation-${$}-$(date +%s)"
PORT_OFFSET=${C25_PORT_OFFSET:-44000}
PD_PORT=$((2379 + PORT_OFFSET))
GO_PORT=$((4000 + PORT_OFFSET))
GO_STATUS_PORT=$((10080 + PORT_OFFSET))
RUST_PORT=$((12000 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
GO_SERVER=${C25_GO_TIDB_SERVER:-}
GO_SERVER_WRAPPER="${SCRIPT_DIR}/launch-campaign25-failpoint-tidb.sh"
RUST_SERVER=${C25_RUST_SERVER:-"${RUST_ROOT}/target/debug/tidb-server"}
MYSQL_CLIENT=${C25_MYSQL_CLIENT:-mysql}
MYSQL_PLUGIN_ARGS=()
RUNTIME_DIR=
PLAYGROUND_PID=
RUST_PID=
RUST_PID_AT_START=
AUTH_FILE=
RUST_LOG=
PLAYGROUND_LOG=
SERVER_READY_ATTEMPTS=${C25_SERVER_READY_ATTEMPTS:-600}
PERSISTENT_CLIENT_PID=
PERSISTENT_CLIENT_FIFO=
PERSISTENT_CLIENT_OUTPUT=
PERSISTENT_CLIENT_ERROR=
PERSISTENT_CLIENT_FD_OPEN=false
PERSISTENT_CONNECTION_ID=
PERSISTENT_SESSION_ID=
PHASE_SNAPSHOT=
PHASE_RESULT=
AUTHENTICATED_QUERY_COUNT=7
STOPPED_TIKV_PIDS=()
HELPER_ID=

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Campaign 25 missing prerequisite: $1" >&2
    exit 1
  }
}

cleanup() {
  local status=$?
  local deadline
  local owned
  trap - EXIT INT TERM
  for stopped_pid in "${STOPPED_TIKV_PIDS[@]-}"; do
    kill -CONT "${stopped_pid}" 2>/dev/null || true
  done
  STOPPED_TIKV_PIDS=()
  if [[ "${PERSISTENT_CLIENT_FD_OPEN}" == true ]]; then
    exec 9>&-
    PERSISTENT_CLIENT_FD_OPEN=false
  fi
  if [[ -n "${PERSISTENT_CLIENT_PID}" ]] && kill -0 "${PERSISTENT_CLIENT_PID}" 2>/dev/null; then
    kill -TERM "${PERSISTENT_CLIENT_PID}" 2>/dev/null || true
    wait "${PERSISTENT_CLIENT_PID}" 2>/dev/null || true
  fi
  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    kill -TERM "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill -TERM "${PLAYGROUND_PID}" 2>/dev/null || true
    deadline=$(( $(date +%s) + 30 ))
    while kill -0 "${PLAYGROUND_PID}" 2>/dev/null && [[ $(date +%s) -lt "${deadline}" ]]; do
      sleep 1
    done
  fi
  # TiUP's interactive supervisor can outlive its parent after a failed
  # startup. Contain only command lines tagged by this invocation.
  owned=$(pgrep -f "${TAG}" || true)
  if [[ -n "${owned}" ]]; then
    kill -TERM ${owned} 2>/dev/null || true
    deadline=$(( $(date +%s) + 30 ))
    while [[ $(date +%s) -lt "${deadline}" ]]; do
      owned=$(pgrep -f "${TAG}" || true)
      [[ -z "${owned}" ]] && break
      sleep 1
    done
  fi
  owned=$(pgrep -f "${TAG}" || true)
  if [[ -n "${owned}" ]]; then
    echo "Campaign 25 cleanup could not stop tag-owned processes: ${owned}" >&2
    status=1
  fi
  tiup clean "${TAG}" --all >/dev/null 2>&1 || true
  if tiup status 2>/dev/null | grep -F "${TAG}" >/dev/null; then
    echo "Campaign 25 cleanup left a TiUP deployment for ${TAG}" >&2
    status=1
  fi
  for port in "${PD_PORT}" "${GO_PORT}" "${GO_STATUS_PORT}" "${RUST_PORT}"; do
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "Campaign 25 cleanup left listener port ${port} occupied" >&2
      status=1
    fi
  done
  if [[ -n "${RUNTIME_DIR}" && -d "${RUNTIME_DIR}" ]]; then
    if [[ "${status}" -eq 0 ]]; then
      rm -rf -- "${RUNTIME_DIR}"
    else
      echo "Campaign 25 retained diagnostics: ${RUNTIME_DIR}" >&2
    fi
  fi
  exit "${status}"
}

mysql_go() {
  # The playground bootstrap account is deliberately passwordless.  Do not
  # inherit MYSQL_PWD from the desktop environment: mysql would otherwise
  # attempt password authentication and reject the fixture connection.
  env -u MYSQL_PWD "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_PORT}" -uroot \
    --skip-password --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" "$@"
}

transfer_pd_leader() {
  local region_id=$1
  local target_store=$2
  local current_leader=0
  for _ in $(seq 1 120); do
    tiup ctl:v8.5.6 pd -u "http://${PD_ADDR}" operator add transfer-leader \
      "${region_id}" "${target_store}" >/dev/null 2>&1 || true
    for _ in $(seq 1 20); do
      current_leader=$(curl -sf --max-time 2 \
        "http://${PD_ADDR}/pd/api/v1/region/id/${region_id}" \
        | jq -r '.leader.store_id // 0' 2>/dev/null || true)
      [[ "${current_leader}" == "${target_store}" ]] && return
      sleep 0.1
    done
    sleep 0.25
  done
  echo "Campaign 25 region ${region_id} did not transfer leadership to store ${target_store}" >&2
  return 1
}

region_leader_address() {
  local region_json=$1
  local store_id
  store_id=$(printf '%s\n' "${region_json}" | jq -r '.leader.store_id // 0')
  [[ "${store_id}" =~ ^[1-9][0-9]*$ ]] || {
    echo "Campaign 25 region did not name a leader store" >&2
    return 1
  }
  curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r --argjson store "${store_id}" \
      '.stores[] | select(.store.id == $store) | .store.address' \
    | head -1
}

publication_for_query_table() {
  local query_id=$1
  local table_id=$2
  grep -F '"event":"query_multi_transport_published"' "${RUST_LOG}" \
    | jq -c --argjson query "${query_id}" --argjson table "${table_id}" \
      'select(.query_id == $query and .table_id == $table)' \
    | tail -1
}

transport_for_query_table() {
  local query_id=$1
  local table_id=$2
  grep -F '"event":"query_multi_transport"' "${RUST_LOG}" \
    | jq -c --argjson query "${query_id}" --argjson table "${table_id}" \
      'select(.query_id == $query and .table_id == $table)' \
    | tail -1
}

desired_store_ids_json() {
  jq -cn '$ARGS.positional | map(tonumber) | sort' --args "$@"
}

region_has_placement() {
  local region_json=$1
  local leader_store=$2
  shift 2
  local expected_voters
  expected_voters=$(desired_store_ids_json "$@")
  printf '%s\n' "${region_json}" | jq -e \
    --argjson leader "${leader_store}" --argjson voters "${expected_voters}" \
    '(.leader.store_id == $leader)
     and ([.peers[] | select(.role_name == "Voter") | .store_id] | sort == $voters)
     and (.pending_peers | length == 0)
     and (.down_peers | length == 0)' >/dev/null
}

ensure_region_placement() {
  local region_id=$1
  local leader_store=$2
  shift 2
  [[ $# -eq 3 && "$1" == "${leader_store}" ]] || {
    echo "Campaign 25 placement requires the leader plus exactly two voter stores" >&2
    return 1
  }
  local region_json
  local deadline=$(( $(date +%s) + 120 ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    region_json=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/region/id/${region_id}" || true)
    if [[ -n "${region_json}" ]] && region_has_placement "${region_json}" "${leader_store}" "$@"; then
      return
    fi
    tiup ctl:v8.5.6 pd -u "http://${PD_ADDR}" operator add transfer-region \
      "${region_id}" "${leader_store}" leader \
      "$2" voter "$3" voter >/dev/null 2>&1 || true
    sleep 1
  done
  echo "Campaign 25 region ${region_id} did not reach voter placement [$*] with leader ${leader_store}" >&2
  printf '%s\n' "${region_json:-<no PD region response>}" >&2
  return 1
}

table_record_region_id() {
  local table_name=$1
  local table_id=$2
  local handle=$3
  local record_key="t_${table_id}_r_${handle}"
  mysql_go -Nse "SHOW TABLE campaign25.${table_name} REGIONS" \
    | awk -F '\t' -v key="${record_key}" '$2 == key { print $1; exit }'
}

store_address() {
  local store_id=$1
  curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r --argjson store "${store_id}" \
      '.stores[] | select(.store.id == $store) | .store.address' \
    | head -1
}

freeze_tag_owned_store() {
  local store_id=$1
  local address
  local port
  local pid
  address=$(store_address "${store_id}")
  port=${address##*:}
  pid=$(lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t | head -1 || true)
  [[ -n "${pid}" ]] && ps -ww -p "${pid}" -o command= | grep -F "${TAG}" >/dev/null \
    && ps -ww -p "${pid}" -o command= | grep -F tikv-server >/dev/null || {
      echo "Campaign 25 refuses to freeze a non-tag-owned TiKV store ${store_id}" >&2
      return 1
    }
  kill -STOP "${pid}"
  STOPPED_TIKV_PIDS+=("${pid}")
}

line_count() {
  awk 'END { print NR + 0 }' "$1"
}

start_persistent_rust_client() {
  PERSISTENT_CLIENT_FIFO="${RUNTIME_DIR}/persistent-rust-client.fifo"
  PERSISTENT_CLIENT_OUTPUT="${RUNTIME_DIR}/persistent-rust-client.out"
  PERSISTENT_CLIENT_ERROR="${RUNTIME_DIR}/persistent-rust-client.err"
  mkfifo "${PERSISTENT_CLIENT_FIFO}"
  exec 9<>"${PERSISTENT_CLIENT_FIFO}"
  PERSISTENT_CLIENT_FD_OPEN=true
  (
    exec 9>&-
    export MYSQL_PWD="${AUTH_PASSWORD}"
    export MARIADB_PWD="${AUTH_PASSWORD}"
    exec "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${RUST_PORT}" \
      -u"${AUTH_USER}" --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" \
      --unbuffered --batch --skip-column-names <"${PERSISTENT_CLIENT_FIFO}" \
      >"${PERSISTENT_CLIENT_OUTPUT}" 2>"${PERSISTENT_CLIENT_ERROR}"
  ) &
  PERSISTENT_CLIENT_PID=$!
  for _ in $(seq 1 300); do
    local begin
    begin=$(grep -F '"event":"connection_begin"' "${RUST_LOG}" | tail -1 || true)
    if kill -0 "${PERSISTENT_CLIENT_PID}" 2>/dev/null && [[ -n "${begin}" ]] \
      && printf '%s\n' "${begin}" | jq -e '.active == 1 and .accepted == 1' >/dev/null; then
      return
    fi
    sleep 0.1
  done
  echo "Campaign 25 persistent authenticated stock client was not admitted" >&2
  sed -n '1,160p' "${PERSISTENT_CLIENT_ERROR}" >&2
  return 1
}

run_persistent_query() {
  local phase=$1
  local query=$2
  local expected=$3
  local before_output before_errors before_snapshots before_transports expected_lines deadline
  before_output=$(line_count "${PERSISTENT_CLIENT_OUTPUT}")
  before_errors=$(line_count "${PERSISTENT_CLIENT_ERROR}")
  before_snapshots=$(grep -c -F '"event":"query_multi_snapshot"' "${RUST_LOG}" || true)
  before_transports=$(grep -c -F '"event":"query_multi_transport"' "${RUST_LOG}" || true)
  expected_lines=$(printf '%s\n' "${expected}" | sed '/^[[:space:]]*$/d' | awk 'END { print NR + 0 }')
  kill -0 "${PERSISTENT_CLIENT_PID}" 2>/dev/null || {
    echo "Campaign 25 persistent stock client exited before ${phase}" >&2
    return 1
  }
  printf '%s\n' "${query}" >&9
  deadline=$(( $(date +%s) + 30 ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    local current_output current_errors current_snapshots current_transports
    current_output=$(line_count "${PERSISTENT_CLIENT_OUTPUT}")
    current_errors=$(line_count "${PERSISTENT_CLIENT_ERROR}")
    current_snapshots=$(grep -c -F '"event":"query_multi_snapshot"' "${RUST_LOG}" || true)
    current_transports=$(grep -c -F '"event":"query_multi_transport"' "${RUST_LOG}" || true)
    if [[ "${current_errors}" -gt "${before_errors}" ]] || ! kill -0 "${PERSISTENT_CLIENT_PID}" 2>/dev/null; then
      break
    fi
    if [[ "${current_output}" -eq $((before_output + expected_lines)) \
      && "${current_snapshots}" -ge $((before_snapshots + 1)) \
      && "${current_transports}" -ge $((before_transports + 2)) ]]; then
      break
    fi
    sleep 0.05
  done
  local actual_output current_output current_errors current_snapshots current_transports
  current_output=$(line_count "${PERSISTENT_CLIENT_OUTPUT}")
  current_errors=$(line_count "${PERSISTENT_CLIENT_ERROR}")
  current_snapshots=$(grep -c -F '"event":"query_multi_snapshot"' "${RUST_LOG}" || true)
  current_transports=$(grep -c -F '"event":"query_multi_transport"' "${RUST_LOG}" || true)
  if [[ "${current_output}" -ne $((before_output + expected_lines)) \
    || "${current_errors}" -ne "${before_errors}" \
    || "${current_snapshots}" -lt $((before_snapshots + 1)) \
    || "${current_transports}" -lt $((before_transports + 2)) ]]; then
    echo "Campaign 25 ${phase} persistent stock-client query did not complete" >&2
    tail -40 "${PERSISTENT_CLIENT_OUTPUT}" >&2
    sed -n '1,160p' "${PERSISTENT_CLIENT_ERROR}" >&2
    return 1
  fi
  if [[ "${expected_lines}" -eq 0 ]]; then
    actual_output=
  else
    actual_output=$(sed -n "$((before_output + 1)),$((before_output + expected_lines))p" "${PERSISTENT_CLIENT_OUTPUT}")
  fi
  [[ "${actual_output}" == "${expected}" ]] || {
    echo "Campaign 25 ${phase} output mismatch: ${actual_output@Q}" >&2
    return 1
  }
  PHASE_RESULT=${actual_output}
  PHASE_SNAPSHOT=$(grep -F '"event":"query_multi_snapshot"' "${RUST_LOG}" | tail -1)
  local connection_id session_id
  connection_id=$(printf '%s\n' "${PHASE_SNAPSHOT}" | jq -r '.connection_id // 0')
  session_id=$(printf '%s\n' "${PHASE_SNAPSHOT}" | jq -r '.session_id // 0')
  [[ "${connection_id}" =~ ^[1-9][0-9]*$ && "${session_id}" =~ ^[1-9][0-9]*$ ]] || {
    echo "Campaign 25 ${phase} omitted persistent connection identity" >&2
    return 1
  }
  if [[ -z "${PERSISTENT_CONNECTION_ID}" ]]; then
    PERSISTENT_CONNECTION_ID=${connection_id}
    PERSISTENT_SESSION_ID=${session_id}
  elif [[ "${connection_id}" != "${PERSISTENT_CONNECTION_ID}" \
    || "${session_id}" != "${PERSISTENT_SESSION_ID}" ]]; then
    echo "Campaign 25 ${phase} did not remain on the authenticated session" >&2
    return 1
  fi
}

hash_native_password() {
  printf '%s' "$1" | openssl dgst -sha1 -binary | openssl dgst -sha1 -hex \
    | awk '{ print toupper($NF) }'
}

if [[ "${1:-}" == "--self-test" ]]; then
  [[ $((2379 + 44000)) == 46379 ]]
  [[ $((4000 + 44000)) == 48000 ]]
  [[ $((12000 + 44000)) == 56000 ]]
  [[ $(hash_native_password password) =~ ^[0-9A-F]{40}$ ]]
  echo "Campaign 25 multi-relation smoke harness self-test passed"
  exit 0
fi

for tool in tiup cargo curl jq nc openssl awk grep mktemp lsof perl; do
  require "${tool}"
done
require "${MYSQL_CLIENT}"
plugin_dir=${C25_MYSQL_PLUGIN_DIR:-}
if [[ -z "${plugin_dir}" ]]; then
  for candidate in /opt/homebrew/opt/mysql-client/lib/plugin /usr/local/opt/mysql-client/lib/plugin; do
    if [[ -f "${candidate}/mysql_native_password.so" ]]; then
      plugin_dir=${candidate}
      break
    fi
  done
fi
[[ -f "${plugin_dir}/mysql_native_password.so" ]] || {
  echo "C25_MYSQL_PLUGIN_DIR must contain mysql_native_password.so" >&2
  exit 1
}
MYSQL_PLUGIN_ARGS=(--plugin-dir="${plugin_dir}")
[[ "${PORT_OFFSET}" =~ ^[0-9]+$ && "${PORT_OFFSET}" -le 44375 ]] || {
  echo "C25_PORT_OFFSET must be an integer no greater than 44375" >&2
  exit 1
}
[[ "${SERVER_READY_ATTEMPTS}" =~ ^[1-9][0-9]*$ ]] || {
  echo "C25_SERVER_READY_ATTEMPTS must be a positive integer" >&2
  exit 1
}
[[ -n "${GO_SERVER}" && -x "${GO_SERVER}" ]] || {
  echo "C25_GO_TIDB_SERVER must name a TiDB v8.5.6 fixture binary" >&2
  exit 1
}
if [[ "${C25_ENABLE_BLOCKED_SHUTDOWN:-false}" == true ]]; then
  for marker in enableTestAPI beforeCommitSecondaries prewriteSecondary; do
    LC_ALL=C grep -a -q "${marker}" "${GO_SERVER}" || {
      echo "C25_GO_TIDB_SERVER is missing required ${marker} failpoint marker" >&2
      exit 1
    }
  done
  TIKV_COUNT=5
else
  TIKV_COUNT=3
fi
for port in "${PD_PORT}" "${GO_PORT}" "${RUST_PORT}"; do
  ! nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1 || {
    echo "Campaign 25 refuses occupied port ${port}" >&2
    exit 1
  }
done

trap cleanup EXIT INT TERM
RUNTIME_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}.XXXXXX")
AUTH_FILE="${RUNTIME_DIR}/auth.tsv"
RUST_LOG="${RUNTIME_DIR}/rust.log"
PLAYGROUND_LOG="${RUNTIME_DIR}/playground.log"
AUTH_USER=campaign25
AUTH_PASSWORD=${C25_AUTH_PASSWORD:-campaign25-native-password}
AUTH_HASH=$(hash_native_password "${AUTH_PASSWORD}")
(umask 077; printf '%s\t127.0.0.1\tmysql_native_password\t*%s\n' \
  "${AUTH_USER}" "${AUTH_HASH}" >"${AUTH_FILE}")
unset AUTH_HASH

# A three-PD quorum must become serving before TiKV starts. On this macOS
# runner, starting all six processes together can make every TiKV exit during
# initial TSO bootstrap. This retains the required final 3-PD/3-TiKV topology;
# it only makes the dependency order explicit.
tiup playground v8.5.6 --without-monitor --tag "${TAG}" --db 0 --pd 3 --kv 0 \
  --tiflash 0 --port-offset "${PORT_OFFSET}" \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

ready=false
for _ in $(seq 1 240); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    tail -120 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  members=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/members" 2>/dev/null || true)
  member_count=$(printf '%s\n' "${members}" | jq -r '.members | length' 2>/dev/null || true)
  if [[ "${member_count:-0}" == 3 ]]; then
    ready=true
    break
  fi
  sleep 1
done
[[ "${ready}" == true ]] || {
  echo "Campaign 25 three-PD quorum did not become ready" >&2
  exit 1
}
tiup playground scale-out --tag "${TAG}" --kv "${TIKV_COUNT}" >>"${PLAYGROUND_LOG}" 2>&1
ready=false
for _ in $(seq 1 240); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    tail -120 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  stores=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" 2>/dev/null || true)
  store_count=$(printf '%s\n' "${stores}" | jq -r '[.stores[] | select(.store.state_name == "Up")] | length' 2>/dev/null || true)
  if [[ "${store_count:-0}" == "${TIKV_COUNT}" ]]; then
    ready=true
    break
  fi
  sleep 1
done
[[ "${ready}" == true ]] || {
  echo "Campaign 25 ${TIKV_COUNT} TiKV nodes did not become ready" >&2
  exit 1
}
GO_SERVER_BINPATH="${GO_SERVER}"
if [[ "${C25_ENABLE_BLOCKED_SHUTDOWN:-false}" == true ]]; then
  GO_SERVER_BINPATH="${GO_SERVER_WRAPPER}"
fi
tiup playground scale-out --tag "${TAG}" --db 1 --db.binpath "${GO_SERVER_BINPATH}" \
  >>"${PLAYGROUND_LOG}" 2>&1
ready=false
for _ in $(seq 1 240); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    tail -120 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  if mysql_go -Nse 'select 1' >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
[[ "${ready}" == true ]] || {
  echo "Campaign 25 Go TiDB fixture did not become ready" >&2
  exit 1
}
if [[ "${C25_ENABLE_BLOCKED_SHUTDOWN:-false}" == true ]]; then
  active_failpoints=$(curl -sf --max-time 2 "http://127.0.0.1:${GO_STATUS_PORT}/fail/" || true)
  [[ "${active_failpoints}" == *'github.com/pingcap/tidb/pkg/server/enableTestAPI=return'* ]] || {
    echo "Campaign 25 Go TiDB fixture did not activate the failpoint test API" >&2
    exit 1
  }
fi
mysql_go <<'SQL'
DROP DATABASE IF EXISTS campaign25;
CREATE DATABASE campaign25;
SQL
if [[ "${C25_ENABLE_BLOCKED_SHUTDOWN:-false}" == true ]]; then
  # client-go chooses the first encoded mutation key as the pessimistic
  # transaction primary.  Create this helper before the read tables so its
  # record key sorts first and the left-row mutation is a real secondary.
  mysql_go <<'SQL'
CREATE TABLE campaign25.lock_secondary (
  id BIGINT PRIMARY KEY CLUSTERED,
  value BIGINT NOT NULL
);
INSERT INTO campaign25.lock_secondary VALUES (1, 1);
SQL
fi
mysql_go <<'SQL'
CREATE TABLE campaign25.left_rows (
  id BIGINT PRIMARY KEY CLUSTERED,
  join_key BIGINT NOT NULL,
  payload BIGINT NOT NULL
);
CREATE TABLE campaign25.right_rows (
  id BIGINT PRIMARY KEY CLUSTERED,
  join_key BIGINT NOT NULL,
  payload BIGINT NOT NULL
);
INSERT INTO campaign25.left_rows VALUES (-7, 10, 700), (0, 20, 800), (42, 30, 900);
INSERT INTO campaign25.right_rows VALUES (1, 10, 1000), (2, 30, 3000), (3, 40, 4000);
SQL
if [[ "${C25_ENABLE_BLOCKED_SHUTDOWN:-false}" == true ]]; then
  mysql_go <<'SQL'
SET SESSION tidb_wait_split_region_finish = 1;
SPLIT TABLE campaign25.left_rows BY (-7);
SPLIT TABLE campaign25.lock_secondary BY (1);
SQL
fi
LEFT_ID=$(mysql_go -Nse "select tidb_table_id from information_schema.tables where table_schema='campaign25' and table_name='left_rows'")
RIGHT_ID=$(mysql_go -Nse "select tidb_table_id from information_schema.tables where table_schema='campaign25' and table_name='right_rows'")
[[ "${LEFT_ID}" =~ ^[1-9][0-9]*$ && "${RIGHT_ID}" =~ ^[1-9][0-9]*$ && "${LEFT_ID}" != "${RIGHT_ID}" ]] || {
  echo "Campaign 25 fixture did not expose two physical table IDs" >&2
  exit 1
}
if [[ "${C25_ENABLE_BLOCKED_SHUTDOWN:-false}" == true ]]; then
  HELPER_ID=$(mysql_go -Nse "select tidb_table_id from information_schema.tables where table_schema='campaign25' and table_name='lock_secondary'")
  [[ "${HELPER_ID}" =~ ^[1-9][0-9]*$ && "${HELPER_ID}" -lt "${LEFT_ID}" ]] || {
    echo "Campaign 25 helper key cannot be the pessimistic transaction primary" >&2
    exit 1
  }
fi

cd "${RUST_ROOT}"
if [[ ! -x "${RUST_SERVER}" ]]; then
  CARGO_BUILD_JOBS=12 cargo build --offline --locked -j12 -p tidb-server --bin tidb-server
  RUST_SERVER="${RUST_ROOT}/target/debug/tidb-server"
fi
"${RUST_SERVER}" --path "${PD_ADDR}" --store tikv --host 127.0.0.1 --port "${RUST_PORT}" \
  --read-table campaign25 left_rows "${LEFT_ID}" 3 \
  id:1:clustered-pk join_key:2:stored-not-null payload:3:stored-not-null \
  --read-table campaign25 right_rows "${RIGHT_ID}" 3 \
  id:1:clustered-pk join_key:2:stored-not-null payload:3:stored-not-null \
  --auth-file "${AUTH_FILE}" --max-connections 1 --connection-timeout-ms 120000 \
  >"${RUST_LOG}" 2>&1 &
RUST_PID=$!
RUST_PID_AT_START=${RUST_PID}

for _ in $(seq 1 "${SERVER_READY_ATTEMPTS}"); do
  if ! kill -0 "${RUST_PID}" 2>/dev/null; then
    tail -160 "${RUST_LOG}" >&2
    exit 1
  fi
  grep -q '"event":"sql_node_ready"' "${RUST_LOG}" && break
  sleep 0.1
done
ready_json=$(grep -F '"event":"sql_node_ready"' "${RUST_LOG}" | tail -1)
printf '%s\n' "${ready_json}" | jq -e \
  --argjson left "${LEFT_ID}" --argjson right "${RIGHT_ID}" \
  '.tables | length == 2 and .[0].table_id == $left and .[1].table_id == $right' >/dev/null
shutdown_grace_ms=$(printf '%s\n' "${ready_json}" | jq -r '.shutdown_grace_ms // -1')
[[ "${shutdown_grace_ms}" =~ ^[0-9]+$ ]] || {
  echo "Campaign 25 Rust node omitted shutdown grace" >&2
  exit 1
}

start_persistent_rust_client
run_persistent_query on \
  'SELECT l.id, l.payload, r.payload FROM campaign25.left_rows AS l INNER JOIN campaign25.right_rows AS r ON l.join_key = r.join_key WHERE l.id >= -7 AND r.payload != 4000;' \
  $'-7\t700\t1000\n42\t900\t3000'
printf '%s\n' "${PHASE_SNAPSHOT}" | jq -e \
  '.join_equality == {"left_full_offset": 1, "right_full_offset": 4}
   and .relations[0].predicate_count == 0
   and .relations[1].predicate_count == 1' >/dev/null || {
  echo "Campaign 25 ON query omitted the bound FullSchema join keys or residual Selection" >&2
  printf '%s\n' "${PHASE_SNAPSHOT}" >&2
  exit 1
}
result=${PHASE_RESULT}
RUST_PID_BEFORE_CHURN=${RUST_PID}
on_query_id=$(printf '%s\n' "${PHASE_SNAPSHOT}" | jq -r '.query_id // 0')
left_publication=$(publication_for_query_table "${on_query_id}" "${LEFT_ID}")
left_region_id=$(printf '%s\n' "${left_publication}" | jq -r '.region_id // 0')
right_publication=$(publication_for_query_table "${on_query_id}" "${RIGHT_ID}")
right_region_id=$(printf '%s\n' "${right_publication}" | jq -r '.region_id // 0')
[[ "${left_region_id}" =~ ^[1-9][0-9]*$ ]] || {
  echo "Campaign 25 on-query did not publish a left-table TiKV region" >&2
  printf '%s\n' "${left_publication}" >&2
  exit 1
}
[[ "${right_region_id}" =~ ^[1-9][0-9]*$ ]] || {
  echo "Campaign 25 ON query did not publish a right-table TiKV region" >&2
  printf '%s\n' "${right_publication}" >&2
  exit 1
}
left_region=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/region/id/${left_region_id}")
leader_address=$(region_leader_address "${left_region}")
[[ -n "${leader_address}" \
  && $(printf '%s\n' "${left_publication}" | jq -r '.physical_address // empty') == "${leader_address}" ]] || {
  echo "Campaign 25 initial left-table dispatch did not reach PD's leader" >&2
  printf '%s\n%s\n' "${left_region}" "${left_publication}" >&2
  exit 1
}
leader_store=$(printf '%s\n' "${left_region}" | jq -r '.leader.store_id // 0')
next_store=$(printf '%s\n' "${left_region}" | jq -r --argjson leader "${leader_store}" \
  '.peers[] | select(.store_id != $leader and .role_name == "Voter") | .store_id' | head -1)
[[ "${left_region_id}" =~ ^[1-9][0-9]*$ && "${next_store}" =~ ^[1-9][0-9]*$ ]] || {
  echo "Campaign 25 left table did not expose a transferable voter region" >&2
  exit 1
}
transfer_pd_leader "${left_region_id}" "${next_store}"
[[ -n "${RUST_PID_BEFORE_CHURN}" ]] && kill -0 "${RUST_PID_BEFORE_CHURN}" 2>/dev/null || {
  echo "Campaign 25 PD leader transfer did not retain the Rust SQL node" >&2
  exit 1
}
run_persistent_query post_churn \
  'SELECT l.id, l.payload, r.payload FROM campaign25.left_rows AS l INNER JOIN campaign25.right_rows AS r ON l.join_key = r.join_key WHERE l.id >= -7 AND r.payload != 4000;' \
  "${result}"
post_churn_result=${PHASE_RESULT}
[[ "${post_churn_result}" == "${result}" ]] || {
  echo "Campaign 25 post-churn join output mismatch: ${post_churn_result@Q}" >&2
  exit 1
}
post_churn_query_id=$(printf '%s\n' "${PHASE_SNAPSHOT}" | jq -r '.query_id // 0')
post_churn_publication=$(publication_for_query_table "${post_churn_query_id}" "${LEFT_ID}")
post_churn_region_id=$(printf '%s\n' "${post_churn_publication}" | jq -r '.region_id // 0')
post_churn_transport=$(transport_for_query_table "${post_churn_query_id}" "${LEFT_ID}")
post_churn_leader=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/region/id/${left_region_id}")
post_churn_leader_address=$(region_leader_address "${post_churn_leader}")
[[ "${post_churn_region_id}" == "${left_region_id}" \
  && $(printf '%s\n' "${post_churn_leader}" | jq -r '.leader.store_id // 0') == "${next_store}" \
  && $(printf '%s\n' "${post_churn_transport}" | jq -r '.unary_attempts // 0') -ge 1 ]] || {
  echo "Campaign 25 post-transfer read did not recover from the stale leader" >&2
  printf '%s\n%s\n%s\n' "${post_churn_leader}" "${post_churn_publication}" "${post_churn_transport}" >&2
  exit 1
}
run_persistent_query post_churn_revalidated \
  'SELECT l.id, l.payload, r.payload FROM campaign25.left_rows AS l INNER JOIN campaign25.right_rows AS r ON l.join_key = r.join_key WHERE l.id >= -7 AND r.payload != 4000;' \
  "${result}"
post_churn_revalidated_query_id=$(printf '%s\n' "${PHASE_SNAPSHOT}" | jq -r '.query_id // 0')
post_churn_revalidated_publication=$(publication_for_query_table "${post_churn_revalidated_query_id}" "${LEFT_ID}")
[[ $(printf '%s\n' "${post_churn_revalidated_publication}" | jq -r '.region_id // 0') == "${left_region_id}" \
  && $(printf '%s\n' "${post_churn_revalidated_publication}" | jq -r '.physical_address // empty') == "${post_churn_leader_address}" ]] || {
  echo "Campaign 25 revalidated dispatch did not publish to PD's new leader" >&2
  printf '%s\n%s\n' "${post_churn_leader}" "${post_churn_revalidated_publication}" >&2
  exit 1
}
run_persistent_query using \
  'SELECT l.id, r.id FROM campaign25.left_rows AS l JOIN campaign25.right_rows AS r USING (join_key) WHERE l.id = -7;' \
  $'-7\t1'
run_persistent_query cross \
  'SELECT l.id, r.id FROM campaign25.left_rows AS l CROSS JOIN campaign25.right_rows AS r WHERE l.id = -7 AND r.id = 1;' \
  $'-7\t1'
run_persistent_query comma \
  'SELECT l.id, r.id FROM campaign25.left_rows AS l, campaign25.right_rows AS r WHERE l.id = 42 AND r.id = 2;' \
  $'42\t2'
run_persistent_query no_match \
  'SELECT l.id, r.id FROM campaign25.left_rows AS l INNER JOIN campaign25.right_rows AS r ON l.join_key = r.join_key WHERE r.id = 3;' \
  ''

if [[ "${C25_ENABLE_BLOCKED_SHUTDOWN:-false}" == true ]]; then
  store_ids=($(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r '.stores[] | select(.store.state_name == "Up" and ((.store.node_state_name // "Serving") == "Serving")) | .store.id' \
    | sort -n))
  [[ ${#store_ids[@]} -eq 5 ]] || {
    echo "Campaign 25 blocked-shutdown proof requires exactly five serving TiKV stores" >&2
    exit 1
  }
  helper_a=${store_ids[0]}
  shared_b=${store_ids[1]}
  read_c=${store_ids[2]}
  read_d=${store_ids[3]}
  helper_e=${store_ids[4]}
  tiup ctl:v8.5.6 pd -u "http://${PD_ADDR}" config set leader-schedule-limit 0 >/dev/null
  tiup ctl:v8.5.6 pd -u "http://${PD_ADDR}" config set region-schedule-limit 0 >/dev/null
  [[ $(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/config/schedule" \
    | jq -r '."leader-schedule-limit" // -1') == 0 ]] || {
    echo "Campaign 25 could not disable background leader scheduling" >&2
    exit 1
  }
  helper_region_id=$(table_record_region_id lock_secondary "${HELPER_ID}" 1)
  [[ "${helper_region_id}" =~ ^[1-9][0-9]*$ \
    && "${helper_region_id}" != "${left_region_id}" \
    && "${helper_region_id}" != "${right_region_id}" ]] || {
    echo "Campaign 25 fixture did not expose a distinct helper-primary region" >&2
    mysql_go -Nse "SHOW TABLE campaign25.lock_secondary REGIONS" >&2 || true
    exit 1
  }
  ensure_region_placement "${left_region_id}" "${shared_b}" "${shared_b}" "${read_c}" "${read_d}"
  if [[ "${right_region_id}" != "${left_region_id}" ]]; then
    ensure_region_placement "${right_region_id}" "${shared_b}" "${shared_b}" "${read_c}" "${read_d}"
  fi
  # A leads the helper primary.  After A/E pause, B remains alive for both
  # read regions but is only a follower of the helper region, so it cannot
  # satisfy a primary lookup from a still-valid local leader lease.
  ensure_region_placement "${helper_region_id}" "${helper_a}" "${helper_a}" "${shared_b}" "${helper_e}"

  # The Rust node is deliberately long-lived.  Warm both of its relation
  # routes after the placement moves and prove their live BatchCommands
  # publications already target B before A and E are frozen.  Without this,
  # a previously cached stream to a frozen peer turns a topology proof into a
  # transport-cache timeout rather than a lock-resolution proof.
  shared_b_address=$(store_address "${shared_b}")
  [[ -n "${shared_b_address}" ]] || {
    echo "Campaign 25 placement preflight could not resolve the shared read-store address" >&2
    exit 1
  }
  placement_preflight_ready=false
  for placement_attempt in $(seq 1 5); do
    run_persistent_query "placement_preflight_${placement_attempt}" \
      'SELECT l.id, r.id FROM campaign25.left_rows AS l INNER JOIN campaign25.right_rows AS r ON l.join_key = r.join_key WHERE l.id = 42;' \
      $'42\t2'
    AUTHENTICATED_QUERY_COUNT=$((AUTHENTICATED_QUERY_COUNT + 1))
    placement_preflight_query_id=$(printf '%s\n' "${PHASE_SNAPSHOT}" | jq -r '.query_id // 0')
    [[ "${placement_preflight_query_id}" =~ ^[1-9][0-9]*$ ]] || {
      echo "Campaign 25 placement preflight did not expose a query identity" >&2
      exit 1
    }
    placement_preflight_ready=true
    for table_id in "${LEFT_ID}" "${RIGHT_ID}"; do
      expected_region_id=${left_region_id}
      [[ "${table_id}" == "${RIGHT_ID}" ]] && expected_region_id=${right_region_id}
      placement_publication=$(publication_for_query_table "${placement_preflight_query_id}" "${table_id}")
      placement_transport=$(transport_for_query_table "${placement_preflight_query_id}" "${table_id}")
      if [[ -z "${placement_publication}" || -z "${placement_transport}" ]] \
        || ! printf '%s\n' "${placement_publication}" | jq -e \
          --argjson region "${expected_region_id}" --arg address "${shared_b_address}" \
          '.region_id == $region and .physical_address == $address' >/dev/null \
        || ! printf '%s\n' "${placement_transport}" | jq -e \
          --argjson region "${expected_region_id}" \
          '(.located_region_ids == [$region]) and (.dispatched_region_ids == [$region]) and (.batch_attempts >= 1)' >/dev/null; then
        placement_preflight_ready=false
        break
      fi
    done
    [[ "${placement_preflight_ready}" == true ]] && break
  done
  [[ "${placement_preflight_ready}" == true ]] || {
    echo "Campaign 25 placement preflight did not refresh both relation routes to the surviving shared read store" >&2
    printf '%s\n' "${placement_publication:-<missing publication>}" >&2
    printf '%s\n' "${placement_transport:-<missing transport>}" >&2
    exit 1
  }

  lock_marker="${TAG}-left-secondary-ready"
  lock_output="${RUNTIME_DIR}/left-secondary-lock.out"
  lock_error="${RUNTIME_DIR}/left-secondary-lock.err"
  (
    mysql_go --unbuffered -Nse \
      "SET SESSION tidb_enable_async_commit = 0; SET SESSION tidb_enable_1pc = 0; BEGIN PESSIMISTIC; UPDATE campaign25.lock_secondary SET value = value + 1 WHERE id = 1; UPDATE campaign25.left_rows SET payload = payload + 1 WHERE id = -7; SELECT '${lock_marker}'; COMMIT;"
  ) >"${lock_output}" 2>"${lock_error}" &
  lock_holder_pid=$!
  for _ in $(seq 1 300); do
    grep -Fx "${lock_marker}" "${lock_output}" >/dev/null 2>&1 && break
    kill -0 "${lock_holder_pid}" 2>/dev/null || break
    sleep 0.1
  done
  grep -Fx "${lock_marker}" "${lock_output}" >/dev/null || {
    echo "Campaign 25 Go lock holder did not publish its prewrite marker" >&2
    sed -n '1,160p' "${lock_error}" >&2
    exit 1
  }
  wait "${lock_holder_pid}"
  fixture_logged=false
  for _ in $(seq 1 100); do
    if grep -Rqs 'injected skip committing secondaries' "${HOME}/.tiup/data/${TAG}"; then
      fixture_logged=true
      break
    fi
    sleep 0.1
  done
  [[ "${fixture_logged}" == true ]] || {
    echo "Campaign 25 Go fixture did not execute beforeCommitSecondaries=skip" >&2
    tail -160 "${PLAYGROUND_LOG}" >&2
    exit 1
  }
  freeze_tag_owned_store "${helper_a}"
  freeze_tag_owned_store "${helper_e}"
  # A paused peer can retain a short-lived stale lease.  Let the helper
  # leader's lease expire before testing its unavailable primary lookup.
  sleep 12
  run_persistent_query blocked_control \
    'SELECT l.id, r.id FROM campaign25.left_rows AS l INNER JOIN campaign25.right_rows AS r ON l.join_key = r.join_key WHERE l.id = 42;' \
    $'42\t2'
  AUTHENTICATED_QUERY_COUNT=$((AUTHENTICATED_QUERY_COUNT + 1))
  probe_output="${RUNTIME_DIR}/locked-row-probe.out"
  probe_error="${RUNTIME_DIR}/locked-row-probe.err"
  (
    mysql_go -Nse 'SELECT payload FROM campaign25.left_rows WHERE id = -7;'
  ) >"${probe_output}" 2>"${probe_error}" &
  lock_probe_pid=$!
  sleep 1
  kill -0 "${lock_probe_pid}" 2>/dev/null \
    && [[ ! -s "${probe_output}" ]] || {
      echo "Campaign 25 stock Go control read did not block on the isolated secondary lock" >&2
      sed -n '1,160p' "${probe_output}" >&2
      sed -n '1,160p' "${probe_error}" >&2
      exit 1
    }

  block_before_output=$(line_count "${PERSISTENT_CLIENT_OUTPUT}")
  block_before_errors=$(line_count "${PERSISTENT_CLIENT_ERROR}")
  block_before_snapshots=$(grep -c -F '"event":"query_multi_snapshot"' "${RUST_LOG}" || true)
  block_before_publications=$(grep -c -F '"event":"query_multi_transport_published"' "${RUST_LOG}" || true)
  block_before_transports=$(grep -c -F '"event":"query_multi_transport"' "${RUST_LOG}" || true)
  printf '%s\n' 'SELECT l.id, l.payload, r.payload FROM campaign25.left_rows AS l INNER JOIN campaign25.right_rows AS r ON l.join_key = r.join_key WHERE l.id >= -7 AND r.payload != 4000;' >&9
  AUTHENTICATED_QUERY_COUNT=$((AUTHENTICATED_QUERY_COUNT + 1))
  for _ in $(seq 1 300); do
    current_snapshots=$(grep -c -F '"event":"query_multi_snapshot"' "${RUST_LOG}" || true)
    current_publications=$(grep -c -F '"event":"query_multi_transport_published"' "${RUST_LOG}" || true)
    [[ "${current_snapshots}" -ge $((block_before_snapshots + 1)) \
      && "${current_publications}" -ge $((block_before_publications + 1)) ]] && break
    sleep 0.1
  done
  block_snapshot=$(grep -F '"event":"query_multi_snapshot"' "${RUST_LOG}" | tail -1)
  block_query_id=$(printf '%s\n' "${block_snapshot}" | jq -r '.query_id // 0')
  [[ "${block_query_id}" =~ ^[1-9][0-9]*$ \
    && $(printf '%s\n' "${block_snapshot}" | jq -r '.connection_id') == "${PERSISTENT_CONNECTION_ID}" \
    && $(printf '%s\n' "${block_snapshot}" | jq -r '.session_id') == "${PERSISTENT_SESSION_ID}" ]] || {
      echo "Campaign 25 blocked join did not preserve the persistent identity" >&2
      exit 1
  }
  block_publications=$(grep -F '"event":"query_multi_transport_published"' "${RUST_LOG}" \
    | tail -n +$((block_before_publications + 1)) | jq -s '.')
  printf '%s\n' "${block_publications}" | jq -e \
    --argjson query "${block_query_id}" --argjson left "${LEFT_ID}" \
    'any(.[]; .query_id == $query and .relation == 0 and .table_id == $left)' >/dev/null || {
      echo "Campaign 25 blocked join did not dispatch its locked left relation" >&2
      exit 1
    }
  sleep 1
  block_transports=$(grep -F '"event":"query_multi_transport"' "${RUST_LOG}" \
    | tail -n +$((block_before_transports + 1)) | jq -s '.')
  if [[ $(line_count "${PERSISTENT_CLIENT_OUTPUT}") != "${block_before_output}" \
    || $(line_count "${PERSISTENT_CLIENT_ERROR}") != "${block_before_errors}" ]] \
    || ! printf '%s\n' "${block_transports}" | jq -e --argjson query "${block_query_id}" \
      'all(.[]; .query_id != $query)' >/dev/null; then
    echo "Campaign 25 Rust join was not blocked on the isolated real TiKV lock" >&2
    exit 1
  fi
  shutdown_started_ms=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time() * 1000')
  kill -TERM "${RUST_PID}"
  shutdown_wait_seconds=$(((shutdown_grace_ms + 2999) / 1000))
  for _ in $(seq 1 "${shutdown_wait_seconds}"); do
    kill -0 "${RUST_PID}" 2>/dev/null || break
    sleep 1
  done
  kill -0 "${RUST_PID}" 2>/dev/null && {
    echo "Campaign 25 Rust SQL node exceeded its advertised shutdown grace" >&2
    exit 1
  }
  set +e
  wait "${RUST_PID}"
  rust_status=$?
  set -e
  RUST_PID=
  shutdown_elapsed_ms=$(( $(perl -MTime::HiRes=time -e 'printf "%.0f\n", time() * 1000') - shutdown_started_ms ))
  for stopped_pid in "${STOPPED_TIKV_PIDS[@]-}"; do
    kill -CONT "${stopped_pid}" 2>/dev/null || true
  done
  STOPPED_TIKV_PIDS=()
  kill -TERM "${lock_probe_pid}" 2>/dev/null || true
  wait "${lock_probe_pid}" 2>/dev/null || true
  [[ "${rust_status}" == 0 && "${shutdown_elapsed_ms}" -le $((shutdown_grace_ms + 2000)) ]] || {
    echo "Campaign 25 blocked-query shutdown did not complete cleanly" >&2
    tail -240 "${RUST_LOG}" >&2
    exit 1
  }
  for _ in $(seq 1 100); do
    kill -0 "${PERSISTENT_CLIENT_PID}" 2>/dev/null || break
    sleep 0.1
  done
  set +e
  wait "${PERSISTENT_CLIENT_PID}"
  blocked_client_status=$?
  set -e
  [[ "${blocked_client_status}" -ne 0 && $(line_count "${PERSISTENT_CLIENT_OUTPUT}") == "${block_before_output}" ]] || {
    echo "Campaign 25 blocked stock client completed instead of observing cancellation" >&2
    exit 1
  }
  PERSISTENT_CLIENT_PID=
  exec 9>&-
  PERSISTENT_CLIENT_FD_OPEN=false
fi

snapshot=$(grep -F '"event":"query_multi_snapshot"' "${RUST_LOG}" | head -1)
printf '%s\n' "${snapshot}" | jq -e --argjson left "${LEFT_ID}" --argjson right "${RIGHT_ID}" \
  '.snapshot_ts > 0 and (.relations | length == 2)
   and .relations[0].table_id == $left and .relations[1].table_id == $right
   and (.relations[] | .handle_range_count > 0)' >/dev/null
publication_count=$(grep -F '"event":"query_multi_transport_published"' "${RUST_LOG}" | wc -l | tr -d ' ')
[[ "${publication_count}" -ge 2 ]] || {
  echo "Campaign 25 did not publish one real TiKV dispatch per relation" >&2
  tail -160 "${RUST_LOG}" >&2
  exit 1
}
grep -F '"event":"query_multi_transport"' "${RUST_LOG}" | tail -2 | jq -s \
  --argjson left "${LEFT_ID}" --argjson right "${RIGHT_ID}" \
  'length == 2 and ([.[].table_id] | sort) == ([$left, $right] | sort)
   and all(.[]; .batch_attempts >= 1 and .unary_attempts == 0)' >/dev/null

echo "Campaign 25 live multi-relation smoke proof passed: rust_pid=${RUST_PID_AT_START}; persistent_connection_id=${PERSISTENT_CONNECTION_ID}; persistent_session_id=${PERSISTENT_SESSION_ID}; snapshot=$(printf '%s' "${snapshot}" | jq -r '.snapshot_ts'); tables=${LEFT_ID},${RIGHT_ID}; rows=3; authenticated_queries=${AUTHENTICATED_QUERY_COUNT}; join_shapes=on,using,cross,comma,no-match; left_region=${left_region_id}; leader_transfer=${leader_store}->${next_store}"
