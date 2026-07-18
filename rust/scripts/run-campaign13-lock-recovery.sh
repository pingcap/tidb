#!/usr/bin/env bash

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="campaign13-lock-recovery-${$}"
PORT_OFFSET=${C13_PORT_OFFSET:-31000}
PD_PORT=$((2379 + PORT_OFFSET))
DB_PORT=$((4000 + PORT_OFFSET))
KV_PORT=$((20160 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
DB_ADDR="127.0.0.1:${DB_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
TIDB_SERVER=${C13_TIDB_SERVER:-}
MYSQL_CLIENT=${C13_MYSQL_CLIENT:-mysql}
READINESS_ATTEMPTS=240
PLAYGROUND_PID=
STORE_ADDRESSES=

tag_status_rows() {
  tiup status | awk -v tag="${TAG}" \
    'NR > 2 && ($1 == tag || index($0, "/data/" tag "/")) { print }'
}

tag_owned_pids() {
  pgrep -f "${TAG_DIR}" || true
}

cleanup() {
  local original_status=$?
  local cleanup_failed=false
  trap - EXIT INT TERM

  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "Campaign 13 cleanup failed: tiup clean failed for ${TAG}" >&2
    cleanup_failed=true
  fi

  local cleaned=false
  for _ in $(seq 1 30); do
    local alive=false
    local pid
    for pid in $(tag_owned_pids); do
      if kill -0 "${pid}" 2>/dev/null; then
        alive=true
        break
      fi
    done
    local rows
    rows=$(tag_status_rows 2>/dev/null || true)
    if [[ "${alive}" == false ]] && [[ -z "${rows}" ]]; then
      cleaned=true
      break
    fi
    sleep 1
  done
  if [[ "${cleaned}" != true ]]; then
    echo "Campaign 13 cleanup failed: owned process or registry row remains" >&2
    cleanup_failed=true
  fi

  local address
  for address in ${STORE_ADDRESSES}; do
    local port=${address##*:}
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "Campaign 13 cleanup failed: TiKV ${address} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if nc -z -w 1 127.0.0.1 "${KV_PORT}" >/dev/null 2>&1; then
    echo "Campaign 13 cleanup failed: TiKV 127.0.0.1:${KV_PORT} remains reachable" >&2
    cleanup_failed=true
  fi
  if nc -z -w 1 127.0.0.1 "${DB_PORT}" >/dev/null 2>&1; then
    echo "Campaign 13 cleanup failed: TiDB ${DB_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "Campaign 13 cleanup failed: PD ${PD_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi
  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
  fi
  if [[ "${cleanup_failed}" == false ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f "${PLAYGROUND_LOG}" "${RUST_LOG}"
  else
    echo "Campaign 13 retained logs: ${PLAYGROUND_LOG} ${RUST_LOG}" >&2
  fi
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}
trap cleanup EXIT INT TERM

if [[ -z "${TIDB_SERVER}" ]] || [[ ! -x "${TIDB_SERVER}" ]]; then
  echo "C13_TIDB_SERVER must name an executable failpoint-enabled tidb-server" >&2
  exit 1
fi
if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
  echo "C13_MYSQL_CLIENT must name an executable MySQL client" >&2
  exit 1
fi
if nc -z -w 1 127.0.0.1 "${PD_PORT}" >/dev/null 2>&1 \
  || nc -z -w 1 127.0.0.1 "${DB_PORT}" >/dev/null 2>&1 \
  || nc -z -w 1 127.0.0.1 "${KV_PORT}" >/dev/null 2>&1; then
  echo "refusing occupied Campaign 13 endpoints; set C13_PORT_OFFSET" >&2
  exit 1
fi

export GO_FAILPOINTS='github.com/pingcap/tidb/pkg/server/enableTestAPI=return;tikvclient/beforeCommitSecondaries=return("skip")'
tiup playground v8.5.7 --without-monitor --tag "${TAG}" \
  --db 1 --pd 1 --kv 1 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  --db.binpath "${TIDB_SERVER}" >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

ready=false
for _ in $(seq 1 "${READINESS_ATTEMPTS}"); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    echo "TiUP playground exited before readiness" >&2
    tail -120 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  STORE_ADDRESSES=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r '.stores[] | select(.store.state_name == "Up" and ((.store.node_state_name // "Serving") == "Serving")) | .store.address' \
      2>/dev/null) || true
  if [[ -n "${STORE_ADDRESSES}" ]] \
    && "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${DB_PORT}" -uroot -Nse 'select 1' \
      >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  echo "TiDB/PD/TiKV did not become ready" >&2
  tail -120 "${PLAYGROUND_LOG}" >&2
  exit 1
fi
if [[ -z "$(tag_owned_pids)" ]]; then
  echo "TiUP did not publish owned processes for ${TAG}" >&2
  exit 1
fi

"${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${DB_PORT}" -uroot <<'SQL'
DROP DATABASE IF EXISTS campaign13_lock;
CREATE DATABASE campaign13_lock;
USE campaign13_lock;
CREATE TABLE locked_secondary (id BIGINT PRIMARY KEY CLUSTERED, value BIGINT);
SET SESSION tidb_wait_split_region_finish = 1;
SPLIT TABLE locked_secondary BY (2);
SET SESSION tidb_enable_async_commit = 0;
SET SESSION tidb_enable_1pc = 0;
BEGIN OPTIMISTIC;
INSERT INTO locked_secondary VALUES (1, 10), (2, 20);
COMMIT;
SQL

fixture_logged=false
for _ in $(seq 1 20); do
  if grep -Rqs 'injected skip committing secondaries' "${TAG_DIR}"; then
    fixture_logged=true
    break
  fi
  sleep 1
done
if [[ "${fixture_logged}" != true ]]; then
  echo "fixture failed: tidb-server did not execute beforeCommitSecondaries=skip" >&2
  tail -160 "${PLAYGROUND_LOG}" >&2
  exit 1
fi

export C13_PD_ADDR="${PD_ADDR}"
export C13_LOCK_TABLE_ID
C13_LOCK_TABLE_ID=$("${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${DB_PORT}" -uroot -Nse \
  "select tidb_table_id from information_schema.tables where table_schema='campaign13_lock' and table_name='locked_secondary'")
export C13_CURRENT_TS
C13_CURRENT_TS=$("${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${DB_PORT}" -uroot -Nse \
  'begin; select @@tidb_current_ts; rollback')
if [[ ! "${C13_LOCK_TABLE_ID}" =~ ^[0-9]+$ ]] || [[ ! "${C13_CURRENT_TS}" =~ ^[0-9]+$ ]]; then
  echo "fixture failed: invalid table ID or current TSO" >&2
  exit 1
fi

cd "${RUST_ROOT}"
CARGO_BUILD_JOBS=12 cargo test -j12 -p difftest-transaction-tests \
  --test realtikv_lock_recovery \
  committed_primary_resolves_secondary_then_publishes_one_cop_response \
  -- --ignored --exact --nocapture >"${RUST_LOG}" 2>&1 || {
  echo "Campaign 13 Rust lock-recovery proof failed" >&2
  tail -180 "${RUST_LOG}" >&2
  exit 1
}

MARKER=$(grep '^campaign13_lock_recovery ' "${RUST_LOG}" | tail -1 || true)
if [[ "${MARKER}" != *"status=committed"* ]] \
  || [[ "${MARKER}" != *"lock_start_ts="* ]] \
  || [[ "${MARKER}" != *"caller_start_ts="* ]] \
  || [[ "${MARKER}" != *"locked_key_hex="* ]] \
  || [[ "${MARKER}" != *"primary_key_hex="* ]] \
  || [[ "${MARKER}" != *"primary_route="* ]] \
  || [[ "${MARKER}" != *"commit_ts="* ]] \
  || [[ "${MARKER}" != *"resolve_route="* ]] \
  || [[ "${MARKER}" != *"cop_route="* ]] \
  || [[ "${MARKER}" != *"cop_attempts=2"* ]] \
  || [[ "${MARKER}" != *"publications=1"* ]] \
  || [[ "${MARKER}" != *"resolve_key_hex="* ]]; then
  echo "Campaign 13 marker did not prove committed-primary lock recovery" >&2
  tail -180 "${RUST_LOG}" >&2
  exit 1
fi

echo "Campaign 13 lock recovery passed: ${MARKER}"
