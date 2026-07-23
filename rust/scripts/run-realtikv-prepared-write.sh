#!/usr/bin/env bash

# Real-PD/TiKV proof that a prepared INSERT/UPDATE persists through the Stage D
# server composition. It starts a tag-owned three-TiKV playground (no Go TiDB:
# the focused test writes its own configured row keys), runs the ignored Rust
# test with the discovered PD address, verifies the durability marker, then
# removes only its own tagged processes and data.

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="realtikv-prepared-write-${$}"
PORT_OFFSET=${PREPARED_WRITE_PORT_OFFSET:-44000}
PD_PORT=$((2379 + PORT_OFFSET))
KV_PORT=$((20160 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
PLAYGROUND_PID=
RUST_PID=
STORE_ADDRESSES=
TIUP_CLEAN_REQUIRED=true
SELF_TEST_ROOT=

tag_status_rows() {
  tiup status 2>/dev/null | awk -v tag="${TAG}" \
    'NR > 2 && ($1 == tag || index($0, "/data/" tag "/")) { print }'
}

tag_owned_pids() {
  pgrep -f "${TAG_DIR}" || true
}

endpoint_reachable() {
  local address=$1
  nc -z -w 1 "${address%:*}" "${address##*:}" >/dev/null 2>&1
}

validate_owned_paths() {
  if [[ -n "${SELF_TEST_ROOT}" ]]; then
    [[ "${TAG_DIR}" == "${SELF_TEST_ROOT}/"* ]]
    return
  fi
  local tiup_data="${TIUP_HOME:-${HOME}/.tiup}/data"
  [[ "${TAG_DIR}" == "${tiup_data}/${TAG}" ]] \
    && [[ "${TAG}" == campaign28-prepared-write-* ]]
}

cleanup_resources() {
  local cleanup_failed=false
  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    kill "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if [[ "${TIUP_CLEAN_REQUIRED}" == true ]] \
    && ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "prepared-write cleanup failed: tiup clean failed for ${TAG}" >&2
    cleanup_failed=true
  fi
  local cleaned=false
  for _ in $(seq 1 30); do
    local rows=
    if [[ "${TIUP_CLEAN_REQUIRED}" == true ]]; then
      rows=$(tag_status_rows || true)
    fi
    if [[ -z "$(tag_owned_pids)" ]] && [[ -z "${rows}" ]]; then
      cleaned=true
      break
    fi
    sleep 1
  done
  if [[ "${cleaned}" != true ]]; then
    echo "prepared-write cleanup left an owned process or TiUP row" >&2
    cleanup_failed=true
  fi
  local address
  for address in ${STORE_ADDRESSES}; do
    if endpoint_reachable "${address}"; then
      echo "prepared-write cleanup left TiKV ${address} reachable" >&2
      cleanup_failed=true
    fi
  done
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "prepared-write cleanup left PD ${PD_ADDR} reachable" >&2
    cleanup_failed=true
  fi
  if ! validate_owned_paths; then
    echo "prepared-write cleanup refused unsafe paths" >&2
    cleanup_failed=true
  elif [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
  fi
  [[ "${cleanup_failed}" == false ]]
}

cleanup() {
  local original_status=$?
  local cleanup_status=0
  trap - EXIT INT TERM
  cleanup_resources || cleanup_status=$?
  if [[ "${cleanup_status}" -eq 0 ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG}"
  else
    echo "prepared-write retained logs: ${PLAYGROUND_LOG} ${RUST_LOG}" >&2
  fi
  if [[ "${cleanup_status}" -ne 0 ]]; then
    exit "${cleanup_status}"
  fi
  exit "${original_status}"
}

self_test_cleanup() {
  local unrelated_pid=
  SELF_TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/c28-stage-e-self-test.XXXXXX")
  TAG="campaign28-prepared-write-self-test-${$}"
  TAG_DIR="${SELF_TEST_ROOT}/${TAG}"
  mkdir -p "${TAG_DIR}"
  TIUP_CLEAN_REQUIRED=false
  STORE_ADDRESSES=
  PD_ADDR="127.0.0.1:1"

  bash -c "exec -a '${TAG_DIR}/owned' sleep 120" &
  PLAYGROUND_PID=$!
  sleep 120 &
  unrelated_pid=$!
  sleep 1
  cleanup_resources
  if kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    echo "Stage E self-test left the owned process alive" >&2
    kill "${unrelated_pid}" 2>/dev/null || true
    return 1
  fi
  if ! kill -0 "${unrelated_pid}" 2>/dev/null; then
    echo "Stage E self-test killed an unrelated process" >&2
    return 1
  fi
  kill "${unrelated_pid}" 2>/dev/null || true
  wait "${unrelated_pid}" 2>/dev/null || true
  if [[ -e "${TAG_DIR}" ]]; then
    echo "Stage E self-test left owned paths" >&2
    return 1
  fi
  rmdir "${SELF_TEST_ROOT}"
  echo "prepared-write cleanup self-test passed"
}

if [[ "${1:-}" == "--self-test-cleanup" ]]; then
  self_test_cleanup
  exit 0
fi
if [[ $# -ne 0 ]]; then
  echo "usage: $0 [--self-test-cleanup]" >&2
  exit 2
fi
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] \
  || (( PORT_OFFSET < 1000 || PD_PORT > 65535 || KV_PORT > 65535 )); then
  echo "PREPARED_WRITE_PORT_OFFSET must be numeric, at least 1000, and keep ports valid" >&2
  exit 2
fi
for command in tiup curl jq nc pgrep cargo awk; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "prepared-write requires ${command}" >&2
    exit 1
  fi
done
if endpoint_reachable "${PD_ADDR}" || endpoint_reachable "127.0.0.1:${KV_PORT}"; then
  echo "refusing occupied Stage E endpoints; set PREPARED_WRITE_PORT_OFFSET" >&2
  exit 1
fi

trap cleanup EXIT INT TERM
tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 0 --pd 1 --kv 3 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

ready=false
for _ in $(seq 1 240); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    echo "Stage E TiUP playground exited before readiness" >&2
    tail -120 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  STORE_ADDRESSES=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r '.stores[] | select(.store.state_name == "Up" and ((.store.node_state_name // "Serving") == "Serving")) | .store.address' \
      2>/dev/null) || true
  STORE_COUNT=$(printf '%s\n' "${STORE_ADDRESSES}" | awk 'NF { count++ } END { print count + 0 }')
  if [[ "${STORE_COUNT}" -eq 3 ]]; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]] || [[ -z "$(tag_owned_pids)" ]]; then
  echo "Stage E real PD/TiKV topology did not become ready" >&2
  tail -120 "${PLAYGROUND_LOG}" >&2
  exit 1
fi

cd "${RUST_ROOT}"
PREPARED_WRITE_PD_ADDR="${PD_ADDR}" \
  CARGO_BUILD_JOBS=12 cargo test --offline --locked -j12 -p tidb-exec \
    --test prepared_write_persists_realtikv_source \
    prepared_insert_and_update_persist_through_one_shared_authority \
    -- --ignored --exact --nocapture >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

wait "${RUST_PID}" || {
  RUST_PID=
  echo "prepared-write prepared-write persistence proof failed" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
}
RUST_PID=

MARKER=$(grep '^campaign28_prepared_write status=passed ' "${RUST_LOG}" | tail -1 || true)
if [[ "${MARKER}" != *"cluster_id="* ]] \
  || [[ "${MARKER}" != *"table_id=528491"* ]] \
  || [[ "${MARKER}" != *"final_balance=107"* ]] \
  || [[ "${MARKER}" != *"write_authority_id="* ]] \
  || [[ "${MARKER}" != *"restart_authority_id="* ]]; then
  echo "Stage E receipt omitted durable-write evidence" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
fi

WRITE_AUTHORITY=$(printf '%s\n' "${MARKER}" | sed -E 's/.* write_authority_id=([0-9]+).*/\1/')
RESTART_AUTHORITY=$(printf '%s\n' "${MARKER}" | sed -E 's/.* restart_authority_id=([0-9]+).*/\1/')
if [[ "${WRITE_AUTHORITY}" == "${RESTART_AUTHORITY}" ]]; then
  echo "Stage E did not prove persistence across a fresh authority" >&2
  exit 1
fi

echo "prepared-write prepared-write persistence passed: ${MARKER}"
