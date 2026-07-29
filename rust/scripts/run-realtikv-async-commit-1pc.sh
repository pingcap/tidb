#!/usr/bin/env bash

# Real-TiKV proofs of the two faster-than-2PC commit protocols:
#   * an async-commit transaction commits at max(min_commit_ts) with no second
#     PD timestamp, and its rows read back at that derived timestamp, and
#   * a single-region 1PC transaction commits inside its prewrite, publishing
#     no Commit RPC at all.
#
# Starts an owned TiUP playground (PD + 3 TiKV, no TiDB), runs the ignored
# Rust proofs against it, then unconditionally tears the playground down and
# verifies nothing it owned survived.

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="realtikv-async-commit-1pc-${$}"
PORT_OFFSET=${ASYNC_COMMIT_PORT_OFFSET:-45000}
PD_PORT=$((2379 + PORT_OFFSET))
KV_PORT=$((20160 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
PLAYGROUND_PID=
RUST_PID=
STORE_ADDRESSES=

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
  local tiup_data="${TIUP_HOME:-${HOME}/.tiup}/data"
  [[ "${TAG_DIR}" == "${tiup_data}/${TAG}" ]] \
    && [[ "${TAG}" == realtikv-async-commit-1pc-* ]]
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
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "async-commit cleanup failed: tiup clean failed for ${TAG}" >&2
    cleanup_failed=true
  fi
  local cleaned=false
  for _ in $(seq 1 30); do
    if [[ -z "$(tag_owned_pids)" ]] && [[ -z "$(tag_status_rows || true)" ]]; then
      cleaned=true
      break
    fi
    sleep 1
  done
  if [[ "${cleaned}" != true ]]; then
    echo "async-commit cleanup left an owned process or TiUP row" >&2
    cleanup_failed=true
  fi
  local address
  for address in ${STORE_ADDRESSES}; do
    if endpoint_reachable "${address}"; then
      echo "async-commit cleanup left TiKV ${address} reachable" >&2
      cleanup_failed=true
    fi
  done
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "async-commit cleanup left PD ${PD_ADDR} reachable" >&2
    cleanup_failed=true
  fi
  if ! validate_owned_paths; then
    echo "async-commit cleanup refused unsafe paths" >&2
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
    echo "async-commit retained logs: ${PLAYGROUND_LOG} ${RUST_LOG}" >&2
  fi
  if [[ "${cleanup_status}" -ne 0 ]]; then
    exit "${cleanup_status}"
  fi
  exit "${original_status}"
}

if [[ $# -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 2
fi
if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] \
  || (( PORT_OFFSET < 1000 || PD_PORT > 65535 || KV_PORT > 65535 )); then
  echo "ASYNC_COMMIT_PORT_OFFSET must be numeric, at least 1000, and keep ports valid" >&2
  exit 2
fi
for command in tiup curl jq nc pgrep cargo awk; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "async-commit requires ${command}" >&2
    exit 1
  fi
done
if endpoint_reachable "${PD_ADDR}" || endpoint_reachable "127.0.0.1:${KV_PORT}"; then
  echo "refusing occupied endpoints; set ASYNC_COMMIT_PORT_OFFSET" >&2
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
    echo "TiUP playground exited before readiness" >&2
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
  echo "real PD/TiKV topology did not become ready" >&2
  tail -120 "${PLAYGROUND_LOG}" >&2
  exit 1
fi

cd "${RUST_ROOT}"
ASYNC_COMMIT_PD_ADDR="${PD_ADDR}" \
  CARGO_BUILD_JOBS=12 cargo test --offline --locked -j12 -p tidb-txnkv \
    --test all \
    async_commit_one_pc_realtikv_source:: \
    -- --ignored --nocapture --test-threads 1 >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

wait "${RUST_PID}" || {
  RUST_PID=
  echo "real async-commit/1PC proof failed" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
}
RUST_PID=

ASYNC_MARKER=$(grep 'async_commit_realtikv status=passed ' "${RUST_LOG}" | tail -1 || true)
ASYNC_PHASES=$(grep 'async_commit_realtikv phase=' "${RUST_LOG}" || true)
if [[ "${ASYNC_MARKER}" != *"cluster_id="* ]] \
  || [[ "${ASYNC_MARKER}" != *"start_ts="* ]] \
  || [[ "${ASYNC_MARKER}" != *"commit_ts="* ]] \
  || [[ "${ASYNC_PHASES}" != *"phase=prewrite tag=3"* ]] \
  || [[ "${ASYNC_PHASES}" != *"request_id="* ]] \
  || [[ "${ASYNC_PHASES}" != *"physical_address="* ]]; then
  echo "receipt omitted the async-commit prewrite evidence" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
fi

ONE_PC_MARKER=$(grep 'one_pc_realtikv status=passed ' "${RUST_LOG}" | tail -1 || true)
ONE_PC_PHASES=$(grep 'one_pc_realtikv phase=' "${RUST_LOG}" || true)
if [[ "${ONE_PC_MARKER}" != *"cluster_id="* ]] \
  || [[ "${ONE_PC_MARKER}" != *"commit_ts="* ]] \
  || [[ "${ONE_PC_PHASES}" != *"phase=prewrite tag=3"* ]] \
  || [[ "${ONE_PC_PHASES}" != *"phase=no_commit_rpc "* ]] \
  || [[ "${ONE_PC_PHASES}" != *"primary_publications=0"* ]]; then
  echo "receipt omitted the 1PC no-commit-RPC evidence" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
fi
# Commit is BatchCommands field 4; a 1PC transaction must never publish one.
if [[ "${ONE_PC_PHASES}" == *"tag=4"* ]]; then
  echo "1PC receipt contains a Commit publication" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
fi

echo "async commit passed: ${ASYNC_MARKER}"
echo "1PC passed: ${ONE_PC_MARKER}"
