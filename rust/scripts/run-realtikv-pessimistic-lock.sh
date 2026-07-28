#!/usr/bin/env bash

# Real-TiKV proof that two transactions serialize on one pessimistic lock.
#
# Starts an owned TiUP playground (PD + 3 TiKV, no TiDB), runs the ignored
# Rust proof against it, then unconditionally tears the playground down and
# verifies nothing it owned survived.

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="realtikv-pessimistic-lock-${$}"
PORT_OFFSET=${PESSIMISTIC_LOCK_PORT_OFFSET:-44000}
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
    && [[ "${TAG}" == realtikv-pessimistic-lock-* ]]
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
    echo "pessimistic-lock cleanup failed: tiup clean failed for ${TAG}" >&2
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
    echo "pessimistic-lock cleanup left an owned process or TiUP row" >&2
    cleanup_failed=true
  fi
  local address
  for address in ${STORE_ADDRESSES}; do
    if endpoint_reachable "${address}"; then
      echo "pessimistic-lock cleanup left TiKV ${address} reachable" >&2
      cleanup_failed=true
    fi
  done
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "pessimistic-lock cleanup left PD ${PD_ADDR} reachable" >&2
    cleanup_failed=true
  fi
  if ! validate_owned_paths; then
    echo "pessimistic-lock cleanup refused unsafe paths" >&2
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
    echo "pessimistic-lock retained logs: ${PLAYGROUND_LOG} ${RUST_LOG}" >&2
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
  echo "PESSIMISTIC_LOCK_PORT_OFFSET must be numeric, at least 1000, and keep ports valid" >&2
  exit 2
fi
for command in tiup curl jq nc pgrep cargo awk; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "pessimistic-lock requires ${command}" >&2
    exit 1
  fi
done
if endpoint_reachable "${PD_ADDR}" || endpoint_reachable "127.0.0.1:${KV_PORT}"; then
  echo "refusing occupied endpoints; set PESSIMISTIC_LOCK_PORT_OFFSET" >&2
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
PESSIMISTIC_LOCK_PD_ADDR="${PD_ADDR}" \
  CARGO_BUILD_JOBS=12 cargo test --offline --locked -j12 -p tidb-txnkv \
    --test all \
    pessimistic_lock_realtikv_source::two_transactions_serialize_on_one_real_pessimistic_lock \
    -- --ignored --exact --nocapture >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

wait "${RUST_PID}" || {
  RUST_PID=
  echo "real pessimistic lock proof failed" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
}
RUST_PID=

MARKER=$(grep '^pessimistic_lock_realtikv status=passed ' "${RUST_LOG}" | tail -1 || true)
PHASES=$(grep '^pessimistic_lock_realtikv phase=' "${RUST_LOG}" || true)
if [[ "${MARKER}" != *"cluster_id="* ]] \
  || [[ "${MARKER}" != *"holder_commit_ts="* ]] \
  || [[ "${MARKER}" != *"waiter_for_update_ts="* ]] \
  || [[ "${PHASES}" != *"phase=held"* ]] \
  || [[ "${PHASES}" != *"phase=blocked"* ]] \
  || [[ "${PHASES}" != *"phase=kept_alive"* ]] \
  || [[ "${PHASES}" != *"phase=released"* ]]; then
  echo "receipt omitted the held/blocked/released lock evidence" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
fi

echo "pessimistic lock serialization passed: ${MARKER}"
