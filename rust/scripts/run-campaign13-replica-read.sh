#!/usr/bin/env bash

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="campaign13-replica-read-${$}"
PORT_OFFSET=${C13_PORT_OFFSET:-28000}
PD_PORT=$((2379 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
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

if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
  echo "refusing occupied PD endpoint ${PD_ADDR}; set C13_PORT_OFFSET" >&2
  exit 1
fi

tiup playground v8.5.6 --mode tikv-slim --without-monitor --tag "${TAG}" \
  --pd 1 --kv 3 --port-offset "${PORT_OFFSET}" >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

ready=false
for _ in $(seq 1 120); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    echo "TiUP playground exited before readiness" >&2
    tail -120 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  STORE_ADDRESSES=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r '.stores[] | select(.store.state_name == "Up" and ((.store.node_state_name // "Serving") == "Serving")) | .store.address' \
      2>/dev/null) || true
  if [[ $(printf '%s\n' "${STORE_ADDRESSES}" | awk 'NF { count++ } END { print count + 0 }') -eq 3 ]]; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  echo "three Up/Serving TiKV stores did not become ready" >&2
  tail -120 "${PLAYGROUND_LOG}" >&2
  exit 1
fi
if [[ -z "$(tag_owned_pids)" ]]; then
  echo "TiUP did not publish owned processes for ${TAG}" >&2
  exit 1
fi

export C13_PD_ADDR="${PD_ADDR}"
cd "${RUST_ROOT}"
CARGO_BUILD_JOBS=12 cargo test -j12 -p difftest-transaction-tests \
  --test realtikv_replica_read \
  follower_policy_reaches_a_live_nonleader_voter \
  -- --ignored --exact --nocapture >"${RUST_LOG}" 2>&1 || {
  echo "Campaign 13 Rust follower-read proof failed" >&2
  tail -160 "${RUST_LOG}" >&2
  exit 1
}

MARKER=$(grep '^campaign13_replica_read ' "${RUST_LOG}" | tail -1 || true)
if [[ -z "${MARKER}" ]] \
  || [[ "${MARKER}" != *"replica_read=true"* ]] \
  || [[ "${MARKER}" != *"stale_read=false"* ]] \
  || [[ "${MARKER}" != *"usable_response=true"* ]]; then
  echo "Campaign 13 marker did not prove a usable nonleader replica read" >&2
  tail -160 "${RUST_LOG}" >&2
  exit 1
fi

LEADER_PEER_ID=$(printf '%s\n' "${MARKER}" | tr ' ' '\n' | sed -n 's/^leader_peer_id=//p' | tail -1)
POST_LEADER_PEER_ID=$(printf '%s\n' "${MARKER}" | tr ' ' '\n' | sed -n 's/^post_leader_peer_id=//p' | tail -1)
SELECTED_PEER_ID=$(printf '%s\n' "${MARKER}" | tr ' ' '\n' | sed -n 's/^selected_peer_id=//p' | tail -1)
if [[ -z "${LEADER_PEER_ID}" ]] \
  || [[ -z "${POST_LEADER_PEER_ID}" ]] \
  || [[ -z "${SELECTED_PEER_ID}" ]] \
  || [[ "${LEADER_PEER_ID}" != "${POST_LEADER_PEER_ID}" ]]; then
  echo "Campaign 13 marker did not preserve the cached leader across follower success" >&2
  tail -160 "${RUST_LOG}" >&2
  exit 1
fi
if [[ "${SELECTED_PEER_ID}" == "${LEADER_PEER_ID}" ]]; then
  echo "Campaign 13 marker selected the cached leader instead of a follower" >&2
  tail -160 "${RUST_LOG}" >&2
  exit 1
fi

echo "Campaign 13 replica read passed: ${MARKER}"
