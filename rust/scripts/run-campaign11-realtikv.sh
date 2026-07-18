#!/usr/bin/env bash

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="campaign11-realtikv-${$}"
PORT_OFFSET=${C11_PORT_OFFSET:-24000}
PD_SEED_PORT=$((2379 + PORT_OFFSET))
PD_SEED="127.0.0.1:${PD_SEED_PORT}"
PLAYGROUND_PID=
RUST_PID=
OWNED_PIDS=
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
PHASE_DIR="${TMPDIR:-/tmp}/${TAG}-phases"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"

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

phase_values() {
  local file=$1
  local key=$2
  awk -F= -v key="${key}" '$1 == key { sub(/^[^=]*=/, ""); print }' "${file}"
}

wait_for_phase() {
  local name=$1
  local path="${PHASE_DIR}/${name}"
  for _ in $(seq 1 1200); do
    if [[ -s "${path}" ]]; then
      return 0
    fi
    if [[ -n "${RUST_PID}" ]] && ! kill -0 "${RUST_PID}" 2>/dev/null; then
      echo "Rust phase process exited while waiting for ${name}" >&2
      tail -100 "${RUST_LOG}" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for Rust phase ${name}" >&2
  return 1
}

url_port() {
  local url=$1
  url=${url%/}
  printf '%s\n' "${url##*:}"
}

cleanup() {
  local original_status=$?
  local cleanup_failed=false
  trap - EXIT INT TERM

  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    kill "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  OWNED_PIDS=$(merge_owned_pids)
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "Campaign 11 cleanup failed: tiup clean failed for ${TAG}" >&2
    cleanup_failed=true
  fi

  local processes_cleaned=false
  for _ in $(seq 1 30); do
    OWNED_PIDS=$(merge_owned_pids)
    local process_alive=false
    local pid
    for pid in ${OWNED_PIDS}; do
      if kill -0 "${pid}" 2>/dev/null; then
        process_alive=true
        break
      fi
    done
    local status_rows
    if status_rows=$(tag_status_rows 2>/dev/null) \
      && [[ "${process_alive}" == false ]] \
      && [[ -z "${status_rows}" ]]; then
      processes_cleaned=true
      break
    fi
    sleep 1
  done
  if [[ "${processes_cleaned}" != true ]]; then
    echo "Campaign 11 cleanup failed: owned registry or process remains for ${TAG}" >&2
    cleanup_failed=true
  fi

  local endpoint
  if [[ -f "${PHASE_DIR}/members-ready" ]]; then
    while IFS= read -r endpoint; do
      if curl -sf --max-time 1 "${endpoint}/pd/api/v1/version" >/dev/null; then
        echo "Campaign 11 cleanup failed: PD endpoint ${endpoint} remains reachable" >&2
        cleanup_failed=true
      fi
    done < <(phase_values "${PHASE_DIR}/members-ready" member_url)
  fi
  if [[ -f "${PHASE_DIR}/route-ready" ]]; then
    while IFS= read -r endpoint; do
      local port
      port=$(url_port "${endpoint}")
      if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
        echo "Campaign 11 cleanup failed: TiKV endpoint ${endpoint} remains reachable" >&2
        cleanup_failed=true
      fi
    done < <(phase_values "${PHASE_DIR}/route-ready" store_address)
  fi
  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}" "${PHASE_DIR}"
    if [[ -e "${TAG_DIR}" || -e "${PHASE_DIR}" ]]; then
      echo "Campaign 11 cleanup failed: owned tag or phase directory remains" >&2
      cleanup_failed=true
    fi
  fi
  rm -f "${PLAYGROUND_LOG}" "${RUST_LOG}"
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}
trap cleanup EXIT INT TERM

if curl -sf --max-time 1 "http://${PD_SEED}/pd/api/v1/version" >/dev/null; then
  echo "refusing occupied PD seed ${PD_SEED}; set C11_PORT_OFFSET" >&2
  exit 1
fi
mkdir -m 700 "${PHASE_DIR}"

tiup playground v8.5.6 --mode tikv-slim --without-monitor --tag "${TAG}" \
  --pd 3 --kv 3 --port-offset "${PORT_OFFSET}" >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

ready=false
for _ in $(seq 1 120); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    echo "TiUP playground exited before readiness" >&2
    tail -100 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  PD_MEMBER_COUNT=$(curl -sf --max-time 2 \
    "http://${PD_SEED}/pd/api/v1/members" | jq -r '.members | length' 2>/dev/null) || true
  TIKV_SERVING_COUNT=$(curl -sf --max-time 2 \
    "http://${PD_SEED}/pd/api/v1/stores" | jq -r \
    '[.stores[] | select(.store.state_name == "Up" and ((.store.node_state_name // "Serving") == "Serving"))] | length' \
    2>/dev/null) || true
  if [[ "${PD_MEMBER_COUNT:-0}" == 3 ]] && [[ "${TIKV_SERVING_COUNT:-0}" == 3 ]]; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  echo "three PD members and three Up/Serving TiKV stores did not become ready" >&2
  tail -100 "${PLAYGROUND_LOG}" >&2
  exit 1
fi
OWNED_PIDS=$(merge_owned_pids)
if [[ -z "${OWNED_PIDS}" ]]; then
  echo "TiUP did not publish owned processes for ${TAG}" >&2
  exit 1
fi

# These are the only two inputs accepted by Rust. Every PD survivor, region,
# peer, store, and TiKV address is discovered and written back by that process.
export C11_PD_SEED="${PD_SEED}"
export C11_PHASE_DIR="${PHASE_DIR}"
cd "${RUST_ROOT}"
CARGO_BUILD_JOBS=12 cargo test -j12 -p difftest-transaction-tests \
  --test realtikv_region_retry \
  same_process_survives_pd_removal_and_region_leader_transfer \
  -- --ignored --exact --nocapture >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

wait_for_phase members-ready
MEMBERS_PHASE="${PHASE_DIR}/members-ready"
REMOVED_PD=$(phase_values "${MEMBERS_PHASE}" leader_url)
if [[ -z "${REMOVED_PD}" ]]; then
  echo "Rust members phase omitted the discovered PD leader URL" >&2
  exit 1
fi
REMOVED_PORT=$(url_port "${REMOVED_PD}")
if [[ -z "${REMOVED_PORT}" ]]; then
  echo "cannot derive removed PD port from ${REMOVED_PD}" >&2
  exit 1
fi
REMOVED_PID=$(lsof -nP -iTCP:"${REMOVED_PORT}" -sTCP:LISTEN -t | head -1 || true)
if [[ -z "${REMOVED_PID}" ]] \
  || ! ps -p "${REMOVED_PID}" -o command= | grep -F "${TAG_DIR}" >/dev/null; then
  echo "refusing to remove PD leader not owned by ${TAG}: ${REMOVED_PD}" >&2
  exit 1
fi
tiup playground scale-in -T "${TAG}" --pid "${REMOVED_PID}"
for _ in $(seq 1 60); do
  if ! curl -sf --max-time 1 "${REMOVED_PD}/pd/api/v1/version" >/dev/null; then
    break
  fi
  sleep 1
done
if curl -sf --max-time 1 "${REMOVED_PD}/pd/api/v1/version" >/dev/null; then
  echo "removed PD endpoint ${REMOVED_PD} is still reachable" >&2
  exit 1
fi
: >"${PHASE_DIR}/pd-removed"

wait_for_phase route-ready
ROUTE_PHASE="${PHASE_DIR}/route-ready"
SURVIVING_PD=$(phase_values "${ROUTE_PHASE}" active_pd)
if [[ -z "${SURVIVING_PD}" ]]; then
  echo "Rust route phase omitted its active surviving PD" >&2
  exit 1
fi
if [[ "${SURVIVING_PD}" == "${REMOVED_PD}" ]]; then
  echo "Rust did not select a surviving discovered PD member" >&2
  exit 1
fi
REGION_ID=$(phase_values "${ROUTE_PHASE}" region_id)
OLD_STORE=$(phase_values "${ROUTE_PHASE}" old_leader_store_id)
OLD_LEADER_ADDRESS=$(phase_values "${ROUTE_PHASE}" old_leader_address)
if [[ -z "${REGION_ID}" ]] || [[ -z "${OLD_STORE}" ]] || [[ -z "${OLD_LEADER_ADDRESS}" ]]; then
  echo "Rust route phase omitted region or old-leader identity" >&2
  exit 1
fi
TARGET_STORE=$(phase_values "${ROUTE_PHASE}" peer_store_id | awk -v old="${OLD_STORE}" '$1 != old { print; exit }')
if [[ -z "${TARGET_STORE}" ]]; then
  echo "region ${REGION_ID} has no alternate discovered TiKV peer" >&2
  exit 1
fi
TARGET_ADDRESS=$(phase_values "${ROUTE_PHASE}" store_route | \
  awk -F '\t' -v target="${TARGET_STORE}" '$1 == target { print $2; exit }')
if [[ -z "${TARGET_ADDRESS}" ]]; then
  echo "Rust route phase has no address for target store ${TARGET_STORE}" >&2
  exit 1
fi
tiup ctl:v8.5.6 pd -u "${SURVIVING_PD}" operator add transfer-leader \
  "${REGION_ID}" "${TARGET_STORE}"
transferred=false
for _ in $(seq 1 120); do
  CURRENT_STORE=$(curl -sf --max-time 2 \
    "${SURVIVING_PD}/pd/api/v1/region/id/${REGION_ID}" | jq -r '.leader.store_id // 0') || true
  if [[ "${CURRENT_STORE}" == "${TARGET_STORE}" ]]; then
    transferred=true
    break
  fi
  sleep 0.5
done
if [[ "${transferred}" != true ]]; then
  echo "region ${REGION_ID} did not transfer from ${OLD_STORE} to ${TARGET_STORE}" >&2
  exit 1
fi
: >"${PHASE_DIR}/region-moved"

wait "${RUST_PID}" || {
  echo "Campaign 11 Rust movement proof failed" >&2
  tail -120 "${RUST_LOG}" >&2
  exit 1
}
RUST_PID=
wait_for_phase completed
COMPLETED_PHASE="${PHASE_DIR}/completed"
TIKV_CALL_COUNT=$(phase_values "${COMPLETED_PHASE}" tikv_address | awk 'END { print NR }')
FIRST_TIKV=$(phase_values "${COMPLETED_PHASE}" tikv_address | head -1)
LAST_TIKV=$(phase_values "${COMPLETED_PHASE}" tikv_address | tail -1)
if [[ -z "${FIRST_TIKV}" ]] || [[ -z "${LAST_TIKV}" ]] \
  || [[ "${TIKV_CALL_COUNT}" -lt 2 ]] \
  || [[ "${FIRST_TIKV}" != "${OLD_LEADER_ADDRESS}" ]] \
  || [[ "${LAST_TIKV}" != "${TARGET_ADDRESS}" ]]; then
  echo "Rust did not prove old-to-new TiKV leader selection" >&2
  echo "expected ${OLD_LEADER_ADDRESS} -> ${TARGET_ADDRESS}, observed ${FIRST_TIKV:-<empty>} -> ${LAST_TIKV:-<empty>}" >&2
  exit 1
fi
echo "Campaign 11 movement proof passed: PD ${REMOVED_PD} -> ${SURVIVING_PD}; TiKV ${FIRST_TIKV} -> ${LAST_TIKV}"
