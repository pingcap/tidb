#!/usr/bin/env bash

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="campaign12-realtikv-${$}"
PORT_OFFSET=${C12_PORT_OFFSET:-26000}
PD_SEED_PORT=$((2379 + PORT_OFFSET))
PD_SEED="127.0.0.1:${PD_SEED_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PHASE_DIR="${TMPDIR:-/tmp}/${TAG}-phases"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
PLAYGROUND_PID=
RUST_PID=
LEADER_PID=

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
      tail -120 "${RUST_LOG}" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for Rust phase ${name}" >&2
  return 1
}

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

  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    kill "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "Campaign 12 cleanup failed: tiup clean failed for ${TAG}" >&2
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
    echo "Campaign 12 cleanup failed: owned process or registry row remains" >&2
    cleanup_failed=true
  fi

  local endpoint
  if [[ -f "${PHASE_DIR}/route-ready" ]]; then
    while IFS= read -r endpoint; do
      local port=${endpoint##*:}
      if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
        echo "Campaign 12 cleanup failed: TiKV ${endpoint} remains reachable" >&2
        cleanup_failed=true
      fi
    done < <(phase_values "${PHASE_DIR}/route-ready" store_address)
  fi
  if curl -sf --max-time 1 "http://${PD_SEED}/pd/api/v1/version" >/dev/null; then
    echo "Campaign 12 cleanup failed: PD ${PD_SEED} remains reachable" >&2
    cleanup_failed=true
  fi
  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}" "${PHASE_DIR}"
  fi
  rm -f "${PLAYGROUND_LOG}" "${RUST_LOG}"
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}
trap cleanup EXIT INT TERM

if curl -sf --max-time 1 "http://${PD_SEED}/pd/api/v1/version" >/dev/null; then
  echo "refusing occupied PD seed ${PD_SEED}; set C12_PORT_OFFSET" >&2
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
    tail -120 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  PD_COUNT=$(curl -sf --max-time 2 "http://${PD_SEED}/pd/api/v1/members" \
    | jq -r '.members | length' 2>/dev/null) || true
  TIKV_COUNT=$(curl -sf --max-time 2 "http://${PD_SEED}/pd/api/v1/stores" \
    | jq -r '[.stores[] | select(.store.state_name == "Up" and ((.store.node_state_name // "Serving") == "Serving"))] | length' \
      2>/dev/null) || true
  if [[ "${PD_COUNT:-0}" == 3 ]] && [[ "${TIKV_COUNT:-0}" == 3 ]]; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  echo "three PD members and three Up/Serving TiKV stores did not become ready" >&2
  tail -120 "${PLAYGROUND_LOG}" >&2
  exit 1
fi
if [[ -z "$(tag_owned_pids)" ]]; then
  echo "TiUP did not publish owned processes for ${TAG}" >&2
  exit 1
fi

export C12_PD_SEED="${PD_SEED}"
export C12_PHASE_DIR="${PHASE_DIR}"
cd "${RUST_ROOT}"
CARGO_BUILD_JOBS=12 cargo test -j12 -p difftest-transaction-tests \
  --test realtikv_transport_retry \
  one_lazy_response_recovers_after_its_cached_tikv_leader_stops \
  -- --ignored --exact --nocapture >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

wait_for_phase split-source
SOURCE_REGION=$(phase_values "${PHASE_DIR}/split-source" region_id)
SPLIT_KEY_HEX=74800000000000002a5f728000000000000000
if [[ -z "${SOURCE_REGION}" ]]; then
  echo "Rust split phase omitted the source region" >&2
  exit 1
fi
INITIAL_REGION_COUNT=$(curl -sf --max-time 2 "http://${PD_SEED}/pd/api/v1/regions" \
  | jq -r '.count // 0')
SPLIT_OUTPUT=$(tiup ctl:v8.5.6 pd -u "http://${PD_SEED}" \
  operator add split-region "${SOURCE_REGION}" --policy=usekey --keys "${SPLIT_KEY_HEX}" 2>&1) || {
  printf '%s\n' "${SPLIT_OUTPUT}" >&2
  exit 1
}
split_ready=false
for _ in $(seq 1 120); do
  REGION_COUNT=$(curl -sf --max-time 2 "http://${PD_SEED}/pd/api/v1/regions" \
    | jq -r '.count // 0' 2>/dev/null) || true
  if [[ "${REGION_COUNT:-0}" -gt "${INITIAL_REGION_COUNT}" ]]; then
    split_ready=true
    break
  fi
  sleep 0.5
done
if [[ "${split_ready}" != true ]]; then
  echo "region ${SOURCE_REGION} did not split at ${SPLIT_KEY_HEX}" >&2
  printf '%s\n' "${SPLIT_OUTPUT}" >&2
  exit 1
fi
: >"${PHASE_DIR}/split-complete"

wait_for_phase split-regions
SPLIT_PHASE="${PHASE_DIR}/split-regions"
REGION_IDS=$(phase_values "${SPLIT_PHASE}" region_id)
if [[ $(printf '%s\n' "${REGION_IDS}" | awk 'NF { count++ } END { print count + 0 }') -ne 2 ]]; then
  echo "Rust did not discover exactly two split regions" >&2
  cat "${SPLIT_PHASE}" >&2
  exit 1
fi
COMMON_STORE=$(phase_values "${SPLIT_PHASE}" region_peer \
  | awk -F '\t' '{ count[$2]++ } END { for (store in count) if (count[store] == 2) { print store; exit } }')
if [[ -z "${COMMON_STORE}" ]]; then
  echo "split regions have no common voter store" >&2
  cat "${SPLIT_PHASE}" >&2
  exit 1
fi

aligned=false
for _ in $(seq 1 120); do
  for region_id in ${REGION_IDS}; do
    tiup ctl:v8.5.6 pd -u "http://${PD_SEED}" \
      operator add transfer-leader "${region_id}" "${COMMON_STORE}" >/dev/null 2>&1 || true
  done
  aligned=true
  for region_id in ${REGION_IDS}; do
    CURRENT_STORE=$(curl -sf --max-time 2 \
      "http://${PD_SEED}/pd/api/v1/region/id/${region_id}" \
      | jq -r '.leader.store_id // 0' 2>/dev/null) || true
    if [[ "${CURRENT_STORE:-0}" != "${COMMON_STORE}" ]]; then
      aligned=false
      break
    fi
  done
  if [[ "${aligned}" == true ]]; then
    break
  fi
  sleep 0.5
done
if [[ "${aligned}" != true ]]; then
  echo "split-region leaders did not align on store ${COMMON_STORE}" >&2
  exit 1
fi
: >"${PHASE_DIR}/leaders-aligned"

wait_for_phase route-ready
ROUTE_PHASE="${PHASE_DIR}/route-ready"
OLD_ADDRESS=$(phase_values "${ROUTE_PHASE}" old_leader_address)
if [[ -z "${OLD_ADDRESS}" ]]; then
  echo "Rust route phase omitted the cached leader address" >&2
  exit 1
fi
OLD_PORT=${OLD_ADDRESS##*:}
LEADER_PID=$(lsof -nP -iTCP:"${OLD_PORT}" -sTCP:LISTEN -t | head -1 || true)
if [[ -z "${LEADER_PID}" ]] \
  || ! ps -p "${LEADER_PID}" -o command= | grep -F "${TAG_DIR}" >/dev/null; then
  echo "refusing to stop TiKV not owned by ${TAG}: ${OLD_ADDRESS}" >&2
  exit 1
fi
kill "${LEADER_PID}"
for _ in $(seq 1 60); do
  if ! kill -0 "${LEADER_PID}" 2>/dev/null \
    && ! nc -z -w 1 127.0.0.1 "${OLD_PORT}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if kill -0 "${LEADER_PID}" 2>/dev/null \
  || nc -z -w 1 127.0.0.1 "${OLD_PORT}" >/dev/null 2>&1; then
  echo "cached leader ${OLD_ADDRESS} did not stop" >&2
  exit 1
fi
: >"${PHASE_DIR}/leader-stopped"

wait "${RUST_PID}" || {
  echo "Campaign 12 Rust transport-retry proof failed" >&2
  tail -160 "${RUST_LOG}" >&2
  exit 1
}
RUST_PID=
wait_for_phase completed
COMPLETED="${PHASE_DIR}/completed"
FAILED_ADDRESS=$(phase_values "${COMPLETED}" failed_address)
FAILED_GENERATION=$(phase_values "${COMPLETED}" failed_generation)
SURVIVOR_ADDRESS=$(phase_values "${COMPLETED}" survivor_address)
STALE_FUTURE=$(phase_values "${COMPLETED}" stale_future_dispatches)
LIVENESS=$(phase_values "${COMPLETED}" recovered_store_liveness)
SURVIVOR_DISPATCHES=$(phase_values "${COMPLETED}" survivor_dispatches)
STRUCTURED_RESULTS=$(phase_values "${COMPLETED}" structured_results)
if [[ "${FAILED_ADDRESS}" != "${OLD_ADDRESS}" ]] \
  || [[ -z "${FAILED_GENERATION}" ]] \
  || [[ -z "${SURVIVOR_ADDRESS}" ]] \
  || [[ "${SURVIVOR_ADDRESS}" == "${OLD_ADDRESS}" ]] \
  || [[ "${SURVIVOR_DISPATCHES:-0}" -lt 2 ]] \
  || [[ "${STALE_FUTURE}" != 0 ]] \
  || [[ "${LIVENESS}" == Reachable ]] \
  || [[ "${STRUCTURED_RESULTS}" != 2 ]]; then
  echo "Campaign 12 marker did not prove exact-generation alternate-peer recovery" >&2
  cat "${COMPLETED}" >&2
  exit 1
fi
echo "Campaign 12 transport retry passed: ${FAILED_ADDRESS}#${FAILED_GENERATION} -> ${SURVIVOR_ADDRESS}; survivor_dispatches=${SURVIVOR_DISPATCHES}; liveness=${LIVENESS}"
