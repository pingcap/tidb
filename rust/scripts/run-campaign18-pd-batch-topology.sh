#!/usr/bin/env bash

set -euo pipefail

for prerequisite in tiup cargo curl jq nc lsof pgrep ps awk sed seq; do
  if ! command -v "${prerequisite}" >/dev/null 2>&1; then
    echo "missing Campaign 18 prerequisite: ${prerequisite}" >&2
    exit 1
  fi
done

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="campaign18-pd-batch-${$}-$(date +%s)"
PORT_OFFSET=${C18_PORT_OFFSET:-38000}
PD_SEED_PORT=$((2379 + PORT_OFFSET))
PD_SEED="127.0.0.1:${PD_SEED_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PHASE_DIR="${TMPDIR:-/tmp}/${TAG}-phases"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
RESTART_LOG="${TMPDIR:-/tmp}/${TAG}-tikv-restart.log"
PLAYGROUND_PID=
RUST_PID=
STOPPED_PID=
RESTART_PID=
OWNED_PIDS=

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
      tail -160 "${RUST_LOG}" >&2
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
    if [[ -n "${RESTART_PID}" ]]; then
      printf '%s\n' "${RESTART_PID}"
    fi
    tag_owned_pids
  } | awk 'NF && !seen[$1]++ { print $1 }' | tr '\n' ' '
}

address_port() {
  local address=$1
  address=${address%/}
  printf '%s\n' "${address##*:}"
}

cleanup() {
  local original_status=$?
  local cleanup_failed=false
  trap - EXIT INT TERM

  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    kill "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  if [[ -n "${STOPPED_PID}" ]] && kill -0 "${STOPPED_PID}" 2>/dev/null; then
    kill -CONT "${STOPPED_PID}" 2>/dev/null || true
    kill "${STOPPED_PID}" 2>/dev/null || true
  fi
  if [[ -n "${RESTART_PID}" ]] && kill -0 "${RESTART_PID}" 2>/dev/null; then
    kill "${RESTART_PID}" 2>/dev/null || true
    wait "${RESTART_PID}" 2>/dev/null || true
  fi
  OWNED_PIDS=$(merge_owned_pids)
  if [[ -n "${PLAYGROUND_PID}" ]] && kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    kill "${PLAYGROUND_PID}" 2>/dev/null || true
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "Campaign 18 cleanup failed: tiup clean failed for ${TAG}" >&2
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
    echo "Campaign 18 cleanup failed: owned process or TiUP registry row remains" >&2
    cleanup_failed=true
  fi

  local endpoint
  if [[ -f "${PHASE_DIR}/topology-ready" ]]; then
    while IFS= read -r endpoint; do
      if curl -sf --max-time 1 "${endpoint}/pd/api/v1/version" >/dev/null; then
        echo "Campaign 18 cleanup failed: PD endpoint ${endpoint} remains reachable" >&2
        cleanup_failed=true
      fi
    done < <(phase_values "${PHASE_DIR}/topology-ready" member_url)
  fi
  if [[ -f "${PHASE_DIR}/route-ready" ]]; then
    while IFS= read -r endpoint; do
      local port
      port=$(address_port "${endpoint}")
      if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
        echo "Campaign 18 cleanup failed: TiKV endpoint ${endpoint} remains reachable" >&2
        cleanup_failed=true
      fi
    done < <(phase_values "${PHASE_DIR}/route-ready" store_address)
  fi

  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}" "${PHASE_DIR}"
    if [[ -e "${TAG_DIR}" || -e "${PHASE_DIR}" ]]; then
      echo "Campaign 18 cleanup failed: owned data or phase directory remains" >&2
      cleanup_failed=true
    fi
  fi
  if [[ "${cleanup_failed}" == false ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG}" "${RESTART_LOG}"
  else
    echo "Campaign 18 retained logs: ${PLAYGROUND_LOG} ${RUST_LOG} ${RESTART_LOG}" >&2
  fi
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}
trap cleanup EXIT INT TERM

if curl -sf --max-time 1 "http://${PD_SEED}/pd/api/v1/version" >/dev/null; then
  echo "refusing occupied PD seed ${PD_SEED}; set C18_PORT_OFFSET" >&2
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
    tail -160 "${PLAYGROUND_LOG}" >&2
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
  tail -160 "${PLAYGROUND_LOG}" >&2
  exit 1
fi
OWNED_PIDS=$(merge_owned_pids)
if [[ -z "${OWNED_PIDS}" ]]; then
  echo "TiUP did not publish tag-owned processes for ${TAG}" >&2
  exit 1
fi

TOPOLOGY_PHASE=
while IFS= read -r endpoint; do
  TOPOLOGY_PHASE="${TOPOLOGY_PHASE}member_url=${endpoint}"$'\n'
done < <(curl -sf --max-time 2 "http://${PD_SEED}/pd/api/v1/members" \
  | jq -r '.members[].client_urls[]')
printf '%s' "${TOPOLOGY_PHASE}" >"${PHASE_DIR}/topology-ready"

export C18_PD_SEED="${PD_SEED}"
export C18_PHASE_DIR="${PHASE_DIR}"
cd "${RUST_ROOT}"
CARGO_BUILD_JOBS=12 cargo test -j12 -p difftest-transaction-tests \
  --test realtikv_replica_read \
  live_pd_prev_region_and_forwarded_batch_survive_same_address_restart \
  -- --ignored --exact --nocapture >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

wait_for_phase split-source
SOURCE_REGION=$(phase_values "${PHASE_DIR}/split-source" region_id)
SPLIT_KEY_HEX=$(phase_values "${PHASE_DIR}/split-source" split_key_hex)
if [[ -z "${SOURCE_REGION}" || -z "${SPLIT_KEY_HEX}" ]]; then
  echo "Rust split phase omitted source region or memcomparable split key" >&2
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
# The proof kills a follower, not a leader. Disable PD-initiated leader
# scheduling before the Rust side snapshots its exact leader/follower route so
# equal before/after leader IDs cannot hide a scheduler-driven ABA transfer.
tiup ctl:v8.5.6 pd -u "http://${PD_SEED}" \
  config set leader-schedule-limit 0 >/dev/null
LEADER_SCHEDULE_LIMIT=$(curl -sf --max-time 2 \
  "http://${PD_SEED}/pd/api/v1/config/schedule" \
  | jq -r '."leader-schedule-limit" // -1' 2>/dev/null) || true
if [[ "${LEADER_SCHEDULE_LIMIT}" != 0 ]]; then
  echo "failed to disable PD leader scheduling for Campaign 18" >&2
  exit 1
fi
: >"${PHASE_DIR}/split-complete"

wait_for_phase route-ready
ROUTE_PHASE="${PHASE_DIR}/route-ready"
LEFT_REGION_ID=$(phase_values "${ROUTE_PHASE}" left_region_id)
PHYSICAL_ADDRESS=$(phase_values "${ROUTE_PHASE}" physical_address)
PHYSICAL_STORE_ID=$(phase_values "${ROUTE_PHASE}" physical_store_id)
LOGICAL_ADDRESS=$(phase_values "${ROUTE_PHASE}" logical_address)
LOGICAL_STORE_ID=$(phase_values "${ROUTE_PHASE}" logical_store_id)
LOGICAL_PEER_ID=$(phase_values "${ROUTE_PHASE}" logical_peer_id)
GENERATION_N=$(phase_values "${ROUTE_PHASE}" generation_n)
FORWARDED_CHANNEL_VERSION=$(phase_values "${ROUTE_PHASE}" forwarded_channel_version)
DIRECT_GENERATION=$(phase_values "${ROUTE_PHASE}" direct_generation)
if [[ -z "${LEFT_REGION_ID}" || -z "${PHYSICAL_ADDRESS}" || -z "${PHYSICAL_STORE_ID}" \
  || -z "${LOGICAL_ADDRESS}" || -z "${LOGICAL_STORE_ID}" || -z "${LOGICAL_PEER_ID}" \
  || -z "${GENERATION_N}" \
  || -z "${FORWARDED_CHANNEL_VERSION}" \
  || -z "${DIRECT_GENERATION}" \
  || "${PHYSICAL_ADDRESS}" == "${LOGICAL_ADDRESS}" ]]; then
  echo "Rust route phase did not prove distinct physical-proxy and logical-follower endpoints" >&2
  cat "${ROUTE_PHASE}" >&2
  exit 1
fi
LOGICAL_PORT=$(address_port "${LOGICAL_ADDRESS}")
STOPPED_PID=$(lsof -nP -iTCP:"${LOGICAL_PORT}" -sTCP:LISTEN -t | head -1 || true)
if [[ -z "${STOPPED_PID}" ]] \
  || ! ps -ww -p "${STOPPED_PID}" -o command= | grep -F "${TAG_DIR}" >/dev/null \
  || ! ps -ww -p "${STOPPED_PID}" -o command= | grep -F tikv-server >/dev/null; then
  echo "refusing to freeze TiKV not owned by ${TAG}: ${LOGICAL_ADDRESS}" >&2
  exit 1
fi
TIKV_COMMAND=$(ps -ww -p "${STOPPED_PID}" -o command=)
if [[ -z "${TIKV_COMMAND}" || "${TIKV_COMMAND}" == *$'\n'* \
  || "${TIKV_COMMAND}" != *"${TAG_DIR}"* \
  || "${TIKV_COMMAND}" != *"${LOGICAL_PORT}"* \
  || "${TIKV_COMMAND}" =~ [^[:alnum:][:space:]/._:=,@%+-] ]]; then
  echo "cannot capture a deterministic same-address tag-owned TiKV command" >&2
  exit 1
fi
REGION_ENDPOINT="http://${PD_SEED}/pd/api/v1/region/id/${LEFT_REGION_ID}"
BASELINE_LEADER_STORE_ID=$(curl -sf --max-time 2 "${REGION_ENDPOINT}" \
  | jq -r '.leader.store_id // 0' 2>/dev/null) || true
if [[ "${BASELINE_LEADER_STORE_ID:-0}" == 0 \
  || "${BASELINE_LEADER_STORE_ID}" == "${LOGICAL_STORE_ID}" ]]; then
  echo "left region ${LEFT_REGION_ID} has no stable non-target leader before follower freeze" >&2
  curl -sf --max-time 2 "${REGION_ENDPOINT}" >&2 || true
  exit 1
fi
kill -STOP "${STOPPED_PID}"
FROZEN_LEADER_STORE_ID=$(curl -sf --max-time 2 "${REGION_ENDPOINT}" \
  | jq -r '.leader.store_id // 0' 2>/dev/null) || true
if [[ "${FROZEN_LEADER_STORE_ID:-0}" != "${BASELINE_LEADER_STORE_ID}" \
  || "${FROZEN_LEADER_STORE_ID}" == "${LOGICAL_STORE_ID}" ]]; then
  kill -CONT "${STOPPED_PID}" 2>/dev/null || true
  echo "left region ${LEFT_REGION_ID} leader changed while freezing logical follower" >&2
  curl -sf --max-time 2 "${REGION_ENDPOINT}" >&2 || true
  exit 1
fi
: >"${PHASE_DIR}/logical-target-frozen"

wait_for_phase request-published
PRE_KILL_LEADER_STORE_ID=$(curl -sf --max-time 2 "${REGION_ENDPOINT}" \
  | jq -r '.leader.store_id // 0' 2>/dev/null) || true
if [[ "${PRE_KILL_LEADER_STORE_ID:-0}" != "${BASELINE_LEADER_STORE_ID}" \
  || "${PRE_KILL_LEADER_STORE_ID}" == "${LOGICAL_STORE_ID}" ]]; then
  kill -CONT "${STOPPED_PID}" 2>/dev/null || true
  echo "left region ${LEFT_REGION_ID} leader changed before logical follower kill" >&2
  curl -sf --max-time 2 "${REGION_ENDPOINT}" >&2 || true
  exit 1
fi
kill -KILL "${STOPPED_PID}"
for _ in $(seq 1 60); do
  if ! kill -0 "${STOPPED_PID}" 2>/dev/null \
    && ! nc -z -w 1 127.0.0.1 "${LOGICAL_PORT}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if kill -0 "${STOPPED_PID}" 2>/dev/null \
  || nc -z -w 1 127.0.0.1 "${LOGICAL_PORT}" >/dev/null 2>&1; then
  echo "frozen logical TiKV ${LOGICAL_ADDRESS} did not stop" >&2
  exit 1
fi
STOPPED_PID=
: >"${PHASE_DIR}/logical-target-stopped"

STOPPED_LEADER_STORE_ID=$(curl -sf --max-time 2 "${REGION_ENDPOINT}" \
  | jq -r '.leader.store_id // 0' 2>/dev/null) || true
if [[ "${STOPPED_LEADER_STORE_ID:-0}" != "${BASELINE_LEADER_STORE_ID}" \
  || "${STOPPED_LEADER_STORE_ID}" == "${LOGICAL_STORE_ID}" ]]; then
  echo "killing logical follower changed leader for left region ${LEFT_REGION_ID}" >&2
  curl -sf --max-time 2 "${REGION_ENDPOINT}" >&2 || true
  exit 1
fi

wait_for_phase failure-observed
FAILED_ROUTE_GENERATION=$(phase_values "${PHASE_DIR}/failure-observed" failed_route_generation)
FAILED_CHANNEL_VERSION=$(phase_values "${PHASE_DIR}/failure-observed" failed_channel_version)
FAILURE_COUNT=$(phase_values "${PHASE_DIR}/failure-observed" failure_count)
TRANSPORT_SCHEDULED_RESENDS=$(phase_values "${PHASE_DIR}/failure-observed" transport_scheduled_resends)
SURVIVING_DIRECT_GENERATION=$(phase_values "${PHASE_DIR}/failure-observed" direct_generation)
DIRECT_SURVIVED=$(phase_values "${PHASE_DIR}/failure-observed" direct_survived)
if [[ "${FAILED_ROUTE_GENERATION}" != "${GENERATION_N}" \
  || "${FAILED_CHANNEL_VERSION}" != "${FORWARDED_CHANNEL_VERSION}" \
  || "${FAILURE_COUNT}" != 1 || "${TRANSPORT_SCHEDULED_RESENDS}" != 0 \
  || "${DIRECT_SURVIVED}" != true \
  || "${SURVIVING_DIRECT_GENERATION}" != "${DIRECT_GENERATION}" ]]; then
  echo "generation N failure, no-resend, or direct/forwarded isolation proof failed" >&2
  cat "${PHASE_DIR}/failure-observed" >&2
  exit 1
fi

# TiUP has no playground restart subcommand. The command was captured only
# after proving its PID, data path, binary, and address belong to this tag.
/bin/sh -c "exec ${TIKV_COMMAND}" >>"${RESTART_LOG}" 2>&1 &
RESTART_PID=$!
restarted=false
for _ in $(seq 1 120); do
  if ! kill -0 "${RESTART_PID}" 2>/dev/null; then
    echo "same-address TiKV restart exited before readiness" >&2
    tail -160 "${RESTART_LOG}" >&2
    exit 1
  fi
  STORE_STATE=$(curl -sf --max-time 2 "http://${PD_SEED}/pd/api/v1/stores" \
    | jq -r --argjson id "${LOGICAL_STORE_ID}" \
      '.stores[] | select(.store.id == $id) | [.store.state_name, (.store.node_state_name // "Serving"), .store.address] | @tsv' \
      2>/dev/null) || true
  REGION_STATE=$(curl -sf --max-time 2 "${REGION_ENDPOINT}" \
    | jq -r --argjson store_id "${LOGICAL_STORE_ID}" --argjson peer_id "${LOGICAL_PEER_ID}" \
      '[.leader.store_id // 0, (any(.peers[]?; .store_id == $store_id and .id == $peer_id and .role_name == "Voter")), (any(.pending_peers[]?; .store_id == $store_id or .id == $peer_id)), (any(.down_peers[]?; .peer.store_id == $store_id or .peer.id == $peer_id))] | @tsv' \
      2>/dev/null) || true
  if nc -z -w 1 127.0.0.1 "${LOGICAL_PORT}" >/dev/null 2>&1 \
    && [[ "${STORE_STATE}" == $'Up\tServing\t'"${LOGICAL_ADDRESS}" ]] \
    && [[ "${REGION_STATE}" == "${BASELINE_LEADER_STORE_ID}"$'\ttrue\tfalse\tfalse' ]]; then
    LISTENER_PID=$(lsof -nP -iTCP:"${LOGICAL_PORT}" -sTCP:LISTEN -t | head -1 || true)
    if [[ "${LISTENER_PID}" == "${RESTART_PID}" ]]; then
      restarted=true
      break
    fi
  fi
  sleep 0.5
done
if [[ "${restarted}" != true ]]; then
  echo "same-address TiKV ${LOGICAL_ADDRESS} did not return as a ready peer under unchanged leader ${BASELINE_LEADER_STORE_ID}" >&2
  tail -160 "${RESTART_LOG}" >&2
  exit 1
fi
RESTARTED_LEADER_STORE_ID=$(curl -sf --max-time 2 "${REGION_ENDPOINT}" \
  | jq -r '.leader.store_id // 0' 2>/dev/null) || true
if [[ "${RESTARTED_LEADER_STORE_ID:-0}" != "${BASELINE_LEADER_STORE_ID}" ]]; then
  echo "left region ${LEFT_REGION_ID} leader changed after logical follower restart" >&2
  curl -sf --max-time 2 "${REGION_ENDPOINT}" >&2 || true
  exit 1
fi
if ! ps -ww -p "${RESTART_PID}" -o command= | grep -F "${TAG_DIR}" >/dev/null; then
  echo "restarted TiKV is not tag-owned" >&2
  exit 1
fi
printf 'restart_pid=%s\naddress=%s\nleader_store_id=%s\n' \
  "${RESTART_PID}" "${LOGICAL_ADDRESS}" "${BASELINE_LEADER_STORE_ID}" \
  >"${PHASE_DIR}/tikv-restarted"

wait "${RUST_PID}" || {
  echo "Campaign 18 Rust live proof failed" >&2
  tail -200 "${RUST_LOG}" >&2
  exit 1
}
RUST_PID=
wait_for_phase completed
COMPLETED_LEADER_STORE_ID=$(curl -sf --max-time 2 "${REGION_ENDPOINT}" \
  | jq -r '.leader.store_id // 0' 2>/dev/null) || true
if [[ "${COMPLETED_LEADER_STORE_ID:-0}" != "${BASELINE_LEADER_STORE_ID}" ]]; then
  echo "left region ${LEFT_REGION_ID} leader changed during restarted-follower retry" >&2
  curl -sf --max-time 2 "${REGION_ENDPOINT}" >&2 || true
  exit 1
fi
COMPLETED="${PHASE_DIR}/completed"
INITIAL_ROUTE_GENERATION=$(phase_values "${COMPLETED}" initial_route_generation)
COMPLETED_FAILED_ROUTE_GENERATION=$(phase_values "${COMPLETED}" failed_route_generation)
RETRY_ROUTE_GENERATION=$(phase_values "${COMPLETED}" retry_route_generation)
INITIAL_CHANNEL_VERSION=$(phase_values "${COMPLETED}" initial_channel_version)
COMPLETED_FAILED_CHANNEL_VERSION=$(phase_values "${COMPLETED}" failed_channel_version)
RETRY_CHANNEL_VERSION=$(phase_values "${COMPLETED}" retry_channel_version)
ADJACENT=$(phase_values "${COMPLETED}" adjacent)
RETRY_USABLE=$(phase_values "${COMPLETED}" retry_usable)
PLAINTEXT_ONLY=$(phase_values "${COMPLETED}" plaintext_only)
COMPLETED_DIRECT_SURVIVED=$(phase_values "${COMPLETED}" direct_survived)
EXACT_PEER_READINESS=$(phase_values "${COMPLETED}" exact_peer_readiness)
COMPLETED_SCHEDULED_RESENDS=$(phase_values "${COMPLETED}" transport_scheduled_resends)
if [[ "${ADJACENT}" != true || "${RETRY_USABLE}" != true \
  || "${PLAINTEXT_ONLY}" != true || "${COMPLETED_DIRECT_SURVIVED}" != true \
  || "${EXACT_PEER_READINESS}" != true \
  || "${COMPLETED_SCHEDULED_RESENDS}" != 0 \
  || "${INITIAL_ROUTE_GENERATION}" != "${GENERATION_N}" \
  || "${COMPLETED_FAILED_ROUTE_GENERATION}" != "${GENERATION_N}" \
  || "${RETRY_ROUTE_GENERATION:-0}" -le "${INITIAL_ROUTE_GENERATION:-0}" \
  || "${INITIAL_CHANNEL_VERSION}" != "${FORWARDED_CHANNEL_VERSION}" \
  || "${COMPLETED_FAILED_CHANNEL_VERSION}" != "${FORWARDED_CHANNEL_VERSION}" \
  || "${RETRY_CHANNEL_VERSION}" != "${FORWARDED_CHANNEL_VERSION}" ]]; then
  echo "Campaign 18 completion marker is incomplete" >&2
  cat "${COMPLETED}" >&2
  exit 1
fi
echo "Campaign 18 live proof passed: GetPrevRegion adjacency; forwarded ${PHYSICAL_ADDRESS} -> follower ${LOGICAL_ADDRESS} under unchanged leader store ${BASELINE_LEADER_STORE_ID}; route generation ${INITIAL_ROUTE_GENERATION} failed once on channel ${INITIAL_CHANNEL_VERSION}; caller retry route generation ${RETRY_ROUTE_GENERATION} succeeded"
