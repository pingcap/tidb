#!/usr/bin/env bash

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="campaign28-optimistic-2pc-${$}"
PORT_OFFSET=${C28_STAGE_B_PORT_OFFSET:-43000}
PD_PORT=$((2379 + PORT_OFFSET))
KV_PORT=$((20160 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PHASE_DIR="${TMPDIR:-/tmp}/${TAG}-phases"
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
    [[ "${TAG_DIR}" == "${SELF_TEST_ROOT}/"* ]] \
      && [[ "${PHASE_DIR}" == "${SELF_TEST_ROOT}/"* ]]
    return
  fi
  local tiup_data="${TIUP_HOME:-${HOME}/.tiup}/data"
  [[ "${TAG_DIR}" == "${tiup_data}/${TAG}" ]] \
    && [[ "${TAG}" == campaign28-optimistic-2pc-* ]] \
    && [[ "${PHASE_DIR}" == "${TMPDIR:-/tmp}/${TAG}-phases" ]]
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
    echo "Campaign 28 Stage B cleanup failed: tiup clean failed for ${TAG}" >&2
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
    echo "Campaign 28 Stage B cleanup left an owned process or TiUP row" >&2
    cleanup_failed=true
  fi
  local address
  for address in ${STORE_ADDRESSES}; do
    if endpoint_reachable "${address}"; then
      echo "Campaign 28 Stage B cleanup left TiKV ${address} reachable" >&2
      cleanup_failed=true
    fi
  done
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "Campaign 28 Stage B cleanup left PD ${PD_ADDR} reachable" >&2
    cleanup_failed=true
  fi
  if ! validate_owned_paths; then
    echo "Campaign 28 Stage B cleanup refused unsafe paths" >&2
    cleanup_failed=true
  elif [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}" "${PHASE_DIR}"
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
    echo "Campaign 28 Stage B retained logs: ${PLAYGROUND_LOG} ${RUST_LOG}" >&2
  fi
  if [[ "${cleanup_status}" -ne 0 ]]; then
    exit "${cleanup_status}"
  fi
  exit "${original_status}"
}

self_test_cleanup() {
  local unrelated_pid=
  SELF_TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/c28-stage-b-self-test.XXXXXX")
  TAG="campaign28-optimistic-2pc-self-test-${$}"
  TAG_DIR="${SELF_TEST_ROOT}/${TAG}"
  PHASE_DIR="${SELF_TEST_ROOT}/${TAG}-phases"
  mkdir -p "${TAG_DIR}" "${PHASE_DIR}"
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
    echo "Stage B self-test left the owned process alive" >&2
    kill "${unrelated_pid}" 2>/dev/null || true
    return 1
  fi
  if ! kill -0 "${unrelated_pid}" 2>/dev/null; then
    echo "Stage B self-test killed an unrelated process" >&2
    return 1
  fi
  kill "${unrelated_pid}" 2>/dev/null || true
  wait "${unrelated_pid}" 2>/dev/null || true
  if [[ -e "${TAG_DIR}" || -e "${PHASE_DIR}" ]]; then
    echo "Stage B self-test left owned paths" >&2
    return 1
  fi
  rmdir "${SELF_TEST_ROOT}"
  echo "Campaign 28 Stage B cleanup self-test passed"
}

wait_for_phase() {
  local name=$1
  for _ in $(seq 1 1200); do
    if [[ -f "${PHASE_DIR}/${name}" ]]; then
      return 0
    fi
    if [[ -n "${RUST_PID}" ]] && ! kill -0 "${RUST_PID}" 2>/dev/null; then
      echo "Stage B Rust proof exited while waiting for ${name}" >&2
      tail -180 "${RUST_LOG}" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for Stage B phase ${name}" >&2
  return 1
}

phase_value() {
  local name=$1
  awk -F= -v name="${name}" '$1 == name { print substr($0, length(name) + 2); exit }' \
    "${PHASE_DIR}/split-source"
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
  echo "C28_STAGE_B_PORT_OFFSET must be numeric, at least 1000, and keep ports valid" >&2
  exit 2
fi
for command in tiup curl jq nc pgrep cargo awk; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Campaign 28 Stage B requires ${command}" >&2
    exit 1
  fi
done
if endpoint_reachable "${PD_ADDR}" || endpoint_reachable "127.0.0.1:${KV_PORT}"; then
  echo "refusing occupied Stage B endpoints; set C28_STAGE_B_PORT_OFFSET" >&2
  exit 1
fi

mkdir -m 700 "${PHASE_DIR}"
trap cleanup EXIT INT TERM
tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
  --db 0 --pd 1 --kv 3 --tiflash 0 --port-offset "${PORT_OFFSET}" \
  >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

ready=false
for _ in $(seq 1 240); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    echo "Stage B TiUP playground exited before readiness" >&2
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
  echo "Stage B real PD/TiKV topology did not become ready" >&2
  tail -120 "${PLAYGROUND_LOG}" >&2
  exit 1
fi

cd "${RUST_ROOT}"
C28_STAGE_B_PD_ADDR="${PD_ADDR}" C28_STAGE_B_PHASE_DIR="${PHASE_DIR}" \
  CARGO_BUILD_JOBS=12 cargo test --offline --locked -j12 -p tidb-txnkv \
    --test optimistic_2pc_realtikv_source \
    normal_optimistic_2pc_commits_two_regions_and_cleans_conflict \
    -- --ignored --exact --nocapture >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

wait_for_phase split-source
SOURCE_REGION=$(phase_value region_id)
SPLIT_KEY_HEX=$(phase_value split_key_hex)
STALE_ADDRESS=$(phase_value stale_address)
if [[ -z "${SOURCE_REGION}" || -z "${SPLIT_KEY_HEX}" || -z "${STALE_ADDRESS}" ]]; then
  echo "Stage B split source omitted region ID, encoded key, or stale address" >&2
  exit 1
fi
INITIAL_REGION_COUNT=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/regions" \
  | jq -r '.count // 0')
SPLIT_OUTPUT=$(tiup ctl:v8.5.6 pd -u "http://${PD_ADDR}" \
  operator add split-region "${SOURCE_REGION}" --policy=usekey --keys "${SPLIT_KEY_HEX}" 2>&1) || {
    printf '%s\n' "${SPLIT_OUTPUT}" >&2
    exit 1
  }
split_ready=false
for _ in $(seq 1 120); do
  REGION_COUNT=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/regions" \
    | jq -r '.count // 0' 2>/dev/null) || true
  if [[ "${REGION_COUNT:-0}" -gt "${INITIAL_REGION_COUNT}" ]]; then
    split_ready=true
    break
  fi
  sleep 0.5
done
if [[ "${split_ready}" != true ]]; then
  echo "Stage B source region did not split at ${SPLIT_KEY_HEX}" >&2
  exit 1
fi

voters_ready=false
for _ in $(seq 1 120); do
  REGION_JSON=$(curl -sf --max-time 2 \
    "http://${PD_ADDR}/pd/api/v1/region/id/${SOURCE_REGION}" 2>/dev/null) || true
  VOTER_COUNT=$(printf '%s' "${REGION_JSON:-}" | jq -r \
    '[.peers[]? | select(((.role_name // "Voter") == "Voter") and ((.is_witness // false) == false))] | length' \
    2>/dev/null) || true
  if [[ "${VOTER_COUNT:-0}" -ge 3 ]]; then
    voters_ready=true
    break
  fi
  sleep 0.5
done
if [[ "${voters_ready}" != true ]]; then
  echo "Stage B source region did not acquire three non-witness voter peers" >&2
  exit 1
fi
OLD_LEADER_STORE=$(printf '%s' "${REGION_JSON}" | jq -r '.leader.store_id // 0')
STALE_STORE_ID=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
  | jq -r --arg address "${STALE_ADDRESS}" \
    '.stores[] | select(.store.address == $address) | .store.id' \
  | head -1)
NEW_LEADER_STORE=$(printf '%s' "${REGION_JSON}" | jq -r \
  --arg old "${OLD_LEADER_STORE}" --arg stale "${STALE_STORE_ID}" \
  '.peers[] | select(((.role_name // "Voter") == "Voter") and ((.is_witness // false) == false) and ((.store_id | tostring) != $old) and ((.store_id | tostring) != $stale)) | .store_id' \
  | head -1)
if [[ "${OLD_LEADER_STORE}" == 0 || -z "${STALE_STORE_ID}" || -z "${NEW_LEADER_STORE}" ]]; then
  echo "Stage B split region lacks a voter distinct from current and stale leaders" >&2
  exit 1
fi
tiup ctl:v8.5.6 pd -u "http://${PD_ADDR}" \
  operator add transfer-leader "${SOURCE_REGION}" "${NEW_LEADER_STORE}" >/dev/null
leader_changed=false
for _ in $(seq 1 120); do
  CURRENT_LEADER=$(curl -sf --max-time 2 \
    "http://${PD_ADDR}/pd/api/v1/region/id/${SOURCE_REGION}" \
    | jq -r '.leader.store_id // 0' 2>/dev/null) || true
  if [[ "${CURRENT_LEADER:-0}" == "${NEW_LEADER_STORE}" ]]; then
    leader_changed=true
    break
  fi
  sleep 0.5
done
if [[ "${leader_changed}" != true ]]; then
  echo "Stage B region ${SOURCE_REGION} leader did not transfer" >&2
  exit 1
fi
printf 'region_id=%s\nold_leader_store=%s\nnew_leader_store=%s\n' \
  "${SOURCE_REGION}" "${OLD_LEADER_STORE}" "${NEW_LEADER_STORE}" \
  >"${PHASE_DIR}/leader-transfer"
: >"${PHASE_DIR}/split-complete"

wait "${RUST_PID}" || {
  RUST_PID=
  echo "Campaign 28 Stage B real optimistic 2PC proof failed" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
}
RUST_PID=

MARKER=$(grep '^campaign28_optimistic_2pc status=passed ' "${RUST_LOG}" | tail -1 || true)
RECEIPTS=$(grep '^campaign28_optimistic_2pc phase=' "${RUST_LOG}" || true)
if [[ "${MARKER}" != *"cluster_id="* ]] \
  || [[ "${MARKER}" != *"primary_region="* ]] \
  || [[ "${MARKER}" != *"secondary_region="* ]] \
  || [[ "${MARKER}" != *"rollback_start_ts="* ]] \
  || [[ "${MARKER}" != *"older_lock_start_ts="* ]] \
  || [[ "${MARKER}" != *"newer_lock_start_ts="* ]] \
  || [[ "${MARKER}" != *"newer_lock_commit_ts="* ]] \
  || [[ "${RECEIPTS}" != *"phase=prewrite tag=3"* ]] \
  || [[ "${RECEIPTS}" != *"phase=prewrite_attempt tag=3"* ]] \
  || [[ "${RECEIPTS}" != *"phase=primary_commit tag=4"* ]] \
  || [[ "${RECEIPTS}" != *"phase=secondary_commit tag=4"* ]] \
  || [[ "${RECEIPTS}" != *"phase=rollback tag=8"* ]] \
  || [[ "${RECEIPTS}" != *"phase=prewrite_regroup key=c28-stage-b-"* ]] \
  || [[ "${RECEIPTS}" != *"stale_region="* ]] \
  || [[ "${RECEIPTS}" != *"stale_address="* ]] \
  || [[ "${RECEIPTS}" != *"confirmed_region="* ]] \
  || [[ "${RECEIPTS}" != *"confirmed_address="* ]] \
  || [[ "${RECEIPTS}" != *"request_id="* ]] \
  || [[ "${RECEIPTS}" != *"physical_address="* ]]; then
  echo "Stage B receipt omitted multi-region 2PC or rollback evidence" >&2
  tail -220 "${RUST_LOG}" >&2
  exit 1
fi

PRIMARY_REGION=$(printf '%s\n' "${MARKER}" | sed -E 's/.* primary_region=([0-9]+).*/\1/')
SECONDARY_REGION=$(printf '%s\n' "${MARKER}" | sed -E 's/.* secondary_region=([0-9]+).*/\1/')
if [[ "${PRIMARY_REGION}" == "${SECONDARY_REGION}" ]]; then
  echo "Stage B receipt used only one region" >&2
  exit 1
fi

echo "Campaign 28 Stage B optimistic 2PC passed: ${MARKER}"
