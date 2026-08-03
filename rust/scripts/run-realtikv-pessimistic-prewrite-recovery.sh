#!/usr/bin/env bash

# The cluster proof for TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY (cde12e0033).
#
# Three claims against a real TiKV, none of which a scripted store can answer:
#   * an EXPIRED pessimistic lock is resolved by an optimistic Prewrite and the
#     writer commits (the availability gap the gate holds shut),
#   * a LIVE pessimistic lock is refused, NOT rolled back, and its owner still
#     commits its own value afterwards (the safety claim the gate exists for),
#   * with the gate unset the same fixture reproduces the recorded refusal.
#
# The gate is read once per process (std::sync::LazyLock), so the first two
# claims and the third CANNOT share a `cargo test` invocation: this runs the
# test binary twice against one playground, once with the variable set and once
# without.
#
# Starts an owned TiUP playground (PD + 3 TiKV, no TiDB), runs the ignored
# Rust proofs against it, then unconditionally tears the playground down and
# verifies nothing it owned survived.

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="realtikv-pessimistic-prewrite-recovery-${$}"
# `--kv 3` claims 20160, 20161 and 20162 above the offset, and each TiKV also
# claims a status port 20 above its own, so 20182 is the highest port an offset
# has to keep inside 65535. Any default above 45353 makes the run refuse itself
# before it starts, which is exactly what 46000 did.
PORT_OFFSET=${PESSIMISTIC_PREWRITE_RECOVERY_PORT_OFFSET:-45000}
PD_PORT=$((2379 + PORT_OFFSET))
KV_PORT=$((20160 + PORT_OFFSET))
KV_HIGHEST_PORT=$((20182 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
RUST_ON_LOG="${TMPDIR:-/tmp}/${TAG}-rust-gate-on.log"
RUST_OFF_LOG="${TMPDIR:-/tmp}/${TAG}-rust-gate-off.log"
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
    && [[ "${TAG}" == realtikv-pessimistic-prewrite-recovery-* ]]
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
    echo "prewrite-recovery cleanup failed: tiup clean failed for ${TAG}" >&2
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
    echo "prewrite-recovery cleanup left an owned process or TiUP row" >&2
    cleanup_failed=true
  fi
  local address
  for address in ${STORE_ADDRESSES}; do
    if endpoint_reachable "${address}"; then
      echo "prewrite-recovery cleanup left TiKV ${address} reachable" >&2
      cleanup_failed=true
    fi
  done
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "prewrite-recovery cleanup left PD ${PD_ADDR} reachable" >&2
    cleanup_failed=true
  fi
  if ! validate_owned_paths; then
    echo "prewrite-recovery cleanup refused unsafe paths" >&2
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
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_ON_LOG}" "${RUST_OFF_LOG}"
  else
    echo "prewrite-recovery retained logs: ${PLAYGROUND_LOG} ${RUST_ON_LOG} ${RUST_OFF_LOG}" >&2
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
  || (( PORT_OFFSET < 1000 || PD_PORT > 65535 || KV_HIGHEST_PORT > 65535 )); then
  echo "PESSIMISTIC_PREWRITE_RECOVERY_PORT_OFFSET must be numeric, at least 1000, and keep ports valid" >&2
  exit 2
fi
for command in tiup curl jq nc pgrep cargo awk; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "prewrite-recovery requires ${command}" >&2
    exit 1
  fi
done
if endpoint_reachable "${PD_ADDR}" || endpoint_reachable "127.0.0.1:${KV_PORT}"; then
  echo "refusing occupied endpoints; set PESSIMISTIC_PREWRITE_RECOVERY_PORT_OFFSET" >&2
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

# Pass 1: the gate is ON. Both claims run in one process, single-threaded so
# the two fixtures cannot interleave their PD timestamps.
PESSIMISTIC_PREWRITE_RECOVERY_PD_ADDR="${PD_ADDR}" \
  TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY=1 \
  CARGO_BUILD_JOBS=12 cargo test --offline --locked -j12 -p tidb-txnkv \
    --test all \
    pessimistic_prewrite_recovery_realtikv_source:: \
    -- --ignored --nocapture --test-threads 1 \
    --skip the_gate_off_run_reproduces_the_recorded_refusal >"${RUST_ON_LOG}" 2>&1 &
RUST_PID=$!
wait "${RUST_PID}" || {
  RUST_PID=
  echo "gate-ON pessimistic prewrite recovery proof failed" >&2
  tail -220 "${RUST_ON_LOG}" >&2
  exit 1
}
RUST_PID=

# Pass 2: the gate is OFF, in a NEW process -- the flag is read once per
# process, so this cannot be folded into pass 1.
env -u TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY \
  PESSIMISTIC_PREWRITE_RECOVERY_PD_ADDR="${PD_ADDR}" \
  CARGO_BUILD_JOBS=12 cargo test --offline --locked -j12 -p tidb-txnkv \
    --test all \
    pessimistic_prewrite_recovery_realtikv_source::the_gate_off_run_reproduces_the_recorded_refusal \
    -- --ignored --exact --nocapture --test-threads 1 >"${RUST_OFF_LOG}" 2>&1 &
RUST_PID=$!
wait "${RUST_PID}" || {
  RUST_PID=
  echo "gate-OFF pessimistic prewrite recovery proof failed" >&2
  tail -220 "${RUST_OFF_LOG}" >&2
  exit 1
}
RUST_PID=

RESOLVED=$(grep 'pessimistic_prewrite_recovery status=passed claim=expired_resolved ' "${RUST_ON_LOG}" | tail -1 || true)
SURVIVED=$(grep 'pessimistic_prewrite_recovery status=passed claim=live_lock_survived ' "${RUST_ON_LOG}" | tail -1 || true)
GATED=$(grep 'pessimistic_prewrite_recovery status=passed claim=gate_off_refusal ' "${RUST_OFF_LOG}" | tail -1 || true)
PHASES=$(cat "${RUST_ON_LOG}" "${RUST_OFF_LOG}" | grep 'pessimistic_prewrite_recovery phase=' || true)

if [[ "${RESOLVED}" != *"cluster_id="* ]] \
  || [[ "${RESOLVED}" != *"lock_start_ts="* ]] \
  || [[ "${RESOLVED}" != *"commit_ts="* ]]; then
  echo "receipt omitted the expired-lock recovery evidence" >&2
  tail -220 "${RUST_ON_LOG}" >&2
  exit 1
fi
if [[ "${SURVIVED}" != *"holder_commit_ts="* ]] \
  || [[ "${PHASES}" != *"phase=refused key=live"* ]]; then
  echo "receipt omitted the live-lock survival evidence, which is the safety claim" >&2
  tail -220 "${RUST_ON_LOG}" >&2
  exit 1
fi
if [[ "${GATED}" != *"outside bounded recovery"* ]]; then
  echo "receipt omitted the gate-off refusal evidence" >&2
  tail -220 "${RUST_OFF_LOG}" >&2
  exit 1
fi

echo "pessimistic prewrite recovery proved: ${RESOLVED}"
echo "pessimistic prewrite recovery safety proved: ${SURVIVED}"
echo "pessimistic prewrite recovery gate proved: ${GATED}"
