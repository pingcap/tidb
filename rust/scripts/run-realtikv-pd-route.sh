#!/usr/bin/env bash

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="realtikv-pd-route-${$}"
PORT_OFFSET=${PD_ROUTE_PORT_OFFSET:-21000}
PD_PORT=$((2379 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
TIKV_PORT=$((20160 + PORT_OFFSET))
TIKV_ADDR="127.0.0.1:${TIKV_PORT}"
PLAYGROUND_PID=
OWNED_PIDS=
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}.log"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"

tag_status_rows() {
  tiup status | awk -v tag="${TAG}" \
    'NR > 2 && ($1 == tag || index($0, "/data/" tag "/")) { print }'
}

tag_owned_pids() {
  pgrep -f "${TAG_DIR}" || true
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

cleanup() {
  local original_status=$?
  local cleanup_failed=false
  trap - EXIT INT TERM
  OWNED_PIDS=$(merge_owned_pids)
  if [[ -n "${PLAYGROUND_PID}" ]]; then
    if kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
      kill "${PLAYGROUND_PID}" 2>/dev/null || true
    fi
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
    echo "PD-route cleanup failed: tiup clean failed for ${TAG}" >&2
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
    if ! status_rows=$(tag_status_rows 2>/dev/null); then
      sleep 1
      continue
    fi
    if [[ "${process_alive}" == false ]] && [[ -z "${status_rows}" ]]; then
      processes_cleaned=true
      break
    fi
    sleep 1
  done
  if [[ "${processes_cleaned}" != true ]]; then
    echo "PD-route cleanup failed: owned TiUP registry or process remains for ${TAG}" >&2
    cleanup_failed=true
  fi
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "PD-route cleanup failed: owned PD endpoint ${PD_ADDR} is still reachable" >&2
    cleanup_failed=true
  fi
  if nc -z -w 1 127.0.0.1 "${TIKV_PORT}" >/dev/null 2>&1; then
    echo "PD-route cleanup failed: owned TiKV endpoint ${TIKV_ADDR} is still reachable" >&2
    cleanup_failed=true
  fi
  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}"
    if [[ -e "${TAG_DIR}" ]]; then
      echo "PD-route cleanup failed: owned tag directory remains at ${TAG_DIR}" >&2
      cleanup_failed=true
    fi
  fi
  rm -f "${PLAYGROUND_LOG}"
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}
trap cleanup EXIT INT TERM

if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
  echo "refusing to reuse occupied PD endpoint ${PD_ADDR}; set PD_ROUTE_PORT_OFFSET" >&2
  exit 1
fi
if nc -z -w 1 127.0.0.1 "${TIKV_PORT}" >/dev/null 2>&1; then
  echo "refusing to reuse occupied TiKV endpoint ${TIKV_ADDR}; set PD_ROUTE_PORT_OFFSET" >&2
  exit 1
fi

tiup playground v8.5.6 --mode tikv-slim --without-monitor --tag "${TAG}" \
  --port-offset "${PORT_OFFSET}" >"${PLAYGROUND_LOG}" 2>&1 &
PLAYGROUND_PID=$!

ready=false
for _ in $(seq 1 120); do
  if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
    echo "TiUP playground exited before readiness" >&2
    tail -80 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  echo "PD endpoint ${PD_ADDR} did not become ready" >&2
  tail -80 "${PLAYGROUND_LOG}" >&2
  exit 1
fi

OWNED_PIDS=$(collect_descendant_pids "${PLAYGROUND_PID}")
OWNED_PIDS=$(merge_owned_pids)
if [[ -z "${OWNED_PIDS}" ]]; then
  echo "TiUP did not publish owned descendant processes for ${TAG}" >&2
  exit 1
fi

# The PD endpoint is the only topology input. Rust discovers cluster, region,
# peer, store, and TiKV address metadata over the checked PD gRPC projection.
export PD_ROUTE_PD_ADDR="${PD_ADDR}"
cd "${RUST_ROOT}"
CARGO_BUILD_JOBS=12 cargo test -j12 -p difftest-transaction-tests \
  --test realtikv_pd_route \
  pd_only_input_discovers_route_and_reaches_tikv -- --ignored --exact --nocapture
