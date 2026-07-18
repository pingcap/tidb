#!/usr/bin/env bash

set -euo pipefail

RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAG="campaign09-realtikv-${$}"
PORT_OFFSET=${C09_PORT_OFFSET:-20000}
PD_PORT=$((2379 + PORT_OFFSET))
PD_ADDR="127.0.0.1:${PD_PORT}"
PLAYGROUND_PID=
OWNED_PIDS=
PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}.log"
TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"

tag_status_rows() {
  tiup status 2>/dev/null | awk -v tag="${TAG}" \
    'NR > 2 && ($1 == tag || index($0, "/data/" tag "/")) { print }'
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
  if [[ -n "${PLAYGROUND_PID}" ]]; then
    if kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
      kill "${PLAYGROUND_PID}" 2>/dev/null || true
    fi
    wait "${PLAYGROUND_PID}" 2>/dev/null || true
  fi
  tiup clean "${TAG}" --all >/dev/null 2>&1 || true

  local processes_cleaned=false
  for _ in $(seq 1 30); do
    local process_alive=false
    local pid
    for pid in ${OWNED_PIDS}; do
      if kill -0 "${pid}" 2>/dev/null; then
        process_alive=true
        break
      fi
    done
    if [[ "${process_alive}" == false ]] && [[ -z "$(tag_status_rows)" ]]; then
      processes_cleaned=true
      break
    fi
    sleep 1
  done
  if [[ "${processes_cleaned}" != true ]]; then
    echo "Campaign 09 cleanup failed: owned TiUP registry or process remains for ${TAG}" >&2
    cleanup_failed=true
  else
    # A tagged playground may not leave the metadata that `tiup clean` needs,
    # so remove residual data only after registry and process teardown is
    # independently proven.
    rm -rf -- "${TAG_DIR}"
    if [[ -e "${TAG_DIR}" ]]; then
      echo "Campaign 09 cleanup failed: owned tag directory remains at ${TAG_DIR}" >&2
      cleanup_failed=true
    fi
  fi
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "Campaign 09 cleanup failed: owned PD endpoint ${PD_ADDR} is still reachable" >&2
    cleanup_failed=true
  fi
  rm -f "${PLAYGROUND_LOG}"
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}
trap cleanup EXIT INT TERM

if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
  echo "refusing to reuse occupied PD endpoint ${PD_ADDR}; set C09_PORT_OFFSET" >&2
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
if [[ -z "${OWNED_PIDS}" ]]; then
  echo "TiUP did not publish owned descendant processes for ${TAG}" >&2
  exit 1
fi

TOPOLOGY=$(python3 - "${PD_ADDR}" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

pd = sys.argv[1]
for _ in range(120):
    try:
        with urllib.request.urlopen(f"http://{pd}/pd/api/v1/cluster") as response:
            cluster_id = json.load(response)["id"]
        with urllib.request.urlopen(f"http://{pd}/pd/api/v1/regions?limit=1") as response:
            regions = json.load(response)["regions"]
    except urllib.error.URLError:
        time.sleep(1)
        continue
    if not cluster_id:
        time.sleep(1)
        continue
    if not regions:
        time.sleep(1)
        continue
    region = next((item for item in regions if not item.get("start_key")), None)
    if region is None:
        time.sleep(1)
        continue
    leader = region.get("leader") or {}
    store_id = leader.get("store_id")
    peer_id = leader.get("id")
    peers = {peer["id"]: peer for peer in region.get("peers", [])}
    peer = peers.get(peer_id)
    if not store_id or not peer_id or not peer:
        time.sleep(1)
        continue
    try:
        with urllib.request.urlopen(f"http://{pd}/pd/api/v1/stores") as response:
            stores = json.load(response)["stores"]
    except urllib.error.URLError:
        time.sleep(1)
        continue
    addresses = {
        item["store"]["id"]: item["store"]["address"]
        for item in stores
        if item.get("store", {}).get("state_name") == "Up"
    }
    address = addresses.get(store_id)
    if address:
        break
    time.sleep(1)
else:
    raise SystemExit("PD did not publish one ready region leader and TiKV address")

role_name = peer.get("role_name", "Voter")
role_by_name = {"Voter": 0, "Learner": 1, "IncomingVoter": 2, "DemotingVoter": 3}
if role_name not in role_by_name:
    raise SystemExit(f"unsupported PD peer role {role_name!r}")
role = role_by_name[role_name]
is_witness = bool(peer.get("is_witness", False))
if role != 0 or is_witness:
    raise SystemExit("fresh playground leader must be a non-witness voter")
epoch = region["epoch"]
print(
    region["id"], epoch["conf_ver"], epoch["version"], peer_id, store_id,
    role, str(is_witness).lower(), cluster_id, address,
    region.get("start_key") or "-", region.get("end_key") or "-"
)
PY
)
read -r C09_REGION_ID C09_REGION_CONF_VER C09_REGION_VERSION \
  C09_PEER_ID C09_STORE_ID C09_PEER_ROLE C09_PEER_IS_WITNESS \
  C09_CLUSTER_ID C09_TIKV_ADDR C09_REGION_START_HEX C09_REGION_END_HEX \
  <<<"${TOPOLOGY}"

export C09_TIKV_ADDR C09_REGION_ID C09_REGION_CONF_VER C09_REGION_VERSION
export C09_PEER_ID C09_STORE_ID C09_PEER_ROLE C09_PEER_IS_WITNESS C09_CLUSTER_ID
export C09_REGION_START_HEX C09_REGION_END_HEX

cd "${RUST_ROOT}"
CARGO_BUILD_JOBS=12 cargo test -p difftest-transaction-tests \
  --test realtikv_tikv_unary \
  realtikv_unary_distql_chain_reaches_tikv -- --ignored --exact --nocapture
