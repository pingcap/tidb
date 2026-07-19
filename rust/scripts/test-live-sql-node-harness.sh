#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HARNESS="${SCRIPT_DIR}/lib/live-sql-node-harness.sh"
RUNNERS=(
  "${SCRIPT_DIR}/run-campaign22-topology-churn-sql-node.sh"
  "${SCRIPT_DIR}/run-campaign23-bigint-selection-sql-node.sh"
  "${SCRIPT_DIR}/run-campaign24-clustered-pk-range-sql-node.sh"
)
MAX_LINES=(220 320 470)

bash -n "${HARNESS}"

for index in "${!RUNNERS[@]}"; do
  runner=${RUNNERS[${index}]}
  bash -n "${runner}"
  line_count=$(awk 'END { print NR + 0 }' "${runner}")
  if [[ "${line_count}" -gt "${MAX_LINES[${index}]}" ]]; then
    echo "live SQL-node scenario grew past its ${MAX_LINES[${index}]}-line boundary: ${runner} (${line_count})" >&2
    exit 1
  fi
  if ! grep -F 'source "${SCRIPT_DIR}/lib/live-sql-node-harness.sh"' "${runner}" >/dev/null \
    || ! grep -F 'run_live_sql_node_topology_scenario' "${runner}" >/dev/null; then
    echo "live SQL-node scenario bypasses the shared topology engine: ${runner}" >&2
    exit 1
  fi
  if rg -n \
    'tiup playground|TIKV_B_COMMAND|process_shutdown_stage|beforeCommitSecondaries|LOCK_SECONDARY_KEY|transfer_leader|SHUTDOWN_STARTED_MS' \
    "${runner}" >/dev/null; then
    echo "live SQL-node lifecycle leaked back into scenario-owned code: ${runner}" >&2
    exit 1
  fi
done

LIFECYCLE_OWNERS=$(rg -l \
  'tiup playground v8\.5\.6 --without-monitor' "${HARNESS}" "${RUNNERS[@]}")
if [[ $(printf '%s\n' "${LIFECYCLE_OWNERS}" | sed '/^$/d' | awk 'END { print NR + 0 }') -ne 1 \
  || "${LIFECYCLE_OWNERS}" != "${HARNESS}" ]]; then
  echo "live SQL-node topology startup must have exactly one owner" >&2
  printf '%s\n' "${LIFECYCLE_OWNERS}" >&2
  exit 1
fi

"${RUNNERS[0]}" --self-test-live-harness
"${RUNNERS[1]}" --self-test-empty-result-framing
"${RUNNERS[1]}" --self-test-live-harness
"${RUNNERS[2]}" --self-test-empty-result-framing
"${RUNNERS[2]}" --self-test-range-contract
"${RUNNERS[2]}" --self-test-live-harness

echo "shared live SQL-node lifecycle architecture self-test passed"
