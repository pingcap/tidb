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
source "${HARNESS}"

start_stubborn_supervisor() {
  TAG=$1
  TAG_DIR="${TMPDIR:-/tmp}/${TAG}"
  PROCESS_STOP_TIMEOUT=1
  TEST_READY_FILE=$(mktemp "${TMPDIR:-/tmp}/${TAG}.XXXXXX")
  rm -f -- "${TEST_READY_FILE}"
  perl -e '
    $SIG{TERM} = "IGNORE";
    open(my $ready, ">", $ARGV[0]) or die $!;
    print {$ready} "ready\n";
    close($ready);
    sleep 10;
  ' "${TEST_READY_FILE}" "${TAG}" &
  TEST_SUPERVISOR_PID=$!
  for _ in $(seq 1 100); do
    if [[ -s "${TEST_READY_FILE}" ]]; then
      break
    fi
    if ! kill -0 "${TEST_SUPERVISOR_PID}" 2>/dev/null; then
      break
    fi
    sleep 0.01
  done
  if [[ ! -s "${TEST_READY_FILE}" ]]; then
    kill -KILL "${TEST_SUPERVISOR_PID}" 2>/dev/null || true
    wait "${TEST_SUPERVISOR_PID}" 2>/dev/null || true
    rm -f -- "${TEST_READY_FILE}"
    echo "failed-scenario cleanup fixture did not become ready" >&2
    return 1
  fi
}

stop_stubborn_supervisor() {
  kill -KILL "${TEST_SUPERVISOR_PID}" 2>/dev/null || true
  wait "${TEST_SUPERVISOR_PID}" 2>/dev/null || true
  rm -f -- "${TEST_READY_FILE}"
}

install_process_enumeration_fixture() {
  # Process enumeration is sandbox-dependent. Keep the production kill(2)
  # liveness check, but inject supervisor identity and an empty child snapshot.
  ps() {
    if [[ " $* " == *" -o stat= "* ]]; then
      printf 'S\n'
    else
      printf '%s\n' "${TEST_PROCESS_COMMAND}"
    fi
  }
  pgrep() {
    return 1
  }
}

test_failed_scenario_cleanup() {
  CAMPAIGN_LABEL="failed-scenario cleanup self-test"
  start_stubborn_supervisor campaign-failed-scenario-cleanup
  TEST_PROCESS_COMMAND="tiup playground --tag ${TAG}"
  install_process_enumeration_fixture
  if ! terminate_playground_supervisor "${TEST_SUPERVISOR_PID}"; then
    stop_stubborn_supervisor
    unset -f ps pgrep
    echo "failed-scenario cleanup rejected its exact tag-owned supervisor" >&2
    return 1
  fi
  unset -f ps pgrep
  rm -f -- "${TEST_READY_FILE}"
  if kill -0 "${TEST_SUPERVISOR_PID}" 2>/dev/null; then
    stop_stubborn_supervisor
    echo "failed-scenario cleanup left its exact tag-owned supervisor alive" >&2
    return 1
  fi
}

test_unowned_supervisor_is_rejected() {
  CAMPAIGN_LABEL="unowned-supervisor cleanup self-test"
  start_stubborn_supervisor campaign-unowned-supervisor-cleanup
  TEST_PROCESS_COMMAND="tiup playground --tag unrelated-campaign"
  install_process_enumeration_fixture
  if terminate_playground_supervisor "${TEST_SUPERVISOR_PID}" 2>/dev/null; then
    unset -f ps pgrep
    stop_stubborn_supervisor
    echo "cleanup accepted a supervisor without exact tag ownership" >&2
    return 1
  fi
  unset -f ps pgrep
  if ! kill -0 "${TEST_SUPERVISOR_PID}" 2>/dev/null; then
    rm -f -- "${TEST_READY_FILE}"
    echo "cleanup signaled an unowned supervisor beyond bounded TERM" >&2
    return 1
  fi
  stop_stubborn_supervisor
}

test_failed_scenario_cleanup
test_unowned_supervisor_is_rejected

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

for hook in \
  scenario_prepare_fixture \
  scenario_configure_server_arguments \
  scenario_validate_ready_json; do
  HOOK_OWNERS=$(rg -l "^${hook}\\(\\)" "${HARNESS}" "${RUNNERS[@]}")
  HOOK_REFERENCES=$(rg -c "${hook}" "${HARNESS}")
  if [[ "${HOOK_OWNERS}" != "${HARNESS}" ]] \
    || [[ "${HOOK_REFERENCES}" -lt 2 ]]; then
    echo "Campaigns 22-24 must consume the shared default ${hook} hook" >&2
    printf '%s\n' "${HOOK_OWNERS}" >&2
    exit 1
  fi
done

if rg -n -- '--database|--table-id|--column([[:space:]]|$)' "${HARNESS}" >/dev/null; then
  echo "shared live SQL-node harness retained the removed singular table grammar" >&2
  exit 1
fi

"${RUNNERS[0]}" --self-test-live-harness
"${RUNNERS[1]}" --self-test-empty-result-framing
"${RUNNERS[1]}" --self-test-live-harness
"${RUNNERS[2]}" --self-test-empty-result-framing
"${RUNNERS[2]}" --self-test-range-contract
"${RUNNERS[2]}" --self-test-live-harness

echo "shared live SQL-node lifecycle architecture self-test passed"
