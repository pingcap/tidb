#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HARNESS="${SCRIPT_DIR}/lib/live-sql-node-harness.sh"
RUNNERS=(
  "${SCRIPT_DIR}/run-live-topology-churn-sql-node.sh"
  "${SCRIPT_DIR}/run-live-bigint-selection-sql-node.sh"
  "${SCRIPT_DIR}/run-live-clustered-pk-range-sql-node.sh"
)
MAX_LINES=(220 320 470)

bash -n "${HARNESS}"
source "${HARNESS}"

for multi_relation_helper in \
  run_live_sql_node_multi_relation_scenario \
  require_multi_relation_table_names \
  validate_multi_relation_receipts_since \
  run_multi_relation_phase; do
  if ! declare -F "${multi_relation_helper}" >/dev/null; then
    echo "shared live SQL-node harness omitted multi-relation helper ${multi_relation_helper}" >&2
    exit 1
  fi
done

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

# ordered-join SQL-node is intentionally optional while its live scenario is being
# assembled. Once present, it must use the paired-receipt runner rather than
# reopening any TiUP/process/topology lifecycle ownership.
MULTI_RELATION_RUNNER="${SCRIPT_DIR}/run-live-ordered-join-sql-node.sh"
if [[ -f "${MULTI_RELATION_RUNNER}" ]]; then
  bash -n "${MULTI_RELATION_RUNNER}"
  multi_line_count=$(awk 'END { print NR + 0 }' "${MULTI_RELATION_RUNNER}")
  if [[ "${multi_line_count}" -gt 430 ]]; then
    echo "ordered-join SQL-node ordered scenario grew past its 430-line boundary: ${MULTI_RELATION_RUNNER} (${multi_line_count})" >&2
    exit 1
  fi
  if ! grep -F 'source "${SCRIPT_DIR}/lib/live-sql-node-harness.sh"' \
    "${MULTI_RELATION_RUNNER}" >/dev/null \
    || ! grep -F 'run_live_sql_node_multi_relation_scenario' \
      "${MULTI_RELATION_RUNNER}" >/dev/null; then
    echo "ordered-join SQL-node ordered scenario bypasses the shared multi-relation topology engine" >&2
    exit 1
  fi
  if rg -n \
    'tiup playground|TIKV_B_COMMAND|process_shutdown_stage|beforeCommitSecondaries|LOCK_SECONDARY_KEY|transfer_leader|SHUTDOWN_STARTED_MS' \
    "${MULTI_RELATION_RUNNER}" >/dev/null; then
    echo "ordered-join SQL-node lifecycle leaked back into scenario-owned code" >&2
    exit 1
  fi
fi

# prepared point-read SQL-node owns a bounded prepared client/benchmark proof, but the shared
# harness must remain the sole process/topology authority.
PREPARED_RUNNER="${SCRIPT_DIR}/run-live-prepared-point-read-sql-node.sh"
if [[ -f "${PREPARED_RUNNER}" ]]; then
  bash -n "${PREPARED_RUNNER}"
  if ! grep -F 'source "${SCRIPT_DIR}/lib/live-sql-node-harness.sh"' \
    "${PREPARED_RUNNER}" >/dev/null \
    || ! grep -F 'run_live_sql_node_topology_scenario' \
      "${PREPARED_RUNNER}" >/dev/null \
    || ! grep -F 'scenario_pre_shutdown_proof()' "${PREPARED_RUNNER}" >/dev/null; then
    echo "prepared point-read SQL-node prepared scenario bypasses the shared healthy-topology hook" >&2
    exit 1
  fi
  if rg -n \
    'tiup playground|TIKV_B_COMMAND|process_shutdown_stage|beforeCommitSecondaries|LOCK_SECONDARY_KEY|transfer_leader|SHUTDOWN_STARTED_MS' \
    "${PREPARED_RUNNER}" >/dev/null; then
    echo "prepared point-read SQL-node lifecycle leaked back into scenario-owned code" >&2
    exit 1
  fi
fi

PRE_SHUTDOWN_HOOK_LINE=$(rg -n '^  scenario_pre_shutdown_proof$' "${HARNESS}" \
  | cut -d: -f1)
RETURNED_TO_B_LINE=$(rg -n '^  B_GENERATION_AFTER=' "${HARNESS}" | cut -d: -f1)
LOCK_SETUP_LINE=$(rg -n '^  LOCK_SECONDARY_KEY=' "${HARNESS}" | cut -d: -f1)
if [[ -z "${PRE_SHUTDOWN_HOOK_LINE}" || -z "${RETURNED_TO_B_LINE}" \
  || -z "${LOCK_SETUP_LINE}" \
  || "${PRE_SHUTDOWN_HOOK_LINE}" -le "${RETURNED_TO_B_LINE}" \
  || "${PRE_SHUTDOWN_HOOK_LINE}" -ge "${LOCK_SETUP_LINE}" ]]; then
  echo "healthy-topology proof hook must run after B convergence and before lock setup" >&2
  exit 1
fi

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

# In force mode mysql continues after a query error. The controlled-shutdown
# path must therefore close its FIFO writer after the server stops but before
# waiting for that client, or the client can wait forever for another line.
SHUTDOWN_CLIENT_BLOCK=$(awk '
  /wait "\$\{RUST_PID\}"/ { capture = 1 }
  capture { print }
  capture && /CLIENT_PIDS=\(\)/ { exit }
' "${HARNESS}")
FIFO_CLOSE_LINE=$(printf '%s\n' "${SHUTDOWN_CLIENT_BLOCK}" | nl -ba \
  | awk '/exec 9>&-/ { print $1; exit }')
CLIENT_EXIT_LINE=$(printf '%s\n' "${SHUTDOWN_CLIENT_BLOCK}" | nl -ba \
  | awk '/printf/ && /q/ && />&9/ { print $1; exit }')
CLIENT_WAIT_LINE=$(printf '%s\n' "${SHUTDOWN_CLIENT_BLOCK}" | nl -ba \
  | awk '/wait_for_pids_until.*PERSISTENT_CLIENT_PID/ { print $1; exit }')
if [[ -z "${CLIENT_EXIT_LINE}" || -z "${FIFO_CLOSE_LINE}" || -z "${CLIENT_WAIT_LINE}" \
  || "${CLIENT_EXIT_LINE}" -ge "${FIFO_CLOSE_LINE}" \
  || "${FIFO_CLOSE_LINE}" -ge "${CLIENT_WAIT_LINE}" ]]; then
  echo "controlled shutdown must send local quit then close the persistent FIFO before waiting for forced mysql" >&2
  exit 1
fi

if ! rg -F 'EXPECTED_FORCED_CONNECTIONS=0' "${HARNESS}" >/dev/null \
  || ! rg -F '[[ "${PERSISTENT_CLIENT_FORCE}" == true ]]' "${HARNESS}" >/dev/null \
  || ! rg -F 'EXPECTED_FORCED_CONNECTIONS=1' "${HARNESS}" >/dev/null \
  || ! rg -F -- '--argjson forced_connections "${EXPECTED_FORCED_CONNECTIONS}"' "${HARNESS}" >/dev/null \
  || ! rg -F '.[0].forced_connections == $forced_connections' "${HARNESS}" >/dev/null; then
  echo "controlled shutdown must require exactly one forced connection only for force-mode clients" >&2
  exit 1
fi

"${RUNNERS[0]}" --self-test-live-harness
"${RUNNERS[1]}" --self-test-empty-result-framing
"${RUNNERS[1]}" --self-test-live-harness
"${RUNNERS[2]}" --self-test-empty-result-framing
"${RUNNERS[2]}" --self-test-range-contract
"${RUNNERS[2]}" --self-test-live-harness
if [[ -f "${MULTI_RELATION_RUNNER}" ]]; then
  bash "${MULTI_RELATION_RUNNER}" --self-test-live-harness
fi

echo "shared live SQL-node lifecycle architecture self-test passed"
