#!/usr/bin/env bash

set -euo pipefail

CAMPAIGN_LABEL="Campaign 23"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${SCRIPT_DIR}/lib/live-sql-node-harness.sh"

if [[ "${1:-}" == --self-test-empty-result-framing \
  || "${1:-}" == --self-test-live-harness ]]; then
  live_sql_node_harness_self_test
  if [[ "${1:-}" == --self-test-empty-result-framing ]]; then
    echo "Campaign 23 empty-result framing self-test passed"
  else
    echo "Campaign 23 shared live-harness self-test passed"
  fi
  exit 0
fi


SCENARIO_ENV_PREFIX=C23
SCENARIO_TAG_SLUG=campaign23-bigint-selection-sql-node
SCENARIO_DATABASE=campaign23
SCENARIO_AUTH_USER=campaign23
SCENARIO_AUTH_DEFAULT_PASSWORD=campaign23-native-password
SCENARIO_EXPECTS_QUERY_SNAPSHOT=true
REFERENCE_QUERY='SELECT balance AS amount, id FROM campaign23.rows;'
REFERENCE_HEADER=$'amount\tid'
REFERENCE_ROWS=$'913\t-7\n-2048\t0\n77\t42'
QUERY_STORED_PREDICATE='SELECT id FROM campaign23.rows WHERE balance < 0;'
QUERY_ID_EQ='SELECT balance AS amount, id FROM campaign23.rows WHERE id = -7;'
QUERY_ID_NE='SELECT balance AS amount, id FROM campaign23.rows WHERE id != 0;'
QUERY_BALANCE_LE='SELECT balance AS amount, id FROM campaign23.rows WHERE balance <= 77;'
QUERY_ID_GT='SELECT balance AS amount, id FROM campaign23.rows WHERE id > 0;'
QUERY_ID_GE='SELECT balance AS amount, id FROM campaign23.rows WHERE id >= 0;'
QUERY_EMPTY='SELECT balance AS amount, id FROM campaign23.rows WHERE balance > 1000;'
QUERY_REVERSED='SELECT balance AS amount, id FROM campaign23.rows WHERE 77 <= balance;'
QUERY_AND='SELECT balance AS amount, id FROM campaign23.rows WHERE balance >= 77 AND id < 42;'
ROWS_ID_EQ=$'913\t-7'
ROWS_ID_NE=$'913\t-7\n77\t42'
ROWS_BALANCE_LE=$'-2048\t0\n77\t42'
ROWS_ID_GT=$'77\t42'
ROWS_ID_GE=$'-2048\t0\n77\t42'
ROWS_REVERSED=$'913\t-7\n77\t42'
SCENARIO_BLOCK_QUERY=${QUERY_ID_EQ}

run_exact_query_phase() {
  local phase=$1
  local expected_address=$2
  local query=$3
  local expected_header=$4
  local expected_rows=$5
  local expected_output_offsets=$6
  local expected_predicate_count=$7
  local mode=${8:-strict}
  if [[ ! "${expected_predicate_count}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Campaign 23 ${phase} has invalid predicate count ${expected_predicate_count}" >&2
    return 1
  fi
  if [[ "${mode}" != strict && "${mode}" != converge ]]; then
    echo "Campaign 23 ${phase} has invalid query phase mode ${mode}" >&2
    return 1
  fi
  local output="${RUNTIME_DIR}/${phase}.out"
  local before_publications
  local before_transports
  local before_snapshots
  local before_activities
  local before_output_lines
  local before_error_lines
  before_publications=$(publication_count)
  before_transports=$(transport_count)
  before_snapshots=$(snapshot_count)
  before_activities=$(grep -c -F '"event":"query_activity"' "${RUST_LOG}" 2>/dev/null || true)
  before_output_lines=$(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}")
  before_error_lines=$(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_ERROR}")
  if ! pid_is_running "${PERSISTENT_CLIENT_PID}"; then
    echo "Campaign 23 persistent stock client exited before ${phase}" >&2
    return 1
  fi
  local expected_row_count
  expected_row_count=$(printf '%s\n' "${expected_rows}" \
    | sed '/^[[:space:]]*$/d' | awk 'END { print NR + 0 }')
  local expected_output_lines
  expected_output_lines=$(expected_persistent_client_output_lines "${expected_row_count}")
  printf '%s\n' "${query}" >&9
  wait_for_new_event_count query_snapshot "${before_snapshots}" "${before_error_lines}"
  wait_for_new_event_count query_transport_published "${before_publications}" "${before_error_lines}"
  wait_for_new_event_count query_transport "${before_transports}" "${before_error_lines}"
  local output_ready=false
  local stable_output_samples=0
  local expected_total_output_lines=$((before_output_lines + expected_output_lines))
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    local current_output_lines
    current_output_lines=$(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}")
    if [[ "${current_output_lines}" -eq "${expected_total_output_lines}" ]]; then
      stable_output_samples=$((stable_output_samples + 1))
      if [[ "${stable_output_samples}" -ge 4 ]]; then
        output_ready=true
        break
      fi
    elif [[ "${current_output_lines}" -gt "${expected_total_output_lines}" ]]; then
      break
    else
      stable_output_samples=0
    fi
    if ! pid_is_running "${PERSISTENT_CLIENT_PID}"; then
      break
    fi
    if [[ $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_ERROR}") \
      -gt "${before_error_lines}" ]]; then
      break
    fi
    sleep 0.05
  done
  if [[ "${output_ready}" != true ]]; then
    echo "Campaign 23 ${phase} persistent stock-client output did not complete" >&2
    tail -40 "${PERSISTENT_CLIENT_OUTPUT}" >&2
    sed -n '1,160p' "${PERSISTENT_CLIENT_ERROR}" >&2
    return 1
  fi
  if [[ "${expected_output_lines}" -eq 0 ]]; then
    : >"${output}"
  else
    sed -n "$((before_output_lines + 1)),$((before_output_lines + expected_output_lines))p" \
      "${PERSISTENT_CLIENT_OUTPUT}" >"${output}"
  fi
  if ! query_output_is_exact "${output}" "${expected_header}" "${expected_rows}"; then
    echo "Campaign 23 ${phase} did not return the exact filtered rows" >&2
    sed -n '1,40p' "${output}" >&2
    return 1
  fi
  local publications
  publications=$(grep -F '"event":"query_transport_published"' "${RUST_LOG}" \
    | tail -n +$((before_publications + 1)))
  PHASE_PUBLICATION=$(printf '%s\n' "${publications}" | tail -1)
  if ! printf '%s\n' "${PHASE_PUBLICATION}" | jq -e \
    --arg expected "${expected_address}" \
    --arg region "${REGION_ID:-}" \
    --arg authority "${AUTHORITY_ID}" \
    --arg mode "${mode}" \
    '($mode == "converge" or $expected == "" or .physical_address == $expected)
     and (.physical_channel_version | type) == "number"
     and .physical_channel_version > 0
     and (.stream_generation | type) == "number"
     and .stream_generation > 0
     and .forwarded_host == null
     and (.authority_id | tostring) == $authority
     and (.connection_id | type) == "number" and .connection_id > 0
     and (.query_id | type) == "number" and .query_id > 0
     and (.session_id | type) == "number" and .session_id > 0
     and ($region == "" or (.region_id | tostring) == $region)' >/dev/null; then
    echo "Campaign 23 ${phase} last physical publication did not match ${expected_address}" >&2
    printf '%s\n' "${publications}" >&2
    return 1
  fi
  PHASE_CONNECTION_ID=$(printf '%s\n' "${PHASE_PUBLICATION}" | jq -r '.connection_id')
  PHASE_QUERY_ID=$(printf '%s\n' "${PHASE_PUBLICATION}" | jq -r '.query_id')
  PHASE_SESSION_ID=$(printf '%s\n' "${PHASE_PUBLICATION}" | jq -r '.session_id')
  if [[ -z "${PERSISTENT_CONNECTION_ID}" ]]; then
    PERSISTENT_CONNECTION_ID=${PHASE_CONNECTION_ID}
    PERSISTENT_SESSION_ID=${PHASE_SESSION_ID}
  elif [[ "${PHASE_CONNECTION_ID}" != "${PERSISTENT_CONNECTION_ID}" \
    || "${PHASE_SESSION_ID}" != "${PERSISTENT_SESSION_ID}" ]]; then
    echo "Campaign 23 ${phase} did not remain on the persistent authenticated session" >&2
    return 1
  fi
  local snapshot
  snapshot=$(grep -F '"event":"query_snapshot"' "${RUST_LOG}" \
    | tail -n +$((before_snapshots + 1)) \
    | jq -c --arg connection "${PHASE_CONNECTION_ID}" --arg query "${PHASE_QUERY_ID}" \
      'select((.connection_id | tostring) == $connection and (.query_id | tostring) == $query)' \
    | tail -1)
  if [[ -z "${snapshot}" ]] || ! printf '%s\n' "${snapshot}" | jq -e \
    --arg authority "${AUTHORITY_ID}" --arg session "${PHASE_SESSION_ID}" \
    --argjson offsets "${expected_output_offsets}" \
    --argjson predicate_count "${expected_predicate_count}" \
    '(.authority_id | tostring) == $authority
     and (.session_id | tostring) == $session
     and .executor_kinds == ["TableScan", "Selection"]
     and .predicate_count == $predicate_count
     and .output_offsets == $offsets' >/dev/null; then
    echo "Campaign 23 ${phase} did not publish the exact TableScan-plus-Selection plan" >&2
    printf '%s\n' "${snapshot}" >&2
    return 1
  fi
  local transport
  transport=$(grep -F '"event":"query_transport"' "${RUST_LOG}" \
    | tail -n +$((before_transports + 1)) | tail -1)
  if ! printf '%s\n' "${transport}" | jq -e \
    --arg connection "${PHASE_CONNECTION_ID}" \
    --arg query "${PHASE_QUERY_ID}" \
    --arg authority "${AUTHORITY_ID}" \
    --arg session "${PHASE_SESSION_ID}" \
    --arg mode "${mode}" \
    '(.connection_id | tostring) == $connection
     and (.query_id | tostring) == $query
     and (.authority_id | tostring) == $authority
     and (.session_id | tostring) == $session
     and ((if $mode == "converge"
       then (.batch_attempts + .unary_attempts) >= 1
       else .batch_attempts >= 1 and .unary_attempts == 0 end))
     and (.located_region_ids | length) > 0
     and (.dispatched_region_ids | length) > 0' >/dev/null; then
    echo "Campaign 23 ${phase} did not finish through BatchCommands-only transport" >&2
    printf '%s\n' "${transport}" >&2
    return 1
  fi
  local activity_ready=false
  deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    local activities
    activities=$(grep -F '"event":"query_activity"' "${RUST_LOG}" \
      | tail -n +$((before_activities + 1)) | jq -s '.')
    if printf '%s\n' "${activities}" | jq -e \
      --arg connection "${PHASE_CONNECTION_ID}" --arg query "${PHASE_QUERY_ID}" \
      '[.[] | select((.connection_id | tostring) == $connection
        and (.query_id | tostring) == $query) | .phase] == ["begin", "end"]' \
      >/dev/null; then
      activity_ready=true
      break
    fi
    sleep 0.05
  done
  if [[ "${activity_ready}" != true ]]; then
    echo "Campaign 23 ${phase} activity did not correlate with publication/transport identity" >&2
    return 1
  fi
  if ! pid_is_running "${RUST_PID}"; then
    echo "Campaign 23 ${phase} did not retain the original Rust process" >&2
    return 1
  fi
}


scenario_pre_transfer_discovery() {
  run_exact_query_phase pre_transfer_discovery "" \
    "${QUERY_STORED_PREDICATE}" id 0 '[0]' 1
}

scenario_pre_transfer_verified() {
  local address=$1
  run_exact_query_phase pre_transfer_verified "${address}" \
    "${QUERY_ID_EQ}" "${REFERENCE_HEADER}" "${ROWS_ID_EQ}" '[0,1]' 1
  run_exact_query_phase pre_transfer_not_equal "${address}" \
    "${QUERY_ID_NE}" "${REFERENCE_HEADER}" "${ROWS_ID_NE}" '[0,1]' 1
  run_exact_query_phase pre_transfer_reversed_operand "${address}" \
    "${QUERY_REVERSED}" "${REFERENCE_HEADER}" "${ROWS_REVERSED}" '[0,1]' 1
  run_exact_query_phase pre_transfer_empty_result "${address}" \
    "${QUERY_EMPTY}" "${REFERENCE_HEADER}" "" '[0,1]' 1
  run_exact_query_phase pre_transfer_and_conjunction "${address}" \
    "${QUERY_AND}" "${REFERENCE_HEADER}" "${ROWS_ID_EQ}" '[0,1]' 2
}

scenario_transferred_to_b() {
  run_exact_query_phase transferred_to_b_convergence "$1" \
    "${QUERY_BALANCE_LE}" "${REFERENCE_HEADER}" "${ROWS_BALANCE_LE}" '[0,1]' 1 converge
  run_exact_query_phase transferred_to_b "$1" \
    "${QUERY_ID_GT}" "${REFERENCE_HEADER}" "${ROWS_ID_GT}" '[0,1]' 1
}

scenario_failed_over_to_c() {
  run_exact_query_phase failed_over_to_c_convergence "$1" \
    "${QUERY_ID_GE}" "${REFERENCE_HEADER}" "${ROWS_ID_GE}" '[0,1]' 1 converge
  run_exact_query_phase failed_over_to_c "$1" \
    "${QUERY_STORED_PREDICATE}" id 0 '[0]' 1
}

scenario_returned_to_b() {
  run_exact_query_phase returned_to_b_convergence "$1" \
    "${QUERY_ID_NE}" "${REFERENCE_HEADER}" "${ROWS_ID_NE}" '[0,1]' 1 converge
  run_exact_query_phase returned_to_b "$1" \
    "${QUERY_ID_EQ}" "${REFERENCE_HEADER}" "${ROWS_ID_EQ}" '[0,1]' 1
}

scenario_validate_block_snapshot() {
  [[ -n "$1" ]] && printf '%s\n' "$1" | jq -e \
    '.executor_kinds == ["TableScan", "Selection"]
     and .predicate_count == 1 and .output_offsets == [0, 1]' >/dev/null
}

scenario_emit_success_receipt() {
  echo "Campaign 23 live BIGINT-Selection topology proof passed: same_rust_pid=${ORIGINAL_RUST_PID}; persistent_connection_id=${PERSISTENT_CONNECTION_ID}; persistent_session_id=${PERSISTENT_SESSION_ID}; region_id=${REGION_ID}; leader_stores=${STORE_A}->${STORE_B}->${STORE_C}->${STORE_B}; comparison_matrix=eq,ne,lt,le,gt,ge,reversed,empty,and; stored_predicate_plan=TableScan,Selection; stored_predicate_output_offsets=[0]; physical_addresses=${ADDRESS_A}->${ADDRESS_B}->${ADDRESS_C}->${ADDRESS_B}; b_channel_versions=${B_VERSION_BEFORE}->${B_VERSION_AFTER}; b_stream_generations=${B_GENERATION_BEFORE}->${B_GENERATION_AFTER}; blocked_id_eq_publication=$(printf '%s' "${BLOCK_PUBLICATION}" | jq -c '{connection_id,query_id,authority_id,session_id,region_id,physical_address,physical_channel_version,stream_generation}'); shutdown_order=connections,region_cache,tikv_transport,pd,sql_node_stopped; shutdown_elapsed_ms=${SHUTDOWN_ELAPSED_MS}; shutdown_grace_ms=${SHUTDOWN_GRACE_MS}; rust_exit=0; ${FINAL_CONNECTIONS}; authority_id=${AUTHORITY_ID}; read_authority_id=${READ_AUTHORITY_ID}; pd_cluster_id=${PD_CLUSTER_ID}"
}

run_live_sql_node_topology_scenario
