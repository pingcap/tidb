#!/usr/bin/env bash

set -euo pipefail

CAMPAIGN_LABEL="clustered-PK range SQL node"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${SCRIPT_DIR}/lib/live-sql-node-harness.sh"

range_snapshot_matches() {
  local snapshot=$1
  local expected_executor_kinds=$2
  local expected_ranges=$3
  local expected_predicate_count=$4
  local expected_output_offsets=$5
  local snapshot_mode=${6:-remote}
  printf '%s\n' "${snapshot}" | jq -e \
    --argjson executor_kinds "${expected_executor_kinds}" \
    --argjson ranges "${expected_ranges}" \
    --argjson predicate_count "${expected_predicate_count}" \
    --argjson offsets "${expected_output_offsets}" \
    --arg snapshot_mode "${snapshot_mode}" \
    '.executor_kinds == $executor_kinds
     and .predicate_count == $predicate_count
     and .output_offsets == $offsets
     and .handle_range_count == ($ranges | length)
     and .handle_ranges == $ranges
     and (if $snapshot_mode == "local-empty"
       then .snapshot_ts == null
       else (.snapshot_ts | type) == "number" and .snapshot_ts > 0 end)' \
    >/dev/null
}

campaign24_range_contract_self_test() {
  local narrow='{"executor_kinds":["TableScan"],"predicate_count":0,"output_offsets":[0,1],"handle_range_count":1,"handle_ranges":[{"low":-7,"high":-7,"low_exclude":false,"high_exclude":false}],"snapshot_ts":101}'
  local split='{"executor_kinds":["TableScan"],"predicate_count":0,"output_offsets":[0,1],"handle_range_count":2,"handle_ranges":[{"low":-9223372036854775808,"high":-1,"low_exclude":false,"high_exclude":false},{"low":1,"high":9223372036854775807,"low_exclude":false,"high_exclude":false}],"snapshot_ts":102}'
  local residual='{"executor_kinds":["TableScan","Selection"],"predicate_count":1,"output_offsets":[0,1],"handle_range_count":1,"handle_ranges":[{"low":-7,"high":42,"low_exclude":false,"high_exclude":false}],"snapshot_ts":103}'
  local empty='{"executor_kinds":["TableScan"],"predicate_count":0,"output_offsets":[0,1],"handle_range_count":0,"handle_ranges":[],"snapshot_ts":null}'
  local full='[{"low":-9223372036854775808,"high":9223372036854775807,"low_exclude":false,"high_exclude":false}]'
  range_snapshot_matches "${narrow}" '["TableScan"]' \
    '[{"low":-7,"high":-7,"low_exclude":false,"high_exclude":false}]' 0 '[0,1]'
  range_snapshot_matches "${split}" '["TableScan"]' \
    '[{"low":-9223372036854775808,"high":-1,"low_exclude":false,"high_exclude":false},{"low":1,"high":9223372036854775807,"low_exclude":false,"high_exclude":false}]' 0 '[0,1]'
  range_snapshot_matches "${residual}" '["TableScan","Selection"]' \
    '[{"low":-7,"high":42,"low_exclude":false,"high_exclude":false}]' 1 '[0,1]'
  range_snapshot_matches "${empty}" '["TableScan"]' '[]' 0 '[0,1]' local-empty
  ! range_snapshot_matches "${narrow}" '["TableScan"]' "${full}" 0 '[0,1]'
}

if [[ "${1:-}" == --self-test-empty-result-framing \
  || "${1:-}" == --self-test-live-harness \
  || "${1:-}" == --self-test-range-contract ]]; then
  live_sql_node_harness_self_test
  campaign24_range_contract_self_test
  if [[ "${1:-}" == --self-test-empty-result-framing ]]; then
    echo "clustered-PK range SQL-node empty-result framing self-test passed"
  elif [[ "${1:-}" == --self-test-live-harness ]]; then
    echo "clustered-PK range SQL-node shared live-harness self-test passed"
  else
    echo "clustered-PK range SQL-node clustered-PK range-contract self-test passed"
  fi
  exit 0
fi


SCENARIO_ENV_PREFIX=CLUSTERED_PK_SQL
SCENARIO_TAG_SLUG=campaign24-clustered-pk-range-sql-node
SCENARIO_DATABASE=campaign24
SCENARIO_AUTH_USER=campaign24
SCENARIO_AUTH_DEFAULT_PASSWORD=campaign24-native-password
SCENARIO_EXPECTS_QUERY_SNAPSHOT=true
REFERENCE_QUERY='SELECT balance AS amount, id FROM campaign24.rows;'
REFERENCE_HEADER=$'amount\tid'
REFERENCE_ROWS=$'913\t-7\n-2048\t0\n77\t42'
QUERY_ID_EQ='SELECT balance AS amount, id FROM campaign24.rows WHERE id = -7;'
QUERY_ID_NE='SELECT balance AS amount, id FROM campaign24.rows WHERE id != 0;'
QUERY_ID_LT='SELECT balance AS amount, id FROM campaign24.rows WHERE id < 42;'
QUERY_ID_LE='SELECT balance AS amount, id FROM campaign24.rows WHERE id <= 0;'
QUERY_ID_GT='SELECT balance AS amount, id FROM campaign24.rows WHERE id > 0;'
QUERY_ID_GE='SELECT balance AS amount, id FROM campaign24.rows WHERE id >= 0;'
QUERY_REVERSED='SELECT balance AS amount, id FROM campaign24.rows WHERE 42 > id;'
QUERY_BOUNDED='SELECT balance AS amount, id FROM campaign24.rows WHERE id >= 0 AND id < 42;'
QUERY_MIN='SELECT balance AS amount, id FROM campaign24.rows WHERE id <= -9223372036854775808;'
QUERY_MAX='SELECT balance AS amount, id FROM campaign24.rows WHERE id >= 9223372036854775807;'
QUERY_CONTRADICTION='SELECT balance AS amount, id FROM campaign24.rows WHERE id > 42 AND id < 0;'
QUERY_RESIDUAL='SELECT balance AS amount, id FROM campaign24.rows WHERE id >= -7 AND id <= 42 AND balance != -2048;'
ROWS_ID_EQ=$'913\t-7'
ROWS_ID_NE=$'913\t-7\n77\t42'
ROWS_ID_LT=$'913\t-7\n-2048\t0'
ROWS_ID_LE=$'913\t-7\n-2048\t0'
ROWS_ID_GT=$'77\t42'
ROWS_ID_GE=$'-2048\t0\n77\t42'
ROWS_REVERSED=${ROWS_ID_LT}
ROWS_BOUNDED=$'-2048\t0'
ROWS_RESIDUAL=$'913\t-7\n77\t42'
RANGE_ID_EQ='[{"low":-7,"high":-7,"low_exclude":false,"high_exclude":false}]'
RANGE_ID_NE='[{"low":-9223372036854775808,"high":-1,"low_exclude":false,"high_exclude":false},{"low":1,"high":9223372036854775807,"low_exclude":false,"high_exclude":false}]'
RANGE_ID_LT='[{"low":-9223372036854775808,"high":41,"low_exclude":false,"high_exclude":false}]'
RANGE_ID_LE='[{"low":-9223372036854775808,"high":0,"low_exclude":false,"high_exclude":false}]'
RANGE_ID_GT='[{"low":1,"high":9223372036854775807,"low_exclude":false,"high_exclude":false}]'
RANGE_ID_GE='[{"low":0,"high":9223372036854775807,"low_exclude":false,"high_exclude":false}]'
RANGE_BOUNDED='[{"low":0,"high":41,"low_exclude":false,"high_exclude":false}]'
RANGE_MIN='[{"low":-9223372036854775808,"high":-9223372036854775808,"low_exclude":false,"high_exclude":false}]'
RANGE_MAX='[{"low":9223372036854775807,"high":9223372036854775807,"low_exclude":false,"high_exclude":false}]'
RANGE_RESIDUAL='[{"low":-7,"high":42,"low_exclude":false,"high_exclude":false}]'
SCENARIO_BLOCK_QUERY=${QUERY_ID_EQ}

run_exact_query_phase() {
  local phase=$1
  local expected_address=$2
  local query=$3
  local expected_header=$4
  local expected_rows=$5
  local expected_output_offsets=$6
  local expected_predicate_count=$7
  local expected_ranges=$8
  local expected_executor_kinds=$9
  local mode=${10:-strict}
  if [[ ! "${expected_predicate_count}" =~ ^[0-9]+$ ]]; then
    echo "clustered-PK range SQL-node ${phase} has invalid predicate count ${expected_predicate_count}" >&2
    return 1
  fi
  if [[ "${mode}" != strict && "${mode}" != converge ]]; then
    echo "clustered-PK range SQL-node ${phase} has invalid query phase mode ${mode}" >&2
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
    echo "clustered-PK range SQL-node persistent stock client exited before ${phase}" >&2
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
    echo "clustered-PK range SQL-node ${phase} persistent stock-client output did not complete" >&2
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
    echo "clustered-PK range SQL-node ${phase} did not return the exact filtered rows" >&2
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
    echo "clustered-PK range SQL-node ${phase} last physical publication did not match ${expected_address}" >&2
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
    echo "clustered-PK range SQL-node ${phase} did not remain on the persistent authenticated session" >&2
    return 1
  fi
  local snapshot
  snapshot=$(grep -F '"event":"query_snapshot"' "${RUST_LOG}" \
    | tail -n +$((before_snapshots + 1)) \
    | jq -c --arg connection "${PHASE_CONNECTION_ID}" --arg query "${PHASE_QUERY_ID}" \
      'select((.connection_id | tostring) == $connection and (.query_id | tostring) == $query)' \
    | tail -1)
  if [[ -z "${snapshot}" ]] \
    || ! printf '%s\n' "${snapshot}" | jq -e \
      --arg authority "${AUTHORITY_ID}" --arg session "${PHASE_SESSION_ID}" \
      '(.authority_id | tostring) == $authority
       and (.session_id | tostring) == $session' >/dev/null \
    || ! range_snapshot_matches "${snapshot}" "${expected_executor_kinds}" \
      "${expected_ranges}" "${expected_predicate_count}" "${expected_output_offsets}"; then
    echo "clustered-PK range SQL-node ${phase} did not publish the exact narrowed handle-range plan" >&2
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
    echo "clustered-PK range SQL-node ${phase} did not finish through BatchCommands-only transport" >&2
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
    echo "clustered-PK range SQL-node ${phase} activity did not correlate with publication/transport identity" >&2
    return 1
  fi
  if ! pid_is_running "${RUST_PID}"; then
    echo "clustered-PK range SQL-node ${phase} did not retain the original Rust process" >&2
    return 1
  fi
}

run_empty_range_phase() {
  local phase=$1
  local query=$2
  local expected_output_offsets=$3
  local before_publications
  local before_snapshots
  local before_activities
  local before_output_lines
  local before_error_lines
  before_publications=$(publication_count)
  before_snapshots=$(snapshot_count)
  before_activities=$(grep -c -F '"event":"query_activity"' "${RUST_LOG}" 2>/dev/null || true)
  before_output_lines=$(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}")
  before_error_lines=$(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_ERROR}")
  printf '%s\n' "${query}" >&9
  wait_for_new_event_count query_snapshot "${before_snapshots}" "${before_error_lines}"

  local snapshot
  snapshot=$(grep -F '"event":"query_snapshot"' "${RUST_LOG}" \
    | tail -n +$((before_snapshots + 1)) | tail -1)
  local connection_id
  local query_id
  local session_id
  connection_id=$(printf '%s\n' "${snapshot}" | jq -r '.connection_id // 0')
  query_id=$(printf '%s\n' "${snapshot}" | jq -r '.query_id // 0')
  session_id=$(printf '%s\n' "${snapshot}" | jq -r '.session_id // 0')
  if [[ "${connection_id}" != "${PERSISTENT_CONNECTION_ID}" \
    || "${session_id}" != "${PERSISTENT_SESSION_ID}" ]] \
    || ! printf '%s\n' "${snapshot}" | jq -e \
      --arg authority "${AUTHORITY_ID}" \
      '(.authority_id | tostring) == $authority' >/dev/null \
    || ! range_snapshot_matches "${snapshot}" '["TableScan"]' '[]' 0 \
      "${expected_output_offsets}" local-empty; then
    echo "clustered-PK range SQL-node ${phase} did not publish the exact local zero-range plan" >&2
    printf '%s\n' "${snapshot}" >&2
    return 1
  fi

  local activity_ready=false
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    local activities
    activities=$(grep -F '"event":"query_activity"' "${RUST_LOG}" \
      | tail -n +$((before_activities + 1)) | jq -s '.')
    if printf '%s\n' "${activities}" | jq -e \
      --arg connection "${connection_id}" --arg query "${query_id}" \
      '[.[] | select((.connection_id | tostring) == $connection
        and (.query_id | tostring) == $query) | .phase] == ["begin", "end"]' \
      >/dev/null; then
      activity_ready=true
      break
    fi
    if [[ $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_ERROR}") \
      -gt "${before_error_lines}" ]]; then
      break
    fi
    sleep 0.05
  done
  sleep 0.2
  if [[ "${activity_ready}" != true \
    || $(publication_count) -ne "${before_publications}" \
    || $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}") \
      -ne "${before_output_lines}" \
    || $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_ERROR}") \
      -ne "${before_error_lines}" ]]; then
    echo "clustered-PK range SQL-node ${phase} acquired a physical transport publication or emitted client data for a contradiction" >&2
    return 1
  fi
}


scenario_pre_transfer_discovery() {
  run_exact_query_phase pre_transfer_discovery "" \
    "${QUERY_ID_EQ}" "${REFERENCE_HEADER}" "${ROWS_ID_EQ}" '[0,1]' 0 \
    "${RANGE_ID_EQ}" '["TableScan"]'
}

scenario_pre_transfer_verified() {
  local address=$1
  run_exact_query_phase pre_transfer_verified "${address}" \
    "${QUERY_RESIDUAL}" "${REFERENCE_HEADER}" "${ROWS_RESIDUAL}" '[0,1]' 1 \
    "${RANGE_RESIDUAL}" '["TableScan","Selection"]'
  run_exact_query_phase pre_transfer_not_equal "${address}" \
    "${QUERY_ID_NE}" "${REFERENCE_HEADER}" "${ROWS_ID_NE}" '[0,1]' 0 \
    "${RANGE_ID_NE}" '["TableScan"]'
  run_exact_query_phase pre_transfer_reversed_operand "${address}" \
    "${QUERY_REVERSED}" "${REFERENCE_HEADER}" "${ROWS_REVERSED}" '[0,1]' 0 \
    "${RANGE_ID_LT}" '["TableScan"]'
  run_exact_query_phase pre_transfer_less_than "${address}" \
    "${QUERY_ID_LT}" "${REFERENCE_HEADER}" "${ROWS_ID_LT}" '[0,1]' 0 \
    "${RANGE_ID_LT}" '["TableScan"]'
  run_exact_query_phase pre_transfer_less_equal "${address}" \
    "${QUERY_ID_LE}" "${REFERENCE_HEADER}" "${ROWS_ID_LE}" '[0,1]' 0 \
    "${RANGE_ID_LE}" '["TableScan"]'
  run_exact_query_phase pre_transfer_bounded_intersection "${address}" \
    "${QUERY_BOUNDED}" "${REFERENCE_HEADER}" "${ROWS_BOUNDED}" '[0,1]' 0 \
    "${RANGE_BOUNDED}" '["TableScan"]'
  run_exact_query_phase pre_transfer_minimum_point "${address}" \
    "${QUERY_MIN}" "${REFERENCE_HEADER}" "" '[0,1]' 0 \
    "${RANGE_MIN}" '["TableScan"]'
  run_exact_query_phase pre_transfer_maximum_point "${address}" \
    "${QUERY_MAX}" "${REFERENCE_HEADER}" "" '[0,1]' 0 \
    "${RANGE_MAX}" '["TableScan"]'
  run_empty_range_phase pre_transfer_contradiction "${QUERY_CONTRADICTION}" '[0,1]'
}

scenario_transferred_to_b() {
  run_exact_query_phase transferred_to_b_convergence "$1" \
    "${QUERY_ID_LE}" "${REFERENCE_HEADER}" "${ROWS_ID_LE}" '[0,1]' 0 \
    "${RANGE_ID_LE}" '["TableScan"]' converge
  run_exact_query_phase transferred_to_b "$1" \
    "${QUERY_ID_GT}" "${REFERENCE_HEADER}" "${ROWS_ID_GT}" '[0,1]' 0 \
    "${RANGE_ID_GT}" '["TableScan"]'
}

scenario_failed_over_to_c() {
  run_exact_query_phase failed_over_to_c_convergence "$1" \
    "${QUERY_ID_GE}" "${REFERENCE_HEADER}" "${ROWS_ID_GE}" '[0,1]' 0 \
    "${RANGE_ID_GE}" '["TableScan"]' converge
  run_exact_query_phase failed_over_to_c "$1" \
    "${QUERY_RESIDUAL}" "${REFERENCE_HEADER}" "${ROWS_RESIDUAL}" '[0,1]' 1 \
    "${RANGE_RESIDUAL}" '["TableScan","Selection"]'
}

scenario_returned_to_b() {
  run_exact_query_phase returned_to_b_convergence "$1" \
    "${QUERY_ID_NE}" "${REFERENCE_HEADER}" "${ROWS_ID_NE}" '[0,1]' 0 \
    "${RANGE_ID_NE}" '["TableScan"]' converge
  run_exact_query_phase returned_to_b "$1" \
    "${QUERY_ID_EQ}" "${REFERENCE_HEADER}" "${ROWS_ID_EQ}" '[0,1]' 0 \
    "${RANGE_ID_EQ}" '["TableScan"]'
}

scenario_validate_block_snapshot() {
  [[ -n "$1" ]] && range_snapshot_matches "$1" '["TableScan"]' \
    "${RANGE_ID_EQ}" 0 '[0,1]'
}

scenario_emit_success_receipt() {
  echo "clustered-PK range SQL-node live clustered-PK range proof passed: same_rust_pid=${ORIGINAL_RUST_PID}; persistent_connection_id=${PERSISTENT_CONNECTION_ID}; persistent_session_id=${PERSISTENT_SESSION_ID}; region_id=${REGION_ID}; leader_stores=${STORE_A}->${STORE_B}->${STORE_C}->${STORE_B}; range_matrix=eq,ne,lt,le,gt,ge,reversed,bounded,min,max,contradiction,residual; exact_point_range=${RANGE_ID_EQ}; split_not_equal_range_count=2; contradiction_ranges=[]; contradiction_snapshot_ts=null; contradiction_transport_publications=0; residual_plan=TableScan,Selection; residual_ranges=${RANGE_RESIDUAL}; residual_output_offsets=[0,1]; physical_addresses=${ADDRESS_A}->${ADDRESS_B}->${ADDRESS_C}->${ADDRESS_B}; b_channel_versions=${B_VERSION_BEFORE}->${B_VERSION_AFTER}; b_stream_generations=${B_GENERATION_BEFORE}->${B_GENERATION_AFTER}; blocked_id_eq_publication=$(printf '%s' "${BLOCK_PUBLICATION}" | jq -c '{connection_id,query_id,authority_id,session_id,region_id,physical_address,physical_channel_version,stream_generation}'); shutdown_order=connections,region_cache,tikv_transport,pd,sql_node_stopped; shutdown_elapsed_ms=${SHUTDOWN_ELAPSED_MS}; shutdown_grace_ms=${SHUTDOWN_GRACE_MS}; rust_exit=0; ${FINAL_CONNECTIONS}; authority_id=${AUTHORITY_ID}; read_authority_id=${READ_AUTHORITY_ID}; pd_cluster_id=${PD_CLUSTER_ID}"
}

run_live_sql_node_topology_scenario
