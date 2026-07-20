#!/usr/bin/env bash

# Real prepared point-read and one-thread sysbench proof.  The shared harness
# owns TiUP, topology churn, cancellation, shutdown, and tag-scoped cleanup.

set -euo pipefail

CAMPAIGN_LABEL="Campaign 27"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${SCRIPT_DIR}/lib/live-sql-node-harness.sh"

SCENARIO_ENV_PREFIX=C27
SCENARIO_TAG_SLUG=campaign27-prepared-point-read-sql-node
SCENARIO_DATABASE=campaign27
SCENARIO_AUTH_USER=campaign27
SCENARIO_AUTH_DEFAULT_PASSWORD=campaign27-native-password
SCENARIO_EXPECTS_QUERY_SNAPSHOT=true
SCENARIO_RELATION_ZERO_LOCK_HANDLE=1
SCENARIO_RELATION_ZERO_LOCK_UPDATE_SQL='UPDATE campaign27.rows SET balance = balance + 1 WHERE id = 1;'
REFERENCE_QUERY='SELECT balance FROM campaign27.rows WHERE id = 1;'
REFERENCE_HEADER=balance
REFERENCE_ROWS=107
SCENARIO_BLOCK_QUERY=${REFERENCE_QUERY}
RAW_CLIENT="${SCRIPT_DIR}/mysql-prepared-client.py"
SYSBENCH_SCRIPT="${SCRIPT_DIR}/sysbench-prepared-point-select.lua"

event_count() {
  grep -c -F "\"event\":\"$1\"" "${RUST_LOG}" 2>/dev/null || true
}

wait_for_event_total() {
  local event=$1
  local expected=$2
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    local actual
    actual=$(event_count "${event}")
    if [[ "${actual}" -ge "${expected}" ]]; then
      [[ "${actual}" -eq "${expected}" ]]
      return
    fi
    sleep 0.05
  done
  echo "${CAMPAIGN_LABEL} timed out waiting for ${expected} ${event} events" >&2
  return 1
}

sysbench_command_receipt_is_exact() {
  jq -e '
    .text_query_commands == 0
    and .stmt_prepare_commands == 1
    and .stmt_prepare_successes == 1
    and .stmt_execute_commands == 8
    and .stmt_execute_successes == 8
    and .stmt_close_commands == 1' >/dev/null
}

if [[ "${1:-}" == --self-test-live-harness ]]; then
  live_sql_node_harness_self_test
  "${RAW_CLIENT}" self-test
  SYSBENCH_BIN=${C27_SYSBENCH:-sysbench}
  command -v "${SYSBENCH_BIN}" >/dev/null
  "${SYSBENCH_BIN}" "${SYSBENCH_SCRIPT}" --db-driver=mysql \
    --db-ps-mode=auto --mysql-db= help >/dev/null
  grep -F 'sysbench.sql.type.BIGINT' "${SYSBENCH_SCRIPT}" >/dev/null
  grep -F 'SELECT balance FROM campaign27.rows WHERE id = ?' \
    "${SYSBENCH_SCRIPT}" >/dev/null
  grep -F 'result.nrows ~= 1' "${SYSBENCH_SCRIPT}" >/dev/null
  ! rg -n 'CREATE|DROP|INSERT|UPDATE|DELETE|db-ps-mode=disable' \
    "${SYSBENCH_SCRIPT}" >/dev/null
  ! rg -n 'fetch_row' "${SYSBENCH_SCRIPT}" >/dev/null
  SYSBENCH_INVOCATION=$(sed -n \
    '/"${sysbench_bin}" --db-driver=mysql/,/"${SYSBENCH_SCRIPT}" run/p' \
    "${BASH_SOURCE[0]}")
  ! printf '%s\n' "${SYSBENCH_INVOCATION}" \
    | grep -E -- '--events=0|--time=5' >/dev/null
  grep -F -- '--threads=1 --table-size=16 --events=8 --time=30' \
    "${BASH_SOURCE[0]}" >/dev/null
  printf '%s\n' '{"text_query_commands":0,"stmt_prepare_commands":1,"stmt_prepare_successes":1,"stmt_execute_commands":8,"stmt_execute_successes":8,"stmt_close_commands":1}' \
    | sysbench_command_receipt_is_exact
  ! printf '%s\n' '{"text_query_commands":8,"stmt_prepare_commands":0,"stmt_prepare_successes":0,"stmt_execute_commands":0,"stmt_execute_successes":0,"stmt_close_commands":0}' \
    | sysbench_command_receipt_is_exact
  echo "Campaign 27 prepared live-harness self-test passed"
  exit 0
fi

scenario_prepare_fixture() {
  mysql_go <<SQL
DROP DATABASE IF EXISTS ${SCENARIO_DATABASE};
CREATE DATABASE ${SCENARIO_DATABASE};
CREATE TABLE ${SCENARIO_DATABASE}.rows (
  id BIGINT PRIMARY KEY CLUSTERED,
  balance BIGINT NOT NULL
);
INSERT INTO ${SCENARIO_DATABASE}.rows VALUES
  (1, 107), (2, 207), (3, 307), (4, 407),
  (5, 507), (6, 607), (7, 707), (8, 807),
  (9, 907), (10, 1007), (11, 1107), (12, 1207),
  (13, 1307), (14, 1407), (15, 1507), (16, 1607);
CREATE TABLE ${SCENARIO_DATABASE}.lock_secondary (
  id BIGINT PRIMARY KEY CLUSTERED,
  value BIGINT NOT NULL
);
INSERT INTO ${SCENARIO_DATABASE}.lock_secondary VALUES (1, 1);
SET SESSION tidb_wait_split_region_finish = 1;
SPLIT TABLE ${SCENARIO_DATABASE}.lock_secondary BY (1);
SQL
}

scenario_configure_server_arguments() {
  default_live_sql_node_server_arguments
  if ! command -v shasum >/dev/null 2>&1; then
    echo "missing ${CAMPAIGN_LABEL} Rust binary identity prerequisite: shasum" >&2
    return 1
  fi
  RUST_SERVER_BINARY=$(cd "$(dirname "${RUST_SERVER}")" && pwd)/$(basename "${RUST_SERVER}")
  RUST_SERVER_SHA256=$(shasum -a 256 "${RUST_SERVER_BINARY}" | awk '{ print $1 }')
  if [[ ! "${RUST_SERVER_SHA256}" =~ ^[0-9a-f]{64}$ ]]; then
    echo "${CAMPAIGN_LABEL} could not identify the exact Rust server binary" >&2
    return 1
  fi
}

run_text_point_phase() {
  local phase=$1
  local expected_address=${2:-}
  local before_publications before_transports before_snapshots before_output before_errors
  before_publications=$(publication_count)
  before_transports=$(transport_count)
  before_snapshots=$(snapshot_count)
  before_output=$(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}")
  before_errors=$(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_ERROR}")
  printf '%s\n' "${REFERENCE_QUERY}" >&9
  wait_for_new_event_count query_snapshot "${before_snapshots}" "${before_errors}"
  wait_for_new_event_count query_transport_published "${before_publications}" "${before_errors}"
  wait_for_new_event_count query_transport "${before_transports}" "${before_errors}"
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]] \
    && [[ $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}") -lt $((before_output + 2)) ]]; do
    sleep 0.05
  done
  if [[ $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}") -ne $((before_output + 2)) \
    || $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_ERROR}") -ne "${before_errors}" ]]; then
    echo "${CAMPAIGN_LABEL} ${phase} stock point read did not finish exactly" >&2
    tail -80 "${PERSISTENT_CLIENT_ERROR}" >&2
    return 1
  fi
  local output="${RUNTIME_DIR}/${phase}.out"
  sed -n "$((before_output + 1)),$((before_output + 2))p" \
    "${PERSISTENT_CLIENT_OUTPUT}" >"${output}"
  query_output_is_exact "${output}" "${REFERENCE_HEADER}" "${REFERENCE_ROWS}" || {
    echo "${CAMPAIGN_LABEL} ${phase} returned the wrong seeded row" >&2
    return 1
  }
  local publication
  publication=$(grep -F '"event":"query_transport_published"' "${RUST_LOG}" \
    | tail -n +$((before_publications + 1)) | tail -1)
  if ! printf '%s\n' "${publication}" | jq -e \
    --arg address "${expected_address}" --arg authority "${AUTHORITY_ID}" \
    --arg region "${REGION_ID:-}" \
    '($address == "" or .physical_address == $address)
     and ($region == "" or (.region_id | tostring) == $region)
     and (.authority_id | tostring) == $authority
     and .forwarded_host == null' >/dev/null; then
    echo "${CAMPAIGN_LABEL} ${phase} did not publish the expected physical route" >&2
    printf '%s\n' "${publication}" >&2
    return 1
  fi
  PHASE_PUBLICATION=${publication}
  PHASE_CONNECTION_ID=$(printf '%s\n' "${publication}" | jq -r '.connection_id')
  PHASE_QUERY_ID=$(printf '%s\n' "${publication}" | jq -r '.query_id')
  PHASE_SESSION_ID=$(printf '%s\n' "${publication}" | jq -r '.session_id')
  if [[ -z "${PERSISTENT_CONNECTION_ID}" ]]; then
    PERSISTENT_CONNECTION_ID=${PHASE_CONNECTION_ID}
    PERSISTENT_SESSION_ID=${PHASE_SESSION_ID}
  elif [[ "${PHASE_CONNECTION_ID}" != "${PERSISTENT_CONNECTION_ID}" \
    || "${PHASE_SESSION_ID}" != "${PERSISTENT_SESSION_ID}" ]]; then
    echo "${CAMPAIGN_LABEL} ${phase} left the persistent stock session" >&2
    return 1
  fi
  local snapshot transport
  snapshot=$(grep -F '"event":"query_snapshot"' "${RUST_LOG}" \
    | tail -n +$((before_snapshots + 1)) | tail -1)
  transport=$(grep -F '"event":"query_transport"' "${RUST_LOG}" \
    | tail -n +$((before_transports + 1)) | tail -1)
  printf '%s\n' "${snapshot}" | jq -e \
    --argjson table "${TABLE_ID}" --argjson query "${PHASE_QUERY_ID}" \
    '.query_id == $query and .table_id == $table
     and .executor_kinds == ["TableScan"] and .predicate_count == 0
     and .output_offsets == [0]
     and .handle_ranges == [{"low":1,"high":1,"low_exclude":false,"high_exclude":false}]
     and (.snapshot_ts | type) == "number" and .snapshot_ts > 0' >/dev/null
  printf '%s\n' "${transport}" | jq -e \
    --argjson query "${PHASE_QUERY_ID}" \
    '.query_id == $query and .batch_attempts >= 1 and .unary_attempts == 0' >/dev/null
}

scenario_pre_transfer_discovery() {
  run_text_point_phase pre_transfer_discovery
}

scenario_pre_transfer_verified() {
  run_text_point_phase pre_transfer_verified "$1"
}

scenario_transferred_to_b() {
  run_text_point_phase transferred_to_b_convergence
  run_text_point_phase transferred_to_b "$1"
}

scenario_failed_over_to_c() {
  run_text_point_phase failed_over_to_c_convergence
  run_text_point_phase failed_over_to_c "$1"
}

scenario_returned_to_b() {
  run_text_point_phase returned_to_b_convergence
  run_text_point_phase returned_to_b "$1"
}

assert_zero_storage_delta() {
  local label=$1 before_activities=$2 before_snapshots=$3 before_publications=$4 before_transports=$5
  sleep 0.2
  if [[ $(event_count query_activity) -ne "${before_activities}" \
    || $(snapshot_count) -ne "${before_snapshots}" \
    || $(publication_count) -ne "${before_publications}" \
    || $(transport_count) -ne "${before_transports}" ]]; then
    echo "${CAMPAIGN_LABEL} ${label} reached query/PD/TiKV work" >&2
    return 1
  fi
}

run_raw_prepared_proof() {
  local negative_output="${RUNTIME_DIR}/raw-negative.jsonl"
  local before_activities before_snapshots before_publications before_transports
  before_activities=$(event_count query_activity)
  before_snapshots=$(snapshot_count)
  before_publications=$(publication_count)
  before_transports=$(transport_count)
  "${RAW_CLIENT}" negative --port "${RUST_SQL_PORT}" --user "${AUTH_USER}" \
    --password "${AUTH_PASSWORD}" --database "${SCENARIO_DATABASE}" >"${negative_output}"
  jq -e 'select(.event == "prepared_negative_matrix")
    | .case_count == 16
      and ([.cases[].case] | unique | length) == 16
      and all(.cases[]; (.code == 1210 or .code == 1243 or .code == 1105))' \
    "${negative_output}" >/dev/null
  assert_zero_storage_delta raw_negative_matrix "${before_activities}" \
    "${before_snapshots}" "${before_publications}" "${before_transports}"

  local positive_output="${RUNTIME_DIR}/raw-positive.jsonl"
  before_activities=$(event_count query_activity)
  before_snapshots=$(snapshot_count)
  before_publications=$(publication_count)
  before_transports=$(transport_count)
  "${RAW_CLIENT}" positive --port "${RUST_SQL_PORT}" --user "${AUTH_USER}" \
    --password "${AUTH_PASSWORD}" --database "${SCENARIO_DATABASE}" \
    --first-id 1 --first-balance 107 --second-id 16 --second-balance 1607 \
    >"${positive_output}"
  RAW_SUMMARY=$(jq -c 'select(.event == "prepared_positive")' "${positive_output}")
  if [[ -z "${RAW_SUMMARY}" ]] || ! printf '%s\n' "${RAW_SUMMARY}" | jq -e \
    '.parameter_type == 8 and .result_type == 8
     and .first == {"id":1,"balance":107}
     and .second == {"id":16,"balance":1607,"type_reuse":true}
     and .close == "silent" and .after_close.code == 1243' >/dev/null; then
    echo "${CAMPAIGN_LABEL} raw positive client omitted exact binary/type-reuse evidence" >&2
    cat "${positive_output}" >&2
    return 1
  fi
  wait_for_event_total query_snapshot $((before_snapshots + 2))
  wait_for_event_total query_transport_published $((before_publications + 2))
  wait_for_event_total query_transport $((before_transports + 2))
  wait_for_event_total query_activity $((before_activities + 4))
  RAW_CONNECTION_ID=$(printf '%s\n' "${RAW_SUMMARY}" | jq -r '.connection_id')
  local snapshots publications transports activities
  snapshots=$(grep -F '"event":"query_snapshot"' "${RUST_LOG}" \
    | tail -n +$((before_snapshots + 1)))
  publications=$(grep -F '"event":"query_transport_published"' "${RUST_LOG}" \
    | tail -n +$((before_publications + 1)))
  transports=$(grep -F '"event":"query_transport"' "${RUST_LOG}" \
    | tail -n +$((before_transports + 1)))
  activities=$(grep -F '"event":"query_activity"' "${RUST_LOG}" \
    | tail -n +$((before_activities + 1)))
  if ! printf '%s\n' "${snapshots}" | jq -s -e \
    --argjson connection "${RAW_CONNECTION_ID}" --argjson authority "${AUTHORITY_ID}" \
    --argjson table "${TABLE_ID}" \
    'length == 2
     and all(.[]; .connection_id == $connection and .authority_id == $authority
       and .table_id == $table and .executor_kinds == ["TableScan"]
       and .predicate_count == 0 and .output_offsets == [0]
       and (.snapshot_ts | type) == "number" and .snapshot_ts > 0)
     and .[0].session_id == .[1].session_id
     and .[0].snapshot_ts < .[1].snapshot_ts
     and .[0].handle_ranges == [{"low":1,"high":1,"low_exclude":false,"high_exclude":false}]
     and .[1].handle_ranges == [{"low":16,"high":16,"low_exclude":false,"high_exclude":false}]' \
    >/dev/null \
    || ! printf '%s\n' "${publications}" | jq -s -e \
      --argjson connection "${RAW_CONNECTION_ID}" --argjson region "${REGION_ID}" \
      --arg address "${ADDRESS_B}" \
      'length == 2 and all(.[]; .connection_id == $connection
        and .region_id == $region and .physical_address == $address
        and .forwarded_host == null)' >/dev/null \
    || ! printf '%s\n' "${transports}" | jq -s -e \
      --argjson connection "${RAW_CONNECTION_ID}" --argjson region "${REGION_ID}" \
      'length == 2 and all(.[]; .connection_id == $connection
        and .located_region_ids == [$region] and .dispatched_region_ids == [$region]
        and .batch_attempts >= 1 and .unary_attempts == 0)' >/dev/null \
    || ! printf '%s\n' "${activities}" | jq -s -e \
      --argjson connection "${RAW_CONNECTION_ID}" \
      'length == 4 and all(.[]; .connection_id == $connection)
       and [.[].phase] == ["begin","end","begin","end"]' >/dev/null; then
    echo "${CAMPAIGN_LABEL} raw positive receipts did not correlate to two real point reads" >&2
    return 1
  fi
  RAW_SESSION_ID=$(printf '%s\n' "${snapshots}" | head -1 | jq -r '.session_id')
}

run_sysbench_prepared_proof() {
  local sysbench_bin
  sysbench_bin=$(scenario_environment_value SYSBENCH sysbench)
  if ! command -v otool >/dev/null 2>&1; then
    echo "missing ${CAMPAIGN_LABEL} sysbench linkage prerequisite: otool" >&2
    return 1
  fi
  if ! command -v "${sysbench_bin}" >/dev/null 2>&1; then
    echo "${SCENARIO_ENV_PREFIX}_SYSBENCH must name a real sysbench executable" >&2
    return 1
  fi
  SYSBENCH_BINARY=$(command -v "${sysbench_bin}")
  SYSBENCH_BINARY=$(cd "$(dirname "${SYSBENCH_BINARY}")" && pwd)/$(basename "${SYSBENCH_BINARY}")
  SYSBENCH_VERSION=$("${sysbench_bin}" --version | head -1)
  SYSBENCH_CLIENT_LIBRARY=$(otool -L "${SYSBENCH_BINARY}" \
    | awk '/lib(mysqlclient|mariadb)/ { print $1; exit }')
  if [[ -z "${SYSBENCH_CLIENT_LIBRARY}" ]]; then
    echo "${CAMPAIGN_LABEL} sysbench is not linked to a real MySQL client library" >&2
    return 1
  fi
  local output="${RUNTIME_DIR}/sysbench.out"
  local before_begins before_closes before_snapshots before_publications before_transports before_activities
  before_begins=$(event_count connection_begin)
  before_closes=$(event_count connection_closed)
  before_snapshots=$(snapshot_count)
  before_publications=$(publication_count)
  before_transports=$(transport_count)
  before_activities=$(event_count query_activity)
  local sysbench_status
  set +e
  "${sysbench_bin}" --db-driver=mysql --db-ps-mode=auto \
    --mysql-host=127.0.0.1 --mysql-port="${RUST_SQL_PORT}" \
    --mysql-user="${AUTH_USER}" --mysql-password="${AUTH_PASSWORD}" --mysql-db= \
    --mysql-ssl=off --mysql-compression=off \
    --threads=1 --table-size=16 --events=8 --time=30 --report-interval=1 \
    --rand-type=uniform "${SYSBENCH_SCRIPT}" run >"${output}" 2>&1
  sysbench_status=$?
  set -e
  SYSBENCH_EVENTS=$(awk '/total number of events:/ { print $NF }' "${output}" | tail -1)
  if [[ "${sysbench_status}" -ne 0 || "${SYSBENCH_EVENTS}" != 8 ]] \
    || grep -E 'FATAL|ERROR|SQL error' "${output}" >/dev/null \
    || ! grep -E 'ignored errors:[[:space:]]+0' "${output}" >/dev/null; then
    echo "${CAMPAIGN_LABEL} sysbench did not complete clean prepared events" >&2
    cat "${output}" >&2
    return 1
  fi
  wait_for_event_total connection_begin $((before_begins + 1))
  wait_for_event_total connection_closed $((before_closes + 1))
  wait_for_event_total query_snapshot $((before_snapshots + SYSBENCH_EVENTS))
  wait_for_event_total query_transport_published $((before_publications + SYSBENCH_EVENTS))
  wait_for_event_total query_transport $((before_transports + SYSBENCH_EVENTS))
  wait_for_event_total query_activity $((before_activities + SYSBENCH_EVENTS * 2))
  SYSBENCH_CONNECTION_ID=$(grep -F '"event":"connection_begin"' "${RUST_LOG}" \
    | tail -n +$((before_begins + 1)) | jq -r '.connection_id')
  local close snapshots publications transports activities
  close=$(grep -F '"event":"connection_closed"' "${RUST_LOG}" \
    | tail -n +$((before_closes + 1)) | tail -1)
  snapshots=$(grep -F '"event":"query_snapshot"' "${RUST_LOG}" \
    | tail -n +$((before_snapshots + 1)))
  publications=$(grep -F '"event":"query_transport_published"' "${RUST_LOG}" \
    | tail -n +$((before_publications + 1)))
  transports=$(grep -F '"event":"query_transport"' "${RUST_LOG}" \
    | tail -n +$((before_transports + 1)))
  activities=$(grep -F '"event":"query_activity"' "${RUST_LOG}" \
    | tail -n +$((before_activities + 1)))
  if ! printf '%s\n' "${close}" | jq -e \
    --argjson connection "${SYSBENCH_CONNECTION_ID}" \
    '.connection_id == $connection and .failed == 0' >/dev/null \
    || ! printf '%s\n' "${close}" | sysbench_command_receipt_is_exact \
    || ! printf '%s\n' "${snapshots}" | jq -s -e \
      --argjson count "${SYSBENCH_EVENTS}" --argjson connection "${SYSBENCH_CONNECTION_ID}" \
      --argjson table "${TABLE_ID}" \
      'length == $count and all(.[]; .connection_id == $connection
        and .table_id == $table and .executor_kinds == ["TableScan"]
        and .predicate_count == 0 and .output_offsets == [0]
        and (.snapshot_ts | type) == "number" and .snapshot_ts > 0
        and (.handle_ranges | length) == 1
        and .handle_ranges[0].low == .handle_ranges[0].high
        and .handle_ranges[0].low >= 1 and .handle_ranges[0].low <= 16)
       and ([.[].session_id] | unique | length) == 1
       and ([.[].query_id] | unique | length) == $count' >/dev/null \
    || ! printf '%s\n' "${publications}" | jq -s -e \
      --argjson count "${SYSBENCH_EVENTS}" --argjson connection "${SYSBENCH_CONNECTION_ID}" \
      --argjson region "${REGION_ID}" --arg address "${ADDRESS_B}" \
      'length == $count and all(.[]; .connection_id == $connection
        and .region_id == $region and .physical_address == $address
        and .forwarded_host == null)' >/dev/null \
    || ! printf '%s\n' "${transports}" | jq -s -e \
      --argjson count "${SYSBENCH_EVENTS}" --argjson connection "${SYSBENCH_CONNECTION_ID}" \
      --argjson region "${REGION_ID}" \
      'length == $count and all(.[]; .connection_id == $connection
        and .located_region_ids == [$region] and .dispatched_region_ids == [$region]
        and .batch_attempts >= 1 and .unary_attempts == 0)' >/dev/null \
    || ! printf '%s\n' "${activities}" | jq -s -e \
      --argjson count "${SYSBENCH_EVENTS}" --argjson connection "${SYSBENCH_CONNECTION_ID}" \
      'length == ($count * 2) and all(.[]; .connection_id == $connection)
       and ([.[] | select(.phase == "begin")] | length) == $count
       and ([.[] | select(.phase == "end")] | length) == $count' >/dev/null; then
    echo "${CAMPAIGN_LABEL} sysbench receipts did not match successful prepared events" >&2
    return 1
  fi
  SYSBENCH_TEXT_QUERY_COMMANDS=$(printf '%s\n' "${close}" | jq -r '.text_query_commands')
  SYSBENCH_STMT_PREPARE_COMMANDS=$(printf '%s\n' "${close}" | jq -r '.stmt_prepare_commands')
  SYSBENCH_STMT_PREPARE_SUCCESSES=$(printf '%s\n' "${close}" | jq -r '.stmt_prepare_successes')
  SYSBENCH_STMT_EXECUTE_COMMANDS=$(printf '%s\n' "${close}" | jq -r '.stmt_execute_commands')
  SYSBENCH_STMT_EXECUTE_SUCCESSES=$(printf '%s\n' "${close}" | jq -r '.stmt_execute_successes')
  SYSBENCH_STMT_CLOSE_COMMANDS=$(printf '%s\n' "${close}" | jq -r '.stmt_close_commands')
  SYSBENCH_SESSION_ID=$(printf '%s\n' "${snapshots}" | head -1 | jq -r '.session_id')
}

scenario_pre_shutdown_proof() {
  "${RAW_CLIENT}" self-test >/dev/null
  run_raw_prepared_proof
  run_sysbench_prepared_proof
}

scenario_validate_block_snapshot() {
  [[ -n "$1" ]] && printf '%s\n' "$1" | jq -e \
    '.executor_kinds == ["TableScan"] and .predicate_count == 0
     and .output_offsets == [0]
     and .handle_ranges == [{"low":1,"high":1,"low_exclude":false,"high_exclude":false}]' \
    >/dev/null
}

scenario_emit_success_receipt() {
  echo "Campaign 27 live prepared point-read proof passed: go_release=${GO_RELEASE_VERSION}; go_commit=${GO_COMMIT_HASH}; go_test_api=active; rust_binary=${RUST_SERVER_BINARY}; rust_sha256=${RUST_SERVER_SHA256}; rust_pid=${ORIGINAL_RUST_PID}; raw_connection_id=${RAW_CONNECTION_ID}; raw_session_id=${RAW_SESSION_ID}; raw_executes=2; raw_type_reuse=true; raw_close=silent; negative_cases=16; sysbench_binary=${SYSBENCH_BINARY}; sysbench_version=${SYSBENCH_VERSION}; sysbench_client_library=${SYSBENCH_CLIENT_LIBRARY}; sysbench_threads=1; sysbench_events=8; sysbench_time_cap_seconds=30; sysbench_connection_id=${SYSBENCH_CONNECTION_ID}; sysbench_session_id=${SYSBENCH_SESSION_ID}; sysbench_text_query_commands=${SYSBENCH_TEXT_QUERY_COMMANDS}; sysbench_stmt_prepare_commands=${SYSBENCH_STMT_PREPARE_COMMANDS}; sysbench_stmt_prepare_successes=${SYSBENCH_STMT_PREPARE_SUCCESSES}; sysbench_stmt_execute_commands=${SYSBENCH_STMT_EXECUTE_COMMANDS}; sysbench_stmt_execute_successes=${SYSBENCH_STMT_EXECUTE_SUCCESSES}; sysbench_stmt_close_commands=${SYSBENCH_STMT_CLOSE_COMMANDS}; table_id=${TABLE_ID}; region_id=${REGION_ID}; topology=${STORE_A}->${STORE_B}->${STORE_C}->${STORE_B}; physical_address=${ADDRESS_B}; shutdown_elapsed_ms=${SHUTDOWN_ELAPSED_MS}; shutdown_grace_ms=${SHUTDOWN_GRACE_MS}; ${FINAL_CONNECTIONS}; cleanup=tag-owned"
}

run_live_sql_node_topology_scenario
