#!/usr/bin/env bash

# Live, bounded ORDER BY/LIMIT proof over the configured two-relation TiKV
# read path.  The shared harness owns TiUP, the persistent client, topology
# churn, blocked shutdown, and cleanup; this file owns only the C26 fixture,
# statement assertions, and receipt interpretation.

set -euo pipefail

CAMPAIGN_LABEL="ordered-join SQL node"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${SCRIPT_DIR}/lib/live-sql-node-harness.sh"

SCENARIO_ENV_PREFIX=ORDERED_JOIN_SQL
SCENARIO_TAG_SLUG=campaign26-ordered-join-sql-node
SCENARIO_DATABASE=campaign26
SCENARIO_AUTH_USER=campaign26
SCENARIO_AUTH_DEFAULT_PASSWORD=campaign26-native-password
SCENARIO_RELATION_ZERO_LOCK_HANDLE=-7
SCENARIO_RELATION_ZERO_LOCK_UPDATE_SQL='UPDATE campaign26.left_rows SET payload = payload + 1 WHERE id = -7;'
# ordered-join SQL-node intentionally sends typed rejections over the same authenticated
# session, so the stock client must retain that session after each error.
SCENARIO_PERSISTENT_CLIENT_FORCE=true

JOIN_FROM='FROM campaign26.left_rows AS l INNER JOIN campaign26.right_rows AS r ON l.join_key = r.join_key'
TOPN_QUALIFIED="SELECT l.id AS left_id, r.id AS right_id, r.payload AS amount ${JOIN_FROM} ORDER BY r.payload DESC, l.id ASC LIMIT 2;"
TOPN_COMMA_OFFSET="SELECT l.id AS left_id, r.id AS right_id, r.payload AS amount ${JOIN_FROM} ORDER BY r.payload DESC, l.id ASC LIMIT 1,2;"
TOPN_ALIAS_TIE="SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} ORDER BY left_id DESC LIMIT 2;"
TOPN_ORDINAL="SELECT l.id AS left_id, r.id AS right_id, r.payload AS amount ${JOIN_FROM} ORDER BY 3 DESC, 1 ASC LIMIT 2;"
TOPN_UNPROJECTED="SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} ORDER BY r.payload DESC, l.id ASC LIMIT 2;"
LIMIT_OFFSET="SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} LIMIT 2 OFFSET 1;"
LIMIT_COMMA="SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} LIMIT 1,2;"
TOPN_ZERO="SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} ORDER BY r.payload DESC LIMIT 0;"
TOPN_NO_MATCH="SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} WHERE r.id = 99 ORDER BY l.id ASC LIMIT 2;"
SCENARIO_BLOCK_QUERY="${TOPN_QUALIFIED}"
# The shared topology lifecycle verifies the reference through Go TiDB before
# each leader transition; the Rust query helpers below additionally require
# its exact physical output order.
REFERENCE_QUERY="${TOPN_QUALIFIED}"
REFERENCE_HEADER=$'left_id\tright_id\tamount'
REFERENCE_ROWS=$'42\t2\t3000\n43\t2\t3000'

if [[ "${1:-}" == --self-test-live-harness ]]; then
  live_sql_node_harness_self_test
  [[ "${SCENARIO_RELATION_ZERO_LOCK_HANDLE}" == -7 ]]
  [[ "${SCENARIO_PERSISTENT_CLIENT_FORCE}" == true ]]
  [[ "${TOPN_QUALIFIED}" == *'ORDER BY r.payload DESC, l.id ASC LIMIT 2;' ]]
  [[ "${LIMIT_OFFSET}" == *'LIMIT 2 OFFSET 1;' ]]
  echo "ordered-join SQL-node ordered multi-relation harness self-test passed"
  exit 0
fi

scenario_relation_table_names() {
  printf '%s\n' left_rows right_rows
}

scenario_prepare_fixture() {
  mysql_go <<SQL
DROP DATABASE IF EXISTS ${SCENARIO_DATABASE};
CREATE DATABASE ${SCENARIO_DATABASE};
CREATE TABLE ${SCENARIO_DATABASE}.left_rows (
  id BIGINT PRIMARY KEY CLUSTERED,
  join_key BIGINT NOT NULL,
  payload BIGINT NOT NULL
);
CREATE TABLE ${SCENARIO_DATABASE}.right_rows (
  id BIGINT PRIMARY KEY CLUSTERED,
  join_key BIGINT NOT NULL,
  payload BIGINT NOT NULL
);
INSERT INTO ${SCENARIO_DATABASE}.left_rows VALUES
  (-7, 10, 700), (42, 30, 900), (43, 30, 900),
  (44, 40, 700), (45, 40, 700);
INSERT INTO ${SCENARIO_DATABASE}.right_rows VALUES
  (1, 10, 1000), (2, 30, 3000), (3, 40, 3000), (4, 40, 3000);
CREATE TABLE ${SCENARIO_DATABASE}.lock_secondary (
  id BIGINT PRIMARY KEY CLUSTERED,
  value BIGINT NOT NULL
);
INSERT INTO ${SCENARIO_DATABASE}.lock_secondary VALUES (1, 1);
SET SESSION tidb_wait_split_region_finish = 1;
SPLIT TABLE ${SCENARIO_DATABASE}.left_rows BY (-7);
SPLIT TABLE ${SCENARIO_DATABASE}.lock_secondary BY (1);
SQL
}

scenario_configure_server_arguments() {
  RUST_SERVER_ARGS=(
    --path "${PD_ADDR}" --store tikv
    --host 127.0.0.1 --port "${RUST_SQL_PORT}"
    --read-table "${SCENARIO_DATABASE}" left_rows "${LEFT_TABLE_ID}" 3
    id:1:clustered-pk join_key:2:stored-not-null payload:3:stored-not-null
    --read-table "${SCENARIO_DATABASE}" right_rows "${RIGHT_TABLE_ID}" 3
    id:1:clustered-pk join_key:2:stored-not-null payload:3:stored-not-null
    --auth-file "${AUTH_FILE}" --max-connections 4 --max-topn-rows 3
    --connection-timeout-ms "${CONNECTION_TIMEOUT_MS}"
  )
}

scenario_validate_ready_json() {
  local ready_json=$1
  printf '%s\n' "${ready_json}" | jq -e \
    --arg database "${SCENARIO_DATABASE}" \
    --argjson left "${LEFT_TABLE_ID}" --argjson right "${RIGHT_TABLE_ID}" \
    '.tables == [
      {database: $database, table: "left_rows", table_id: $left,
       columns: ["id:1:clustered-pk", "join_key:2:stored-not-null", "payload:3:stored-not-null"]},
      {database: $database, table: "right_rows", table_id: $right,
       columns: ["id:1:clustered-pk", "join_key:2:stored-not-null", "payload:3:stored-not-null"]}
    ]' >/dev/null
}

line_count() {
  awk 'END { print NR + 0 }' "$1"
}

ordered_output_is_exact() {
  local output=$1
  local header=$2
  local rows=$3
  if [[ -z "${rows}" ]]; then
    [[ ! -s "${output}" ]]
    return
  fi
  printf '%s\n%s\n' "${header}" "${rows}" | cmp -s - "${output}"
}

wait_for_ordered_output() {
  local before_output=$1
  local expected_lines=$2
  local before_errors=$3
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  local stable=0
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    local output_lines
    output_lines=$(line_count "${PERSISTENT_CLIENT_OUTPUT}")
    if [[ "${output_lines}" -eq $((before_output + expected_lines)) ]]; then
      stable=$((stable + 1))
      [[ "${stable}" -ge 3 ]] && return
    else
      stable=0
    fi
    if ! pid_is_running "${PERSISTENT_CLIENT_PID}" \
      || [[ $(line_count "${PERSISTENT_CLIENT_ERROR}") -gt "${before_errors}" ]]; then
      return 1
    fi
    sleep 0.05
  done
  return 1
}

wait_for_ordered_event() {
  local event=$1
  local before=$2
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    if [[ $(grep -c -F "\"event\":\"${event}\"" "${RUST_LOG}" 2>/dev/null || true) -gt "${before}" ]]; then
      return
    fi
    sleep 0.05
  done
  echo "${CAMPAIGN_LABEL} timed out waiting for ${event}" >&2
  return 1
}

assert_ordered_plan() {
  local plan=$1
  local mode=$2
  local keys=$3
  local offset=$4
  local count=$5
  local input_required=$6
  printf '%s\n' "${plan}" | jq -e \
    --arg mode "${mode}" --argjson keys "${keys}" \
    --argjson offset "${offset}" --argjson count "${count}" \
    --argjson input_required "${input_required}" \
    '(.connection_id | type) == "number" and .connection_id > 0
     and (.query_id | type) == "number" and .query_id > 0
     and .mode == $mode and .order_keys == $keys
     and .limit_offset == $offset and .limit_count == $count
     and .limit_end_exclusive == ($offset + $count)
     and .capacity == 3 and .input_required == $input_required' >/dev/null
}

assert_ordered_accounting() {
  local plan=$1
  local kind=$2
  local before=$3
  local expected_consumed=$4
  local expected_emitted=$5
  local expected_skipped=${6:-0}
  local query_id
  query_id=$(printf '%s\n' "${plan}" | jq -r '.query_id')
  local event="query_ordered_${kind}"
  local receipt
  receipt=$(grep -F "\"event\":\"${event}\"" "${RUST_LOG}" \
    | tail -n +$((before + 1)) | jq -c --argjson query "${query_id}" \
      'select(.query_id == $query)' | tail -1)
  [[ -n "${receipt}" ]] || return 1
  if [[ "${kind}" == topn ]]; then
    printf '%s\n' "${receipt}" | jq -e \
      --argjson query "${query_id}" --argjson consumed "${expected_consumed}" \
      --argjson emitted "${expected_emitted}" \
      '(.query_id == $query) and .capacity == 3
       and .high_water_candidates <= 3
       and .rows_consumed == $consumed and .rows_emitted == $emitted' >/dev/null
  else
    printf '%s\n' "${receipt}" | jq -e \
      --argjson query "${query_id}" --argjson requested "${expected_consumed}" \
      --argjson skipped "${expected_skipped}" --argjson emitted "${expected_emitted}" \
      '(.query_id == $query) and .rows_requested == $requested
       and .rows_skipped == $skipped and .rows_emitted == $emitted
       and .source_closed == true' >/dev/null
  fi
}

run_ordered_query() {
  local phase=$1
  local query=$2
  local header=$3
  local rows=$4
  local mode=$5
  local keys=$6
  local offset=$7
  local count=$8
  local consumed=$9
  local emitted=${10}
  local skipped=${11:-0}
  local output="${RUNTIME_DIR}/${phase}.out"
  local before_output before_errors before_plan before_accounting expected_rows expected_lines
  before_output=$(line_count "${PERSISTENT_CLIENT_OUTPUT}")
  before_errors=$(line_count "${PERSISTENT_CLIENT_ERROR}")
  before_plan=$(grep -c -F '"event":"query_ordered_plan"' "${RUST_LOG}" 2>/dev/null || true)
  before_accounting=$(grep -c -F "\"event\":\"query_ordered_${mode}\"" "${RUST_LOG}" 2>/dev/null || true)
  expected_rows=$(printf '%s\n' "${rows}" | sed '/^[[:space:]]*$/d' | awk 'END { print NR + 0 }')
  expected_lines=$(expected_persistent_client_output_lines "${expected_rows}")
  printf '%s\n' "${query}" >&9
  wait_for_ordered_event query_ordered_plan "${before_plan}"
  if ! wait_for_ordered_output "${before_output}" "${expected_lines}" "${before_errors}"; then
    echo "${CAMPAIGN_LABEL} ${phase} persistent ordered query did not complete" >&2
    tail -40 "${PERSISTENT_CLIENT_OUTPUT}" >&2
    tail -80 "${PERSISTENT_CLIENT_ERROR}" >&2
    return 1
  fi
  if [[ $(line_count "${PERSISTENT_CLIENT_ERROR}") -ne "${before_errors}" ]]; then
    echo "${CAMPAIGN_LABEL} ${phase} unexpectedly wrote client errors" >&2
    tail -80 "${PERSISTENT_CLIENT_ERROR}" >&2
    return 1
  fi
  if [[ "${expected_lines}" -gt 0 ]]; then
    sed -n "$((before_output + 1)),$((before_output + expected_lines))p" \
      "${PERSISTENT_CLIENT_OUTPUT}" >"${output}"
  else
    : >"${output}"
  fi
  if ! ordered_output_is_exact "${output}" "${header}" "${rows}"; then
    echo "${CAMPAIGN_LABEL} ${phase} ordered output mismatch" >&2
    sed -n '1,60p' "${output}" >&2
    return 1
  fi
  local plan
  plan=$(grep -F '"event":"query_ordered_plan"' "${RUST_LOG}" \
    | tail -n +$((before_plan + 1)) | tail -1)
  assert_ordered_plan "${plan}" "${mode}" "${keys}" "${offset}" "${count}" true || {
    echo "${CAMPAIGN_LABEL} ${phase} omitted the expected typed order/limit plan" >&2
    printf '%s\n' "${plan}" >&2
    return 1
  }
  wait_for_ordered_event "query_ordered_${mode}" "${before_accounting}"
  local query_id snapshot
  query_id=$(printf '%s\n' "${plan}" | jq -r '.query_id')
  snapshot=$(grep -F '"event":"query_multi_snapshot"' "${RUST_LOG}" \
    | tail -n +1 | jq -c --argjson query "${query_id}" \
      'select(.query_id == $query)' | tail -1)
  if [[ -z "${snapshot}" ]] || ! printf '%s\n' "${snapshot}" | jq -e \
    --argjson query "${query_id}" --argjson left "${LEFT_TABLE_ID}" \
    --argjson right "${RIGHT_TABLE_ID}" \
    '(.query_id == $query) and (.snapshot_ts | type) == "number" and .snapshot_ts > 0
     and (.relations | length) == 2
     and .relations[0].table_id == $left and .relations[1].table_id == $right' >/dev/null; then
    echo "${CAMPAIGN_LABEL} ${phase} plan did not correlate to one real two-table snapshot" >&2
    printf '%s\n' "${snapshot}" >&2
    return 1
  fi
  if ! assert_ordered_accounting "${plan}" "${mode}" "${before_accounting}" \
    "${consumed}" "${emitted}" "${skipped}"; then
    echo "${CAMPAIGN_LABEL} ${phase} ordered accounting did not match the bounded plan" >&2
    return 1
  fi
}

run_limit_zero_query() {
  local before_output before_errors before_plan before_snapshot before_publication before_transport before_topn before_limit
  before_output=$(line_count "${PERSISTENT_CLIENT_OUTPUT}")
  before_errors=$(line_count "${PERSISTENT_CLIENT_ERROR}")
  before_plan=$(grep -c -F '"event":"query_ordered_plan"' "${RUST_LOG}" 2>/dev/null || true)
  before_snapshot=$(multi_snapshot_count)
  before_publication=$(multi_publication_count)
  before_transport=$(multi_transport_count)
  before_topn=$(grep -c -F '"event":"query_ordered_topn"' "${RUST_LOG}" 2>/dev/null || true)
  before_limit=$(grep -c -F '"event":"query_ordered_limit"' "${RUST_LOG}" 2>/dev/null || true)
  printf '%s\n' "${TOPN_ZERO}" >&9
  wait_for_ordered_event query_ordered_plan "${before_plan}"
  wait_for_ordered_output "${before_output}" 0 "${before_errors}"
  local plan
  plan=$(grep -F '"event":"query_ordered_plan"' "${RUST_LOG}" | tail -n +$((before_plan + 1)) | tail -1)
  assert_ordered_plan "${plan}" topn '[{"full_schema_offset":5,"direction":"desc"}]' 0 0 false || return 1
  [[ $(line_count "${PERSISTENT_CLIENT_ERROR}") -eq "${before_errors}" \
    && $(multi_snapshot_count) -eq "${before_snapshot}" \
    && $(multi_publication_count) -eq "${before_publication}" \
    && $(multi_transport_count) -eq "${before_transport}" \
    && $(grep -c -F '"event":"query_ordered_topn"' "${RUST_LOG}" 2>/dev/null || true) -eq "${before_topn}" \
    && $(grep -c -F '"event":"query_ordered_limit"' "${RUST_LOG}" 2>/dev/null || true) -eq "${before_limit}" ]] || {
      echo "${CAMPAIGN_LABEL} LIMIT 0 performed physical or terminal execution work" >&2
      return 1
    }
}

run_rejected_query() {
  local phase=$1
  local query=$2
  local expected=$3
  local before_errors before_plan before_snapshot before_publication before_transport
  before_errors=$(line_count "${PERSISTENT_CLIENT_ERROR}")
  before_plan=$(grep -c -F '"event":"query_ordered_plan"' "${RUST_LOG}" 2>/dev/null || true)
  before_snapshot=$(multi_snapshot_count)
  before_publication=$(multi_publication_count)
  before_transport=$(multi_transport_count)
  printf '%s\n' "${query}" >&9
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]] \
    && [[ $(line_count "${PERSISTENT_CLIENT_ERROR}") -eq "${before_errors}" ]]; do
    sleep 0.05
  done
  if [[ $(line_count "${PERSISTENT_CLIENT_ERROR}") -le "${before_errors}" ]] \
    || ! tail -n +$((before_errors + 1)) "${PERSISTENT_CLIENT_ERROR}" | grep -F "${expected}" >/dev/null \
    || [[ $(grep -c -F '"event":"query_ordered_plan"' "${RUST_LOG}" 2>/dev/null || true) -ne "${before_plan}" ]] \
    || [[ $(multi_snapshot_count) -ne "${before_snapshot}" ]] \
    || [[ $(multi_publication_count) -ne "${before_publication}" ]] \
    || [[ $(multi_transport_count) -ne "${before_transport}" ]]; then
    echo "${CAMPAIGN_LABEL} ${phase} did not fail before ordered read side effects" >&2
    tail -80 "${PERSISTENT_CLIENT_ERROR}" >&2
    return 1
  fi
}

scenario_pre_transfer_discovery() {
  run_ordered_query qualified "${TOPN_QUALIFIED}" $'left_id\tright_id\tamount' \
    $'42\t2\t3000\n43\t2\t3000' topn \
    '[{"full_schema_offset":5,"direction":"desc"},{"full_schema_offset":0,"direction":"asc"}]' 0 2 7 2
}

scenario_pre_transfer_verified() {
  run_ordered_query comma_offset "${TOPN_COMMA_OFFSET}" $'left_id\tright_id\tamount' \
    $'43\t2\t3000\n44\t3\t3000' topn \
    '[{"full_schema_offset":5,"direction":"desc"},{"full_schema_offset":0,"direction":"asc"}]' 1 2 7 2
  run_ordered_query alias_stable_tie "${TOPN_ALIAS_TIE}" $'left_id\tright_id' \
    $'45\t3\n45\t4' topn '[{"full_schema_offset":0,"direction":"desc"}]' 0 2 7 2
  run_ordered_query positive_ordinal "${TOPN_ORDINAL}" $'left_id\tright_id\tamount' \
    $'42\t2\t3000\n43\t2\t3000' topn \
    '[{"full_schema_offset":5,"direction":"desc"},{"full_schema_offset":0,"direction":"asc"}]' 0 2 7 2
  run_ordered_query unprojected_order_key "${TOPN_UNPROJECTED}" $'left_id\tright_id' \
    $'42\t2\n43\t2' topn \
    '[{"full_schema_offset":5,"direction":"desc"},{"full_schema_offset":0,"direction":"asc"}]' 0 2 7 2
  run_ordered_query limit_offset "${LIMIT_OFFSET}" $'left_id\tright_id' $'42\t2\n43\t2' \
    limit '[]' 1 2 3 2 1
  run_ordered_query limit_comma "${LIMIT_COMMA}" $'left_id\tright_id' $'42\t2\n43\t2' \
    limit '[]' 1 2 3 2 1
  run_ordered_query no_match "${TOPN_NO_MATCH}" $'left_id\tright_id' '' \
    topn '[{"full_schema_offset":0,"direction":"asc"}]' 0 2 0 0
  run_limit_zero_query
  run_rejected_query cap_excess \
    "SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} ORDER BY l.id ASC LIMIT 4;" \
    'configured TopN end 4 exceeds capacity 3'
  run_rejected_query order_without_limit \
    "SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} ORDER BY l.id ASC;" \
    OrderRequiresLimit
  run_rejected_query zero_ordinal \
    "SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} ORDER BY 0 LIMIT 1;" \
    InvalidOrderOrdinal
  run_rejected_query expression_key \
    "SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} ORDER BY l.id + 1 LIMIT 1;" \
    UnsupportedOrderExpression
  run_rejected_query arithmetic_limit \
    "SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} ORDER BY l.id ASC LIMIT 1 + 1;" \
    InvalidLimitLiteral
  run_rejected_query overflow_limit \
    "SELECT l.id AS left_id, r.id AS right_id ${JOIN_FROM} ORDER BY l.id ASC LIMIT 18446744073709551616;" \
    InvalidLimitLiteral
}

scenario_transferred_to_b() {
  run_ordered_query transferred_to_b "${TOPN_QUALIFIED}" $'left_id\tright_id\tamount' \
    $'42\t2\t3000\n43\t2\t3000' topn \
    '[{"full_schema_offset":5,"direction":"desc"},{"full_schema_offset":0,"direction":"asc"}]' 0 2 7 2
}

scenario_failed_over_to_c() {
  run_ordered_query failed_over_to_c "${TOPN_COMMA_OFFSET}" $'left_id\tright_id\tamount' \
    $'43\t2\t3000\n44\t3\t3000' topn \
    '[{"full_schema_offset":5,"direction":"desc"},{"full_schema_offset":0,"direction":"asc"}]' 1 2 7 2
}

scenario_returned_to_b() {
  run_ordered_query returned_to_b "${TOPN_UNPROJECTED}" $'left_id\tright_id' $'42\t2\n43\t2' \
    topn '[{"full_schema_offset":5,"direction":"desc"},{"full_schema_offset":0,"direction":"asc"}]' 0 2 7 2
}

scenario_validate_block_snapshot() {
  local snapshot=$1
  local query_id
  query_id=$(printf '%s\n' "${snapshot}" | jq -r '.query_id // 0')
  printf '%s\n' "${snapshot}" | jq -e \
    --argjson left "${LEFT_TABLE_ID}" --argjson right "${RIGHT_TABLE_ID}" \
    '(.snapshot_ts | type) == "number" and .snapshot_ts > 0
     and (.relations | length) == 2
     and .relations[0].table_id == $left and .relations[1].table_id == $right' >/dev/null \
    && grep -F '"event":"query_ordered_plan"' "${RUST_LOG}" | jq -e \
      --argjson query "${query_id}" \
      'select(.query_id == $query) | .mode == "topn"
       and .order_keys == [{"full_schema_offset":5,"direction":"desc"},{"full_schema_offset":0,"direction":"asc"}]
       and .limit_offset == 0 and .limit_count == 2 and .capacity == 3 and .input_required == true' >/dev/null
}

scenario_emit_success_receipt() {
  echo "ordered-join SQL-node live ordered join proof passed: rust_pid=${ORIGINAL_RUST_PID}; persistent_connection_id=${PERSISTENT_CONNECTION_ID}; persistent_session_id=${PERSISTENT_SESSION_ID}; tables=${LEFT_TABLE_ID},${RIGHT_TABLE_ID}; duplicate_order_keys=right.payload,left.id; topn_cap=3; ordered_shapes=qualified,comma-offset,alias-stable-tie,positive-ordinal,unprojected-key; limit_shapes=offset,comma,zero; topology=${STORE_A}->${STORE_B}->${STORE_C}->${STORE_B}; shutdown_elapsed_ms=${SHUTDOWN_ELAPSED_MS}; shutdown_grace_ms=${SHUTDOWN_GRACE_MS}; ${FINAL_CONNECTIONS}"
}

run_live_sql_node_multi_relation_scenario
