#!/usr/bin/env bash

# Shared, source-only helpers for the live SQL-node campaigns. Callers own the
# SQL fixture and feature-specific assertions; this file owns process identity,
# persistent-client framing, topology observation, and bounded cleanup.

expected_persistent_client_output_lines() {
  local row_count=$1
  if [[ "${row_count}" -eq 0 ]]; then
    printf '0\n'
  else
    printf '%s\n' "$((row_count + 1))"
  fi
}

command_is_tag_owned() {
  local command=$1
  [[ "${command}" == *"${TAG}"* || "${command}" == *"${TAG_DIR}"* ]]
}

tag_status_rows() {
  tiup status | awk -v tag="${TAG}" \
    'NR > 2 && ($1 == tag || index($0, "/data/" tag "/")) { print }'
}

now_millis() {
  perl -MTime::HiRes=time -e 'printf "%.0f\n", time() * 1000'
}

tag_owned_pids() {
  pgrep -f "${TAG_DIR}" || true
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

merge_owned_pids() {
  {
    local command
    local pid
    for pid in ${OWNED_PIDS}; do
      command=$(ps -ww -p "${pid}" -o command= 2>/dev/null || true)
      if command_is_tag_owned "${command}"; then
        printf '%s\n' "${pid}"
      fi
    done
    if [[ -n "${PLAYGROUND_PID}" ]]; then
      collect_descendant_pids "${PLAYGROUND_PID}"
    fi
    if [[ -n "${RESTART_PID}" ]]; then
      printf '%s\n' "${RESTART_PID}"
    fi
    tag_owned_pids
  } | awk 'NF && !seen[$1]++ { print $1 }' | tr '\n' ' '
}

pid_is_running() {
  local pid=$1
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 1
  fi
  local state
  state=$(ps -o stat= -p "${pid}" 2>/dev/null | awk 'NR == 1 { print $1 }')
  [[ -n "${state}" && "${state}" != Z* ]]
}

wait_for_pids_until() {
  local deadline=$1
  shift
  while true; do
    local running=false
    local pid
    for pid in "$@"; do
      if [[ -n "${pid}" ]] && pid_is_running "${pid}"; then
        running=true
        break
      fi
    done
    if [[ "${running}" == false ]]; then
      return 0
    fi
    if [[ $(date +%s) -ge "${deadline}" ]]; then
      return 1
    fi
    sleep 0.1
  done
}

terminate_pid_group() {
  local label=$1
  shift
  local pid
  local running_pids=()
  for pid in "$@"; do
    if [[ -n "${pid}" ]] && pid_is_running "${pid}"; then
      running_pids+=("${pid}")
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done
  if [[ ${#running_pids[@]} -eq 0 ]]; then
    for pid in "$@"; do
      if [[ -n "${pid}" ]]; then
        wait "${pid}" 2>/dev/null || true
      fi
    done
    return 0
  fi

  local deadline=$(( $(date +%s) + PROCESS_STOP_TIMEOUT ))
  local forced=false
  if ! wait_for_pids_until "${deadline}" "${running_pids[@]}"; then
    forced=true
    for pid in "${running_pids[@]}"; do
      if pid_is_running "${pid}"; then
        kill -KILL "${pid}" 2>/dev/null || true
      fi
    done
    deadline=$(( $(date +%s) + PROCESS_STOP_TIMEOUT ))
    if ! wait_for_pids_until "${deadline}" "${running_pids[@]}"; then
      echo "${CAMPAIGN_LABEL} cleanup failed: ${label} remained alive after SIGKILL" >&2
      return 1
    fi
  fi
  for pid in "$@"; do
    if [[ -n "${pid}" ]]; then
      wait "${pid}" 2>/dev/null || true
    fi
  done
  if [[ "${forced}" == true ]]; then
    echo "${CAMPAIGN_LABEL} cleanup failed: ${label} required SIGKILL" >&2
    return 1
  fi
  return 0
}

terminate_playground_supervisor() {
  local pid=$1
  if [[ -z "${pid}" ]] || ! pid_is_running "${pid}"; then
    if [[ -n "${pid}" ]]; then
      wait "${pid}" 2>/dev/null || true
    fi
    return 0
  fi

  kill -TERM "${pid}" 2>/dev/null || true
  local deadline=$(( $(date +%s) + PROCESS_STOP_TIMEOUT ))
  if ! wait_for_pids_until "${deadline}" "${pid}"; then
    # TiUP's interactive supervisor can retain the PID of a deliberately
    # killed TiKV. Force only the tag-owned supervisor after all exact service
    # descendants have stopped.
    local command
    local descendants
    command=$(ps -ww -p "${pid}" -o command= 2>/dev/null || true)
    descendants=$(collect_descendant_pids "${pid}")
    if [[ -n "${descendants}" ]]; then
      local service_pids=()
      local service_pid
      for service_pid in ${descendants}; do
        service_pids+=("${service_pid}")
      done
      if ! terminate_pid_group \
        "TiUP-managed playground services after supervisor drain" \
        "${service_pids[@]}"; then
        return 1
      fi
      descendants=$(collect_descendant_pids "${pid}")
    fi
    if [[ "${command}" != *"${TAG}"* || -n "${descendants}" ]]; then
      echo "${CAMPAIGN_LABEL} cleanup failed: TiUP supervisor could not be safely contained" >&2
      return 1
    fi
    kill -KILL "${pid}" 2>/dev/null || true
    deadline=$(( $(date +%s) + PROCESS_STOP_TIMEOUT ))
    if ! wait_for_pids_until "${deadline}" "${pid}"; then
      echo "${CAMPAIGN_LABEL} cleanup failed: TiUP supervisor remained alive after bounded force" >&2
      return 1
    fi
    echo "${CAMPAIGN_LABEL} cleanup contained the stale TiUP supervisor after all services stopped" >&2
  fi
  wait "${pid}" 2>/dev/null || true
  return 0
}

address_port() {
  local address=$1
  address=${address%/}
  printf '%s\n' "${address##*:}"
}

archive_mysql_logs() {
  if [[ -z "${RUNTIME_DIR}" ]] || [[ ! -d "${RUNTIME_DIR}" ]]; then
    return
  fi
  local path
  for path in "${RUNTIME_DIR}"/*.out "${RUNTIME_DIR}"/*.err; do
    if [[ -f "${path}" ]]; then
      printf '%s_begin\n' "$(basename "${path}")" >>"${MYSQL_LOG}"
      sed -n '1,200p' "${path}" >>"${MYSQL_LOG}"
      printf '%s_end\n' "$(basename "${path}")" >>"${MYSQL_LOG}"
    fi
  done
}

cleanup() {
  local original_status=$?
  local cleanup_failed=false
  trap - EXIT INT TERM

  local stopped_pid
  if [[ ${#STOPPED_PIDS[@]} -gt 0 ]]; then
    for stopped_pid in "${STOPPED_PIDS[@]}"; do
      if [[ -n "${stopped_pid}" ]] && kill -0 "${stopped_pid}" 2>/dev/null; then
        kill -CONT "${stopped_pid}" 2>/dev/null || true
      fi
    done
  fi
  STOPPED_PIDS=()
  if [[ "${PERSISTENT_CLIENT_FD_OPEN}" == true ]]; then
    exec 9>&-
    PERSISTENT_CLIENT_FD_OPEN=false
  fi
  if [[ "${PREWRITE_FAILPOINT_ENABLED}" == true ]]; then
    curl -sf --max-time 2 -X DELETE \
      "http://127.0.0.1:${GO_STATUS_PORT}/fail/tikvclient/beforeCommitSecondaries" \
      >/dev/null 2>&1 || true
    PREWRITE_FAILPOINT_ENABLED=false
  fi
  if [[ ${#CLIENT_PIDS[@]} -gt 0 ]]; then
    local client_tree=()
    local client_pid
    for client_pid in "${CLIENT_PIDS[@]}"; do
      if [[ -n "${client_pid}" ]]; then
        client_tree+=("${client_pid}")
        local descendants
        descendants=$(collect_descendant_pids "${client_pid}")
        local descendant
        for descendant in ${descendants}; do
          client_tree+=("${descendant}")
        done
      fi
    done
    if ! terminate_pid_group "stock MySQL client process tree" "${client_tree[@]}"; then
      cleanup_failed=true
    fi
  fi
  archive_mysql_logs

  if [[ -n "${RUST_PID}" ]] \
    && ! terminate_pid_group "Rust SQL node ${RUST_PID}" "${RUST_PID}"; then
    cleanup_failed=true
  fi
  if nc -z -w 1 127.0.0.1 "${RUST_SQL_PORT}" >/dev/null 2>&1; then
    echo "${CAMPAIGN_LABEL} cleanup failed: Rust SQL node ${RUST_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  local handled_restart_pid=${RESTART_PID}
  if [[ -n "${RESTART_PID}" ]] \
    && ! terminate_pid_group "restarted TiKV ${RESTART_PID}" "${RESTART_PID}"; then
    cleanup_failed=true
  fi
  RESTART_PID=
  if [[ -n "${PLAYGROUND_PID}" ]] \
    && ! terminate_playground_supervisor "${PLAYGROUND_PID}"; then
    cleanup_failed=true
  fi
  OWNED_PIDS=$(merge_owned_pids)
  local exact_tag_pids=()
  local owned_pid
  for owned_pid in ${OWNED_PIDS}; do
    if [[ "${owned_pid}" == "${RUST_PID}" \
      || "${owned_pid}" == "${handled_restart_pid}" \
      || "${owned_pid}" == "${PLAYGROUND_PID}" ]]; then
      continue
    fi
    local handled_client=false
    if [[ ${#CLIENT_PIDS[@]} -gt 0 ]]; then
      local client_pid
      for client_pid in "${CLIENT_PIDS[@]}"; do
        if [[ "${owned_pid}" == "${client_pid}" ]]; then
          handled_client=true
          break
        fi
      done
    fi
    if [[ "${handled_client}" == false ]]; then
      exact_tag_pids+=("${owned_pid}")
    fi
  done
  if [[ ${#exact_tag_pids[@]} -gt 0 ]] \
    && ! terminate_pid_group "orphaned exact tag-owned playground services" "${exact_tag_pids[@]}"; then
    cleanup_failed=true
  fi
  local registered_rows
  registered_rows=$(tag_status_rows 2>/dev/null || true)
  if [[ -n "${registered_rows}" ]] || [[ -d "${TAG_DIR}" ]]; then
    if ! tiup clean "${TAG}" --all >/dev/null 2>&1; then
      echo "${CAMPAIGN_LABEL} cleanup failed: tiup clean failed for ${TAG}" >&2
      cleanup_failed=true
    fi
  fi

  local processes_cleaned=false
  for _ in $(seq 1 30); do
    OWNED_PIDS=$(merge_owned_pids)
    local alive=false
    local pid
    for pid in ${OWNED_PIDS}; do
      if kill -0 "${pid}" 2>/dev/null; then
        alive=true
        break
      fi
    done
    local rows
    rows=$(tag_status_rows 2>/dev/null || true)
    if [[ "${alive}" == false ]] && [[ -z "${rows}" ]]; then
      processes_cleaned=true
      break
    fi
    sleep 1
  done
  if [[ "${processes_cleaned}" != true ]]; then
    echo "${CAMPAIGN_LABEL} cleanup failed: owned process or TiUP registry row remains" >&2
    cleanup_failed=true
  fi

  local endpoint
  for endpoint in ${PD_ENDPOINTS}; do
    if curl -sf --max-time 1 "${endpoint}/pd/api/v1/version" >/dev/null; then
      echo "${CAMPAIGN_LABEL} cleanup failed: PD endpoint ${endpoint} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  for endpoint in ${PD_PEER_ENDPOINTS}; do
    local port
    port=$(address_port "${endpoint}")
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "${CAMPAIGN_LABEL} cleanup failed: PD peer endpoint ${endpoint} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  local address
  for address in ${STORE_ADDRESSES}; do
    local port
    port=$(address_port "${address}")
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "${CAMPAIGN_LABEL} cleanup failed: TiKV ${address} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  for address in ${STORE_STATUS_ADDRESSES}; do
    local port
    port=$(address_port "${address}")
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "${CAMPAIGN_LABEL} cleanup failed: TiKV status endpoint ${address} remains reachable" >&2
      cleanup_failed=true
    fi
  done
  if nc -z -w 1 127.0.0.1 "${GO_SQL_PORT}" >/dev/null 2>&1; then
    echo "${CAMPAIGN_LABEL} cleanup failed: Go TiDB ${GO_SQL_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi
  if nc -z -w 1 127.0.0.1 "${GO_STATUS_PORT}" >/dev/null 2>&1; then
    echo "${CAMPAIGN_LABEL} cleanup failed: Go TiDB status port remains reachable" >&2
    cleanup_failed=true
  fi
  if curl -sf --max-time 1 "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; then
    echo "${CAMPAIGN_LABEL} cleanup failed: PD seed ${PD_ADDR} remains reachable" >&2
    cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == false ]]; then
    rm -rf -- "${TAG_DIR}" "${RUNTIME_DIR}"
    if [[ -e "${TAG_DIR}" || -e "${RUNTIME_DIR}" || -e "${AUTH_FILE}" ]]; then
      echo "${CAMPAIGN_LABEL} cleanup failed: owned data/auth/runtime path remains" >&2
      cleanup_failed=true
    fi
  fi

  if [[ "${cleanup_failed}" == false ]] && [[ "${original_status}" -eq 0 ]]; then
    rm -f -- "${PLAYGROUND_LOG}" "${RUST_LOG}" "${MYSQL_LOG}" "${RESTART_LOG}"
    echo "${CAMPAIGN_LABEL} cleanup proof passed: tag processes stopped, endpoints unreachable, data/auth/runtime removed"
  else
    echo "${CAMPAIGN_LABEL} retained logs: ${PLAYGROUND_LOG} ${RUST_LOG} ${MYSQL_LOG} ${RESTART_LOG}" >&2
  fi
  if [[ "${cleanup_failed}" == true ]]; then
    exit 1
  fi
  exit "${original_status}"
}

handle_interrupt() {
  exit 130
}

handle_terminate() {
  exit 143
}

mysql_go() {
  "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
    -uroot --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" "$@"
}

query_output_is_exact() {
  local output=$1
  local expected_header=$2
  local expected_rows=$3
  local order=${4:-lexical}
  [[ -f "${output}" ]] || return 1
  if [[ -z "${expected_rows}" ]]; then
    [[ ! -s "${output}" ]]
    return
  fi
  local header
  header=$(sed -n '1p' "${output}")
  [[ "${header}" == "${expected_header}" ]] || return 1
  local rows
  if [[ "${order}" == numeric_second_column ]]; then
    rows=$(tail -n +2 "${output}" | sed '/^[[:space:]]*$/d' \
      | LC_ALL=C sort -t $'\t' -k2,2n)
    expected_rows=$(printf '%s\n' "${expected_rows}" | sed '/^[[:space:]]*$/d' \
      | LC_ALL=C sort -t $'\t' -k2,2n)
  else
    rows=$(tail -n +2 "${output}" | sed '/^[[:space:]]*$/d' | LC_ALL=C sort)
    expected_rows=$(printf '%s\n' "${expected_rows}" | sed '/^[[:space:]]*$/d' | LC_ALL=C sort)
  fi
  [[ "${rows}" == "${expected_rows}" ]]
}

publication_count() {
  grep -c -F '"event":"query_transport_published"' "${RUST_LOG}" 2>/dev/null || true
}

transport_count() {
  grep -c -F '"event":"query_transport"' "${RUST_LOG}" 2>/dev/null || true
}

snapshot_count() {
  grep -c -F '"event":"query_snapshot"' "${RUST_LOG}" 2>/dev/null || true
}

wait_for_new_event_count() {
  local event=$1
  local before=$2
  local client_error_lines_before=${3:-}
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    if [[ -n "${RUST_PID}" ]] && ! pid_is_running "${RUST_PID}"; then
      echo "Rust SQL node exited while waiting for ${event}" >&2
      return 1
    fi
    if [[ -n "${PERSISTENT_CLIENT_PID}" ]] \
      && ! pid_is_running "${PERSISTENT_CLIENT_PID}"; then
      echo "persistent stock client exited while waiting for ${event}" >&2
      return 1
    fi
    if [[ -n "${client_error_lines_before}" ]] \
      && [[ $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_ERROR}") \
        -gt "${client_error_lines_before}" ]]; then
      echo "persistent stock client reported an error while waiting for ${event}" >&2
      tail -40 "${PERSISTENT_CLIENT_ERROR}" >&2
      return 1
    fi
    local count
    count=$(grep -c -F "\"event\":\"${event}\"" "${RUST_LOG}" 2>/dev/null || true)
    if [[ "${count}" -gt "${before}" ]]; then
      return 0
    fi
    sleep 0.05
  done
  echo "timed out waiting for new ${event}" >&2
  return 1
}

leader_facts_match() {
  local target=$1
  local expected_address=$2
  local expected_peer=$3
  local region
  local store_state
  local port
  region=$(region_json 2>/dev/null) || return 1
  if ! printf '%s\n' "${region}" | jq -e \
    --argjson target "${target}" --argjson peer "${expected_peer}" \
    '.leader.store_id == $target
     and any(.peers[]?; .store_id == $target and .id == $peer and .role_name == "Voter")
     and all(.pending_peers[]?; .store_id != $target and .id != $peer)
     and all(.down_peers[]?; .peer.store_id != $target and .peer.id != $peer)' \
    >/dev/null 2>&1; then
    return 1
  fi
  store_state=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r --argjson target "${target}" \
      '.stores[] | select(.store.id == $target) | [.store.state_name, (.store.node_state_name // "Serving"), .store.address] | @tsv') \
    || return 1
  [[ "${store_state}" == $'Up\tServing\t'"${expected_address}" ]] || return 1
  port=$(address_port "${expected_address}")
  nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1
}

wait_for_leader_serving() {
  local target=$1
  local expected_address=$2
  local expected_peer=$3
  local phase=$4
  local consecutive=0
  local output="${RUNTIME_DIR}/${phase}-go-read.out"
  local error="${RUNTIME_DIR}/${phase}-go-read.err"
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    if leader_facts_match "${target}" "${expected_address}" "${expected_peer}" \
      && mysql_go -B -e "${REFERENCE_QUERY}" >"${output}" 2>"${error}" \
      && query_output_is_exact "${output}" "${REFERENCE_HEADER}" "${REFERENCE_ROWS}" \
      && leader_facts_match "${target}" "${expected_address}" "${expected_peer}"; then
      consecutive=$((consecutive + 1))
      if [[ "${consecutive}" -ge 2 ]]; then
        return 0
      fi
    else
      consecutive=0
    fi
    sleep 0.2
  done
  echo "${phase} did not hold exact leader/peer/store/listener facts across two exact Go reads" >&2
  sed -n '1,80p' "${error}" >&2
  return 1
}

store_address() {
  local store_id=$1
  curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r --argjson id "${store_id}" \
      '.stores[] | select(.store.id == $id) | .store.address' | head -1
}

region_json() {
  curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/region/id/${REGION_ID}"
}

transfer_leader() {
  local target=$1
  local region_id=${2:-${REGION_ID}}
  local output=
  local deadline=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${deadline}" ]]; do
    output=$(tiup ctl:v8.5.6 pd -u "http://${PD_ADDR}" \
      operator add transfer-leader "${region_id}" "${target}" 2>&1) || true
    for _ in $(seq 1 20); do
      local current
      current=$(curl -sf --max-time 2 \
        "http://${PD_ADDR}/pd/api/v1/region/id/${region_id}" \
        | jq -r '.leader.store_id // 0' 2>/dev/null) || true
      if [[ "${current}" == "${target}" ]]; then
        return 0
      fi
      sleep 0.1
    done
    sleep 0.5
  done
  echo "region ${region_id} did not transfer leadership to store ${target}" >&2
  printf '%s\n' "${output}" >&2
  return 1
}

live_sql_node_harness_self_test() {
  [[ $(expected_persistent_client_output_lines 0) == 0 ]]
  [[ $(expected_persistent_client_output_lines 1) == 2 ]]
  [[ $(expected_persistent_client_output_lines 3) == 4 ]]
  [[ $(address_port '127.0.0.1:20160') == 20160 ]]
  [[ $(address_port 'http://127.0.0.1:2379/') == 2379 ]]

  local previous_tag=${TAG:-}
  local previous_tag_dir=${TAG_DIR:-}
  TAG=campaign-self-test
  TAG_DIR=/tmp/campaign-self-test
  command_is_tag_owned 'tiup playground --tag campaign-self-test'
  command_is_tag_owned 'tikv-server --data-dir /tmp/campaign-self-test/data'
  ! command_is_tag_owned 'tikv-server --data-dir /tmp/unrelated/data'

  local fixture
  fixture=$(mktemp "${TMPDIR:-/tmp}/live-sql-node-harness.XXXXXX")
  printf 'amount\tid\n77\t42\n913\t-7\n' >"${fixture}"
  query_output_is_exact "${fixture}" $'amount\tid' $'913\t-7\n77\t42'
  query_output_is_exact "${fixture}" $'amount\tid' $'913\t-7\n77\t42' \
    numeric_second_column
  : >"${fixture}"
  query_output_is_exact "${fixture}" $'amount\tid' ''
  rm -f -- "${fixture}"

  terminate_pid_group "self-test absent child" 999999999
  TAG=${previous_tag}
  TAG_DIR=${previous_tag_dir}
}

scenario_environment_value() {
  local suffix=$1
  local fallback=${2:-}
  local name="${SCENARIO_ENV_PREFIX}_${suffix}"
  printf '%s\n' "${!name:-${fallback}}"
}

initialize_live_sql_node_scenario() {
  for prerequisite in tiup cargo curl jq nc lsof pgrep ps awk sed seq grep sort tail mktemp mkfifo openssl chmod date kill perl; do
    if ! command -v "${prerequisite}" >/dev/null 2>&1; then
      echo "missing ${CAMPAIGN_LABEL} prerequisite: ${prerequisite}" >&2
      return 1
    fi
  done

  MYSQL_CLIENT=$(scenario_environment_value MYSQL_CLIENT mysql)
  if ! command -v "${MYSQL_CLIENT}" >/dev/null 2>&1; then
    echo "${SCENARIO_ENV_PREFIX}_MYSQL_CLIENT must name an executable stock MySQL or MariaDB client" >&2
    return 1
  fi
  MYSQL_PLUGIN_ARGS=()
  local plugin_dir
  plugin_dir=$(scenario_environment_value MYSQL_PLUGIN_DIR)
  if [[ -n "${plugin_dir}" ]]; then
    if [[ ! -f "${plugin_dir}/mysql_native_password.so" ]]; then
      echo "${SCENARIO_ENV_PREFIX}_MYSQL_PLUGIN_DIR does not contain mysql_native_password.so" >&2
      return 1
    fi
    MYSQL_PLUGIN_ARGS=(--plugin-dir="${plugin_dir}")
  else
    local mysql_bin_dir
    mysql_bin_dir=$(cd "$(dirname "$(command -v "${MYSQL_CLIENT}")")" && pwd)
    local candidate
    for candidate in \
      "${mysql_bin_dir}/../opt/mysql-client/lib/plugin" \
      /opt/homebrew/opt/mysql-client/lib/plugin \
      /usr/local/opt/mysql-client/lib/plugin; do
      if [[ -f "${candidate}/mysql_native_password.so" ]]; then
        MYSQL_PLUGIN_ARGS=(--plugin-dir="${candidate}")
        break
      fi
    done
  fi

  RUST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
  GO_TIDB_SERVER=$(scenario_environment_value GO_TIDB_SERVER)
  if [[ -z "${GO_TIDB_SERVER}" || ! -x "${GO_TIDB_SERVER}" ]]; then
    echo "${SCENARIO_ENV_PREFIX}_GO_TIDB_SERVER must name an executable failpoint-enabled tidb-server" >&2
    return 1
  fi
  if ! LC_ALL=C grep -a -q 'beforeCommitSecondaries' "${GO_TIDB_SERVER}"; then
    echo "${SCENARIO_ENV_PREFIX}_GO_TIDB_SERVER does not contain the required client-go commit failpoint" >&2
    return 1
  fi
  TAG="${SCENARIO_TAG_SLUG}-${$}-$(date +%s)"
  PORT_OFFSET=$(scenario_environment_value PORT_OFFSET 43000)
  if [[ ! "${PORT_OFFSET}" =~ ^[0-9]+$ ]] || [[ "${PORT_OFFSET}" -gt 44375 ]]; then
    echo "${SCENARIO_ENV_PREFIX}_PORT_OFFSET must be an unsigned integer no greater than 44375" >&2
    return 1
  fi
  PD_PORT=$((2379 + PORT_OFFSET))
  GO_SQL_PORT=$((4000 + PORT_OFFSET))
  TIKV_SEED_PORT=$((20160 + PORT_OFFSET))
  GO_STATUS_PORT=$((10080 + PORT_OFFSET))
  RUST_SQL_PORT=$((12000 + PORT_OFFSET))
  PD_ADDR="127.0.0.1:${PD_PORT}"
  GO_SQL_ADDR="127.0.0.1:${GO_SQL_PORT}"
  RUST_SQL_ADDR="127.0.0.1:${RUST_SQL_PORT}"
  TAG_DIR="${TIUP_HOME:-${HOME}/.tiup}/data/${TAG}"
  PLAYGROUND_LOG="${TMPDIR:-/tmp}/${TAG}-playground.log"
  RUST_LOG="${TMPDIR:-/tmp}/${TAG}-rust.log"
  MYSQL_LOG="${TMPDIR:-/tmp}/${TAG}-mysql.log"
  RESTART_LOG="${TMPDIR:-/tmp}/${TAG}-tikv-restart.log"
  RUNTIME_DIR=
  AUTH_FILE=
  AUTH_USER=${SCENARIO_AUTH_USER}
  AUTH_PASSWORD=$(scenario_environment_value AUTH_PASSWORD "${SCENARIO_AUTH_DEFAULT_PASSWORD}")
  PLAYGROUND_PID=
  RUST_PID=
  RESTART_PID=
  STOPPED_PIDS=()
  OWNED_PIDS=
  PD_ENDPOINTS=
  PD_PEER_ENDPOINTS=
  STORE_ADDRESSES=
  STORE_STATUS_ADDRESSES=
  CLIENT_PIDS=()
  PERSISTENT_CLIENT_PID=
  PERSISTENT_CLIENT_FIFO=
  PERSISTENT_CLIENT_OUTPUT=
  PERSISTENT_CLIENT_ERROR=
  PERSISTENT_CLIENT_FD_OPEN=false
  PERSISTENT_CONNECTION_ID=
  PERSISTENT_SESSION_ID=
  PREWRITE_FAILPOINT_ENABLED=false
  PROCESS_STOP_TIMEOUT=$(scenario_environment_value PROCESS_STOP_TIMEOUT 30)
  PHASE_TIMEOUT=$(scenario_environment_value PHASE_TIMEOUT 120)
  local timeout_name
  for timeout_name in PROCESS_STOP_TIMEOUT PHASE_TIMEOUT; do
    local timeout_value=${!timeout_name}
    if [[ ! "${timeout_value}" =~ ^[1-9][0-9]*$ ]]; then
      echo "${timeout_name} must be a positive integer" >&2
      return 1
    fi
  done
  CONNECTION_TIMEOUT_MS=$((PHASE_TIMEOUT * 12 * 1000))
  SCENARIO_RUST_SERVER=$(scenario_environment_value RUST_SERVER)
}

run_live_sql_node_topology_scenario() {
  initialize_live_sql_node_scenario || exit 1
  local hook
  for hook in \
    scenario_pre_transfer_discovery \
    scenario_pre_transfer_verified \
    scenario_transferred_to_b \
    scenario_failed_over_to_c \
    scenario_returned_to_b \
    scenario_validate_block_snapshot \
    scenario_emit_success_receipt; do
    if ! declare -F "${hook}" >/dev/null; then
      echo "${CAMPAIGN_LABEL} scenario is missing required hook ${hook}" >&2
      exit 1
    fi
  done
  if [[ ! "${SCENARIO_DATABASE}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "${CAMPAIGN_LABEL} scenario database is not a safe SQL identifier" >&2
    exit 1
  fi
  cd "${RUST_ROOT}"
  if [[ -z "${SCENARIO_RUST_SERVER}" ]]; then
    CARGO_BUILD_JOBS=12 cargo build -j12 -p tidb-server --bin tidb-server
    RUST_SERVER="${RUST_ROOT}/target/debug/tidb-server"
  else
    RUST_SERVER=${SCENARIO_RUST_SERVER}
  fi
  if [[ ! -x "${RUST_SERVER}" ]]; then
    echo "${CAMPAIGN_LABEL} Rust server is not executable: ${RUST_SERVER}" >&2
    exit 1
  fi

  for port in "${PD_PORT}" "${GO_SQL_PORT}" "${TIKV_SEED_PORT}" \
    "${GO_STATUS_PORT}" "${RUST_SQL_PORT}"; do
    if nc -z -w 1 127.0.0.1 "${port}" >/dev/null 2>&1; then
      echo "refusing occupied ${CAMPAIGN_LABEL} port ${port}; set ${SCENARIO_ENV_PREFIX}_PORT_OFFSET" >&2
      exit 1
    fi
  done

  trap cleanup EXIT
  trap handle_interrupt INT
  trap handle_terminate TERM
  RUNTIME_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TAG}-runtime.XXXXXX")
  AUTH_FILE="${RUNTIME_DIR}/auth.tsv"
  AUTH_HASH_HEX=$(printf '%s' "${AUTH_PASSWORD}" \
    | openssl dgst -sha1 -binary \
    | openssl dgst -sha1 -hex \
    | awk '{ print toupper($NF) }')
  if [[ ! "${AUTH_HASH_HEX}" =~ ^[0-9A-F]{40}$ ]]; then
    echo "could not derive ${CAMPAIGN_LABEL} native-password stage-two hash" >&2
    exit 1
  fi
  (umask 077; printf '%s\t%s\t%s\t*%s\n' \
    "${AUTH_USER}" "127.0.0.1" "mysql_native_password" "${AUTH_HASH_HEX}" >"${AUTH_FILE}")
  chmod 0600 "${AUTH_FILE}"
  unset AUTH_HASH_HEX

  export GO_FAILPOINTS='github.com/pingcap/tidb/pkg/server/enableTestAPI=return'
  tiup playground v8.5.6 --without-monitor --tag "${TAG}" \
    --db 1 --pd 3 --kv 3 --tiflash 0 --port-offset "${PORT_OFFSET}" \
    --db.binpath "${GO_TIDB_SERVER}" \
    >"${PLAYGROUND_LOG}" 2>&1 &
  PLAYGROUND_PID=$!
  unset GO_FAILPOINTS

  ready=false
  PD_MEMBERS_JSON=
  for _ in $(seq 1 240); do
    if ! kill -0 "${PLAYGROUND_PID}" 2>/dev/null; then
      echo "TiUP playground exited before readiness" >&2
      tail -160 "${PLAYGROUND_LOG}" >&2
      exit 1
    fi
    PD_MEMBERS_JSON=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/members" 2>/dev/null) || true
    PD_COUNT=$(printf '%s\n' "${PD_MEMBERS_JSON}" | jq -r '.members | length' 2>/dev/null) || true
    STORES_JSON=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" 2>/dev/null) || true
    TIKV_COUNT=$(printf '%s\n' "${STORES_JSON}" | jq -r \
      '[.stores[] | select(.store.state_name == "Up" and ((.store.node_state_name // "Serving") == "Serving"))] | length' \
      2>/dev/null) || true
    if [[ "${PD_COUNT:-0}" == 3 ]] && [[ "${TIKV_COUNT:-0}" == 3 ]] \
      && mysql_go -Nse 'select 1' >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "${ready}" != true ]]; then
    echo "three PD, three TiKV, and Go TiDB did not become ready" >&2
    tail -160 "${PLAYGROUND_LOG}" >&2
    exit 1
  fi
  ACTIVE_GO_FAILPOINTS=$(curl -sf --max-time 2 \
    "http://127.0.0.1:${GO_STATUS_PORT}/fail/" || true)
  if [[ "${ACTIVE_GO_FAILPOINTS}" != *'github.com/pingcap/tidb/pkg/server/enableTestAPI=return'* ]]; then
    echo "Go TiDB did not activate the test API required for the prewrite barrier" >&2
    exit 1
  fi
  PD_ENDPOINTS=$(printf '%s\n' "${PD_MEMBERS_JSON}" \
    | jq -r '.members[].client_urls[]' | sort -u)
  PD_PEER_ENDPOINTS=$(printf '%s\n' "${PD_MEMBERS_JSON}" \
    | jq -r '.members[].peer_urls[]' | sort -u)
  STORE_ADDRESSES=$(printf '%s\n' "${STORES_JSON}" | jq -r \
    '.stores[] | select(.store.state_name == "Up") | .store.address' | sort -u)
  STORE_STATUS_ADDRESSES=$(printf '%s\n' "${STORES_JSON}" | jq -r \
    '.stores[] | select(.store.state_name == "Up") | .store.status_address' | sort -u)
  OWNED_PIDS=$(merge_owned_pids)
  if [[ -z "${OWNED_PIDS}" ]]; then
    echo "TiUP did not publish tag-owned processes for ${TAG}" >&2
    exit 1
  fi
  PD_CLUSTER_ID=$(printf '%s\n' "${PD_MEMBERS_JSON}" \
    | jq -r '.header.cluster_id // .cluster_id // .id // empty')
  if [[ ! "${PD_CLUSTER_ID}" =~ ^[0-9]+$ ]] || [[ "${PD_CLUSTER_ID}" =~ ^0+$ ]]; then
    echo "PD membership omitted a nonzero cluster identity" >&2
    exit 1
  fi
  if [[ $(printf '%s\n' "${PD_ENDPOINTS}" | sed '/^$/d' | awk 'END { print NR + 0 }') != 3 \
    || $(printf '%s\n' "${PD_PEER_ENDPOINTS}" | sed '/^$/d' | awk 'END { print NR + 0 }') != 3 \
    || $(printf '%s\n' "${STORE_ADDRESSES}" | sed '/^$/d' | awk 'END { print NR + 0 }') != 3 \
    || $(printf '%s\n' "${STORE_STATUS_ADDRESSES}" | sed '/^$/d' | awk 'END { print NR + 0 }') != 3 ]]; then
    echo "${CAMPAIGN_LABEL} topology did not expose exactly three unique PD client/peer and TiKV service/status endpoints" >&2
    exit 1
  fi
  for endpoint in ${PD_ENDPOINTS}; do
    MEMBER_VIEW=$(curl -sf --max-time 2 "${endpoint}/pd/api/v1/members") || {
      echo "PD client endpoint ${endpoint} was not live" >&2
      exit 1
    }
    MEMBER_CLUSTER_ID=$(printf '%s\n' "${MEMBER_VIEW}" \
      | jq -r '.header.cluster_id // .cluster_id // .id // empty')
    MEMBER_CLIENT_ENDPOINTS=$(printf '%s\n' "${MEMBER_VIEW}" \
      | jq -r '.members[].client_urls[]' | sort -u)
    if [[ "${MEMBER_CLUSTER_ID}" != "${PD_CLUSTER_ID}" \
      || "${MEMBER_CLIENT_ENDPOINTS}" != "${PD_ENDPOINTS}" ]]; then
      echo "PD client endpoint ${endpoint} did not report the same three-member cluster" >&2
      exit 1
    fi
  done

  tiup ctl:v8.5.6 pd -u "http://${PD_ADDR}" \
    config set leader-schedule-limit 0 >/dev/null
  if [[ $(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/config/schedule" \
    | jq -r '."leader-schedule-limit" // -1') != 0 ]]; then
    echo "failed to disable background leader scheduling" >&2
    exit 1
  fi

  mysql_go <<SQL
  DROP DATABASE IF EXISTS ${SCENARIO_DATABASE};
  CREATE DATABASE ${SCENARIO_DATABASE};
  CREATE TABLE ${SCENARIO_DATABASE}.rows (
    id BIGINT PRIMARY KEY CLUSTERED,
    balance BIGINT NOT NULL
  );
  INSERT INTO ${SCENARIO_DATABASE}.rows VALUES (-7, 913), (0, -2048), (42, 77);
  CREATE TABLE ${SCENARIO_DATABASE}.lock_secondary (
    id BIGINT PRIMARY KEY CLUSTERED,
    value BIGINT NOT NULL
  );
  INSERT INTO ${SCENARIO_DATABASE}.lock_secondary VALUES (1, 1);
  SET SESSION tidb_wait_split_region_finish = 1;
  SPLIT TABLE ${SCENARIO_DATABASE}.lock_secondary BY (1);
SQL
  TABLE_ID=$(mysql_go -Nse \
    "select tidb_table_id from information_schema.tables where table_schema='${SCENARIO_DATABASE}' and table_name='rows'")
  if [[ ! "${TABLE_ID}" =~ ^[0-9]+$ ]] || [[ "${TABLE_ID}" =~ ^0+$ ]]; then
    echo "Go TiDB did not resolve the physical table ID" >&2
    exit 1
  fi
  LOCK_SECONDARY_TABLE_ID=$(mysql_go -Nse \
    "select tidb_table_id from information_schema.tables where table_schema='${SCENARIO_DATABASE}' and table_name='lock_secondary'")
  if [[ ! "${LOCK_SECONDARY_TABLE_ID}" =~ ^[0-9]+$ ]] \
    || [[ "${LOCK_SECONDARY_TABLE_ID}" =~ ^0+$ ]] \
    || [[ "${LOCK_SECONDARY_TABLE_ID}" -le "${TABLE_ID}" ]]; then
    echo "Go TiDB did not preserve rows-before-lock_secondary physical table ordering" >&2
    exit 1
  fi

  "${RUST_SERVER}" --path "${PD_ADDR}" --store tikv \
    --host 127.0.0.1 --port "${RUST_SQL_PORT}" \
    --database "${SCENARIO_DATABASE}" --table rows --table-id "${TABLE_ID}" \
    --column id:1:clustered-pk \
    --column balance:2:stored-not-null \
    --auth-file "${AUTH_FILE}" --max-connections 4 \
    --connection-timeout-ms "${CONNECTION_TIMEOUT_MS}" >"${RUST_LOG}" 2>&1 &
  RUST_PID=$!
  ORIGINAL_RUST_PID=${RUST_PID}

  READY_JSON=
  for _ in $(seq 1 600); do
    if ! pid_is_running "${RUST_PID}"; then
      echo "Rust SQL node exited before readiness" >&2
      tail -200 "${RUST_LOG}" >&2
      exit 1
    fi
    READY_JSON=$(grep -F '"event":"sql_node_ready"' "${RUST_LOG}" | tail -1 || true)
    if [[ -n "${READY_JSON}" ]] \
      && printf '%s\n' "${READY_JSON}" \
        | jq -e '.event == "sql_node_ready" and has("shutdown_grace_ms")' \
          >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
  if [[ -z "${READY_JSON}" ]] || ! printf '%s\n' "${READY_JSON}" | jq -e \
    --arg table_id "${TABLE_ID}" --arg cluster_id "${PD_CLUSTER_ID}" \
    --arg database "${SCENARIO_DATABASE}" \
    '(.table_id | tostring) == $table_id and (.cluster_id | tostring) == $cluster_id
     and .database == $database and .table == "rows"
     and .columns == ["id:1:clustered-pk", "balance:2:stored-not-null"]
     and .max_connections == 4 and .account_count == 1
     and .authority_id > 0 and .read_authority_id > 0' >/dev/null; then
    echo "Rust readiness omitted ${CAMPAIGN_LABEL} process identity" >&2
    printf '%s\n' "${READY_JSON}" >&2
    exit 1
  fi
  AUTHORITY_ID=$(printf '%s\n' "${READY_JSON}" | jq -r '.authority_id')
  READ_AUTHORITY_ID=$(printf '%s\n' "${READY_JSON}" | jq -r '.read_authority_id')
  SHUTDOWN_GRACE_MS=$(printf '%s\n' "${READY_JSON}" | jq -r '.shutdown_grace_ms')
  if [[ ! "${SHUTDOWN_GRACE_MS}" =~ ^[0-9]+$ ]]; then
    echo "Rust readiness omitted numeric shutdown_grace_ms" >&2
    exit 1
  fi

  REGION_ID=
  PHASE_PUBLICATION=
  PERSISTENT_CLIENT_FIFO="${RUNTIME_DIR}/persistent-rust-client.fifo"
  PERSISTENT_CLIENT_OUTPUT="${RUNTIME_DIR}/persistent-rust-client.out"
  PERSISTENT_CLIENT_ERROR="${RUNTIME_DIR}/persistent-rust-client.err"
  mkfifo "${PERSISTENT_CLIENT_FIFO}"
  exec 9<>"${PERSISTENT_CLIENT_FIFO}"
  PERSISTENT_CLIENT_FD_OPEN=true
  (
    exec 9>&-
    export MYSQL_PWD="${AUTH_PASSWORD}"
    export MARIADB_PWD="${AUTH_PASSWORD}"
    exec "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${RUST_SQL_PORT}" \
      -u"${AUTH_USER}" --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" \
      --unbuffered -B <"${PERSISTENT_CLIENT_FIFO}" \
      >"${PERSISTENT_CLIENT_OUTPUT}" 2>"${PERSISTENT_CLIENT_ERROR}"
  ) &
  PERSISTENT_CLIENT_PID=$!
  CLIENT_PIDS=("${PERSISTENT_CLIENT_PID}")
  PERSISTENT_ADMITTED=false
  for _ in $(seq 1 300); do
    CONNECTION_BEGIN=$(grep -F '"event":"connection_begin"' "${RUST_LOG}" | tail -1 || true)
    if pid_is_running "${PERSISTENT_CLIENT_PID}" \
      && [[ -n "${CONNECTION_BEGIN}" ]] \
      && printf '%s\n' "${CONNECTION_BEGIN}" \
        | jq -e '.active == 1 and .accepted == 1' >/dev/null 2>&1; then
      PERSISTENT_ADMITTED=true
      break
    fi
    sleep 0.1
  done
  if [[ "${PERSISTENT_ADMITTED}" != true ]]; then
    echo "${CAMPAIGN_LABEL} persistent authenticated stock client was not admitted" >&2
    sed -n '1,160p' "${PERSISTENT_CLIENT_ERROR}" >&2
    exit 1
  fi

  scenario_pre_transfer_discovery
  # The first request itself is the source of truth for the table region and
  # physical address. Validate both against PD rather than assuming store order.
  INITIAL_PUBLICATION=${PHASE_PUBLICATION}
  if [[ -z "${INITIAL_PUBLICATION}" ]]; then
    echo "initial query did not publish physical BatchCommands evidence" >&2
    tail -220 "${RUST_LOG}" >&2
    exit 1
  fi
  REGION_ID=$(printf '%s\n' "${INITIAL_PUBLICATION}" | jq -r '.region_id // 0')
  INITIAL_ADDRESS=$(printf '%s\n' "${INITIAL_PUBLICATION}" | jq -r '.physical_address // empty')
  if [[ ! "${REGION_ID}" =~ ^[0-9]+$ ]] || [[ "${REGION_ID}" =~ ^0+$ ]] \
    || [[ -z "${INITIAL_ADDRESS}" ]]; then
    echo "initial publication omitted table-region or physical identity" >&2
    exit 1
  fi
  REGION_REPLICATED=false
  for _ in $(seq 1 $((PHASE_TIMEOUT * 10))); do
    REGION=$(region_json 2>/dev/null) || true
    if printf '%s\n' "${REGION}" | jq -e \
      '[.peers[]? | select(.role_name == "Voter") | .store_id] as $voters
       | ($voters | length) == 3 and ($voters | unique | length) == 3
         and (.pending_peers | length) == 0 and (.down_peers | length) == 0' \
      >/dev/null 2>&1; then
      REGION_REPLICATED=true
      break
    fi
    sleep 0.1
  done
  if [[ "${REGION_REPLICATED}" != true ]]; then
    echo "table region ${REGION_ID} did not converge to three healthy voter peers" >&2
    printf '%s\n' "${REGION}" >&2
    exit 1
  fi
  STORE_A=$(printf '%s\n' "${REGION}" | jq -r '.leader.store_id // 0')
  ADDRESS_A=$(store_address "${STORE_A}")
  if [[ "${INITIAL_ADDRESS}" != "${ADDRESS_A}" ]]; then
    echo "initial physical publication ${INITIAL_ADDRESS} was not PD leader store A ${ADDRESS_A}" >&2
    exit 1
  fi
  # Revalidate the exact rows now that the discovered region is part of every
  # publication assertion.
  scenario_pre_transfer_verified "${ADDRESS_A}"

  PEER_STORES=$(printf '%s\n' "${REGION}" | jq -r --argjson a "${STORE_A}" \
    '.peers[] | select(.store_id != $a) | .store_id')
  STORE_B=$(printf '%s\n' "${PEER_STORES}" | head -1)
  if [[ ! "${STORE_B}" =~ ^[0-9]+$ ]] || [[ "${STORE_B}" =~ ^0+$ ]]; then
    echo "table region ${REGION_ID} did not expose a follower peer for store B" >&2
    printf '%s\n' "${REGION}" >&2
    exit 1
  fi
  ADDRESS_B=$(store_address "${STORE_B}")
  PEER_B=$(printf '%s\n' "${REGION}" | jq -r --argjson b "${STORE_B}" \
    '.peers[] | select(.store_id == $b) | .id')
  STORE_C_EXPECTED=$(printf '%s\n' "${REGION}" \
    | jq -r --argjson a "${STORE_A}" --argjson b "${STORE_B}" \
      '.peers[] | select(.store_id != $a and .store_id != $b and .role_name == "Voter") | .store_id')
  if [[ $(printf '%s\n' "${STORE_C_EXPECTED}" | sed '/^$/d' | awk 'END { print NR + 0 }') != 1 \
    || ! "${STORE_C_EXPECTED}" =~ ^[0-9]+$ || "${STORE_C_EXPECTED}" =~ ^0+$ ]]; then
    echo "table region ${REGION_ID} did not expose one distinct third voter store C" >&2
    printf '%s\n' "${REGION}" >&2
    exit 1
  fi
  PEER_C_EXPECTED=$(printf '%s\n' "${REGION}" | jq -r --argjson c "${STORE_C_EXPECTED}" \
    '.peers[] | select(.store_id == $c) | .id')
  ADDRESS_C_EXPECTED=$(store_address "${STORE_C_EXPECTED}")
  if [[ -z "${ADDRESS_C_EXPECTED}" || "${ADDRESS_C_EXPECTED}" == "${ADDRESS_A}" \
    || "${ADDRESS_C_EXPECTED}" == "${ADDRESS_B}" ]]; then
    echo "expected store C did not resolve to an address distinct from A and B" >&2
    exit 1
  fi

  transfer_leader "${STORE_B}"
  wait_for_leader_serving "${STORE_B}" "${ADDRESS_B}" "${PEER_B}" transferred_to_b_stable
  scenario_transferred_to_b "${ADDRESS_B}"
  B_VERSION_BEFORE=$(printf '%s\n' "${PHASE_PUBLICATION}" | jq -r '.physical_channel_version')
  B_GENERATION_BEFORE=$(printf '%s\n' "${PHASE_PUBLICATION}" | jq -r '.stream_generation')

  B_PORT=$(address_port "${ADDRESS_B}")
  B_PID=$(lsof -nP -iTCP:"${B_PORT}" -sTCP:LISTEN -t | head -1 || true)
  if [[ -z "${B_PID}" ]] \
    || ! ps -ww -p "${B_PID}" -o command= | grep -F "${TAG_DIR}" >/dev/null \
    || ! ps -ww -p "${B_PID}" -o command= | grep -F tikv-server >/dev/null; then
    echo "refusing to stop TiKV B not owned by ${TAG}: ${ADDRESS_B}" >&2
    exit 1
  fi
  TIKV_B_COMMAND=$(ps -ww -p "${B_PID}" -o command=)
  if [[ -z "${TIKV_B_COMMAND}" || "${TIKV_B_COMMAND}" == *$'\n'* \
    || "${TIKV_B_COMMAND}" != *"${TAG_DIR}"* \
    || "${TIKV_B_COMMAND}" != *"${B_PORT}"* \
    || "${TIKV_B_COMMAND}" =~ [^[:alnum:][:space:]/._:=,@%+-] ]]; then
    echo "cannot capture a deterministic same-address tag-owned TiKV B command" >&2
    exit 1
  fi
  kill -KILL "${B_PID}"
  for _ in $(seq 1 120); do
    if ! kill -0 "${B_PID}" 2>/dev/null \
      && ! nc -z -w 1 127.0.0.1 "${B_PORT}" >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
  if kill -0 "${B_PID}" 2>/dev/null \
    || nc -z -w 1 127.0.0.1 "${B_PORT}" >/dev/null 2>&1; then
    echo "tag-owned TiKV B ${ADDRESS_B} did not stop" >&2
    exit 1
  fi

  survivor_ready=false
  for _ in $(seq 1 $((PHASE_TIMEOUT * 2))); do
    REGION=$(region_json 2>/dev/null) || true
    SURVIVOR_LEADER=$(printf '%s\n' "${REGION}" | jq -r '.leader.store_id // 0' 2>/dev/null) || true
    if [[ "${SURVIVOR_LEADER}" =~ ^[0-9]+$ ]] && [[ ! "${SURVIVOR_LEADER}" =~ ^0+$ ]] \
      && [[ "${SURVIVOR_LEADER}" != "${STORE_B}" ]] \
      && printf '%s\n' "${REGION}" | jq -e \
        --argjson survivor "${SURVIVOR_LEADER}" \
        'any(.peers[]?; .store_id == $survivor and .role_name == "Voter")
         and all(.pending_peers[]?; .store_id != $survivor)
         and all(.down_peers[]?; .peer.store_id != $survivor)' >/dev/null 2>&1; then
      survivor_ready=true
      break
    fi
    sleep 0.5
  done
  if [[ "${survivor_ready}" != true ]]; then
    echo "region ${REGION_ID} did not elect any healthy surviving non-B leader" >&2
    printf '%s\n' "${REGION}" >&2
    exit 1
  fi

  C_STORE_STATE=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
    | jq -r --argjson c "${STORE_C_EXPECTED}" \
      '.stores[] | select(.store.id == $c) | [.store.state_name, (.store.node_state_name // "Serving"), .store.address] | @tsv')
  C_PORT=$(address_port "${ADDRESS_C_EXPECTED}")
  C_EXPECTED_HEALTHY=$(printf '%s\n' "${REGION}" | jq -r \
    --argjson c "${STORE_C_EXPECTED}" --argjson peer "${PEER_C_EXPECTED}" \
    'any(.peers[]?; .store_id == $c and .id == $peer and .role_name == "Voter")
     and all(.pending_peers[]?; .store_id != $c and .id != $peer)
     and all(.down_peers[]?; .peer.store_id != $c and .peer.id != $peer)')
  if [[ "${C_STORE_STATE}" != $'Up\tServing\t'"${ADDRESS_C_EXPECTED}" ]] \
    || ! nc -z -w 1 127.0.0.1 "${C_PORT}" >/dev/null 2>&1 \
    || [[ "${C_EXPECTED_HEALTHY}" != true ]]; then
    echo "expected distinct store C was not a live healthy voter after B death" >&2
    exit 1
  fi
  if [[ "${SURVIVOR_LEADER}" != "${STORE_C_EXPECTED}" ]]; then
    transfer_leader "${STORE_C_EXPECTED}"
  fi
  REGION=$(region_json)
  STORE_C=$(printf '%s\n' "${REGION}" | jq -r '.leader.store_id // 0')
  ADDRESS_C=$(store_address "${STORE_C}")
  C_FINAL_HEALTHY=$(printf '%s\n' "${REGION}" | jq -r \
    --argjson c "${STORE_C_EXPECTED}" --argjson peer "${PEER_C_EXPECTED}" \
    '.leader.store_id == $c
     and any(.peers[]?; .store_id == $c and .id == $peer and .role_name == "Voter")
     and all(.pending_peers[]?; .store_id != $c and .id != $peer)
     and all(.down_peers[]?; .peer.store_id != $c and .peer.id != $peer)')
  if [[ "${STORE_C}" != "${STORE_C_EXPECTED}" \
    || "${ADDRESS_C}" != "${ADDRESS_C_EXPECTED}" \
    || "${ADDRESS_C}" == "${ADDRESS_A}" || "${ADDRESS_C}" == "${ADDRESS_B}" \
    || "${C_FINAL_HEALTHY}" != true ]]; then
    echo "region ${REGION_ID} did not establish the exact distinct A->B->C topology" >&2
    exit 1
  fi
  wait_for_leader_serving "${STORE_C}" "${ADDRESS_C}" "${PEER_C_EXPECTED}" failed_over_to_c_stable
  scenario_failed_over_to_c "${ADDRESS_C}"

  /bin/sh -c "exec ${TIKV_B_COMMAND}" >>"${RESTART_LOG}" 2>&1 &
  RESTART_PID=$!
  restart_ready=false
  for _ in $(seq 1 $((PHASE_TIMEOUT * 2))); do
    if ! pid_is_running "${RESTART_PID}"; then
      echo "same-address TiKV B restart exited before readiness" >&2
      tail -160 "${RESTART_LOG}" >&2
      exit 1
    fi
    STORE_STATE=$(curl -sf --max-time 2 "http://${PD_ADDR}/pd/api/v1/stores" \
      | jq -r --argjson id "${STORE_B}" \
        '.stores[] | select(.store.id == $id) | [.store.state_name, (.store.node_state_name // "Serving"), .store.address] | @tsv' \
        2>/dev/null) || true
    REGION_STATE=$(region_json | jq -r --argjson b "${STORE_B}" --argjson peer "${PEER_B}" \
      '[(any(.peers[]?; .store_id == $b and .id == $peer)),
        (any(.pending_peers[]?; .store_id == $b or .id == $peer)),
        (any(.down_peers[]?; .peer.store_id == $b or .peer.id == $peer))] | @tsv' \
      2>/dev/null) || true
    LISTENER_PID=$(lsof -nP -iTCP:"${B_PORT}" -sTCP:LISTEN -t | head -1 || true)
    if nc -z -w 1 127.0.0.1 "${B_PORT}" >/dev/null 2>&1 \
      && [[ "${STORE_STATE}" == $'Up\tServing\t'"${ADDRESS_B}" ]] \
      && [[ "${REGION_STATE}" == $'true\tfalse\tfalse' ]] \
      && [[ "${LISTENER_PID}" == "${RESTART_PID}" ]]; then
      restart_ready=true
      break
    fi
    sleep 0.5
  done
  if [[ "${restart_ready}" != true ]]; then
    echo "same-address TiKV B ${ADDRESS_B} did not return as its exact ready peer" >&2
    tail -160 "${RESTART_LOG}" >&2
    exit 1
  fi
  if ! ps -ww -p "${RESTART_PID}" -o command= | grep -F "${TAG_DIR}" >/dev/null; then
    echo "restarted TiKV B is not tag-owned" >&2
    exit 1
  fi

  transfer_leader "${STORE_B}"
  wait_for_leader_serving "${STORE_B}" "${ADDRESS_B}" "${PEER_B}" returned_to_b_stable
  scenario_returned_to_b "${ADDRESS_B}"
  B_VERSION_AFTER=$(printf '%s\n' "${PHASE_PUBLICATION}" | jq -r '.physical_channel_version')
  B_GENERATION_AFTER=$(printf '%s\n' "${PHASE_PUBLICATION}" | jq -r '.stream_generation')
  if [[ "${B_VERSION_AFTER}" != "${B_VERSION_BEFORE}" \
    || "${B_GENERATION_AFTER}" -le "${B_GENERATION_BEFORE}" ]]; then
    echo "same-address TiKV B restart unexpectedly retired its physical channel or failed to advance only the BatchCommands stream: versions ${B_VERSION_BEFORE}->${B_VERSION_AFTER}, generations ${B_GENERATION_BEFORE}->${B_GENERATION_AFTER}" >&2
    exit 1
  fi

  # Leave the rows mutation as an exact prewritten secondary whose committed
  # primary lives in the separately split helper region. Freezing both non-B
  # stores later removes that helper region's quorum while the main region still
  # dispatches through B; this makes lock resolution deterministically in-flight.
  LOCK_SECONDARY_KEY=$(mysql_go -Nse \
    "SELECT tidb_encode_record_key('${SCENARIO_DATABASE}', 'lock_secondary', 1);")
  if [[ ! "${LOCK_SECONDARY_KEY}" =~ ^[0-9A-Fa-f]+$ ]]; then
    echo "Go TiDB did not encode the helper record key for PD lookup" >&2
    exit 1
  fi
  LOCK_REGION=$(curl -sf --max-time 2 \
    "http://${PD_ADDR}/pd/api/v1/region/key/${LOCK_SECONDARY_KEY}")
  LOCK_REGION_ID=$(printf '%s\n' "${LOCK_REGION}" | jq -r '.id // 0')
  if [[ ! "${LOCK_REGION_ID}" =~ ^[0-9]+$ ]] \
    || [[ "${LOCK_REGION_ID}" =~ ^0+$ ]] || [[ "${LOCK_REGION_ID}" == "${REGION_ID}" ]] \
    || ! printf '%s\n' "${LOCK_REGION}" | jq -e \
      '[.peers[]? | select(.role_name == "Voter") | .store_id] as $voters
       | ($voters | length) == 3 and ($voters | unique | length) == 3
         and (.pending_peers | length) == 0 and (.down_peers | length) == 0' \
      >/dev/null; then
    echo "helper record did not resolve to a distinct healthy three-voter region" >&2
    printf '%s\n' "${LOCK_REGION}" >&2
    exit 1
  fi
  transfer_leader "${STORE_A}" "${LOCK_REGION_ID}"

  PREWRITE_FAILPOINT_TERM='return("skip")'
  if ! curl -sf --max-time 2 -X PUT --data "${PREWRITE_FAILPOINT_TERM}" \
    "http://127.0.0.1:${GO_STATUS_PORT}/fail/tikvclient/beforeCommitSecondaries" \
    >/dev/null; then
    echo "failpoint-enabled Go TiDB did not enable the secondary-commit barrier" >&2
    exit 1
  fi
  PREWRITE_FAILPOINT_ENABLED=true
  if [[ $(curl -sf --max-time 2 \
    "http://127.0.0.1:${GO_STATUS_PORT}/fail/tikvclient/beforeCommitSecondaries") \
    != "${PREWRITE_FAILPOINT_TERM}" ]]; then
    echo "Go TiDB did not report the exact secondary-commit barrier" >&2
    exit 1
  fi

  LOCK_MARKER="${TAG}-primary-prewrite-ready"
  LOCK_OUTPUT="${RUNTIME_DIR}/primary-prewrite.out"
  LOCK_ERROR="${RUNTIME_DIR}/primary-prewrite.err"
  (
    export MYSQL_PWD=
    export MARIADB_PWD=
    exec "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
      -uroot --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" --unbuffered -Nse \
      "SET SESSION tidb_enable_async_commit = 0; SET SESSION tidb_enable_1pc = 0; BEGIN PESSIMISTIC; UPDATE ${SCENARIO_DATABASE}.lock_secondary SET value = value + 1 WHERE id = 1; UPDATE ${SCENARIO_DATABASE}.rows SET balance = balance + 1 WHERE id = -7; SELECT '${LOCK_MARKER}'; COMMIT;"
  ) >"${LOCK_OUTPUT}" 2>"${LOCK_ERROR}" &
  LOCK_HOLDER_PID=$!
  CLIENT_PIDS=("${PERSISTENT_CLIENT_PID}" "${LOCK_HOLDER_PID}")
  LOCK_READY=false
  for _ in $(seq 1 $((PHASE_TIMEOUT * 10))); do
    if grep -Fx "${LOCK_MARKER}" "${LOCK_OUTPUT}" >/dev/null 2>&1; then
      LOCK_READY=true
      break
    fi
    if ! pid_is_running "${LOCK_HOLDER_PID}"; then
      break
    fi
    sleep 0.1
  done
  if [[ "${LOCK_READY}" != true ]]; then
    echo "stock Go-MySQL prewrite holder did not reach its readiness marker" >&2
    sed -n '1,160p' "${LOCK_ERROR}" >&2
    exit 1
  fi

  LOCK_HOLDER_DEADLINE=$(( $(date +%s) + PROCESS_STOP_TIMEOUT ))
  if ! wait_for_pids_until "${LOCK_HOLDER_DEADLINE}" "${LOCK_HOLDER_PID}"; then
    echo "prewrite holder did not return after committing its helper primary" >&2
    exit 1
  fi
  set +e
  wait "${LOCK_HOLDER_PID}"
  LOCK_HOLDER_STATUS=$?
  set -e
  if [[ "${LOCK_HOLDER_STATUS}" -ne 0 ]]; then
    echo "prewrite holder returned ${LOCK_HOLDER_STATUS}" >&2
    sed -n '1,160p' "${LOCK_ERROR}" >&2
    exit 1
  fi
  LOCK_HOLDER_PID=
  CLIENT_PIDS=("${PERSISTENT_CLIENT_PID}")

  SECONDARY_LOCK_READY=false
  for _ in $(seq 1 100); do
    MAIN_MVCC=$(mysql_go -Nse \
      "SELECT tidb_mvcc_info(tidb_encode_record_key('${SCENARIO_DATABASE}', 'rows', -7));")
    HELPER_MVCC=$(mysql_go -Nse \
      "SELECT tidb_mvcc_info(tidb_encode_record_key('${SCENARIO_DATABASE}', 'lock_secondary', 1));")
    if printf '%s\n' "${MAIN_MVCC}" | jq -e \
      '.[0].mvcc.info.lock.start_ts > 0
       and ((.[0].mvcc.info.lock.type // 0) == 0)' >/dev/null 2>&1 \
      && printf '%s\n' "${HELPER_MVCC}" | jq -e \
        '.[0].mvcc.info | has("lock") | not' >/dev/null 2>&1; then
      SECONDARY_LOCK_READY=true
      break
    fi
    sleep 0.1
  done
  if [[ "${SECONDARY_LOCK_READY}" != true ]]; then
    echo "fixture did not leave rows as a prewritten secondary of a committed helper primary" >&2
    printf '%s\n%s\n' "${MAIN_MVCC}" "${HELPER_MVCC}" >&2
    exit 1
  fi

  for FREEZE_ADDRESS in "${ADDRESS_A}" "${ADDRESS_C}"; do
    FREEZE_PORT=$(address_port "${FREEZE_ADDRESS}")
    FREEZE_PID=$(lsof -nP -iTCP:"${FREEZE_PORT}" -sTCP:LISTEN -t | head -1 || true)
    if [[ -z "${FREEZE_PID}" || "${FREEZE_PID}" == "${RESTART_PID}" ]] \
      || ! ps -ww -p "${FREEZE_PID}" -o command= | grep -F "${TAG_DIR}" >/dev/null \
      || ! ps -ww -p "${FREEZE_PID}" -o command= | grep -F tikv-server >/dev/null; then
      echo "refusing to freeze non-B TiKV not owned by ${TAG}: ${FREEZE_ADDRESS}" >&2
      exit 1
    fi
    kill -STOP "${FREEZE_PID}"
    STOPPED_PIDS+=("${FREEZE_PID}")
  done
  sleep 0.5

  LOCK_BARRIER_READY=false
  LOCK_PROBE_PID=
  for probe_attempt in $(seq 1 10); do
    LOCK_PROBE_OUTPUT="${RUNTIME_DIR}/primary-lock-probe-${probe_attempt}.out"
    LOCK_PROBE_ERROR="${RUNTIME_DIR}/primary-lock-probe-${probe_attempt}.err"
    (
      export MYSQL_PWD=
      export MARIADB_PWD=
      exec "${MYSQL_CLIENT}" --protocol=tcp -h 127.0.0.1 -P "${GO_SQL_PORT}" \
        -uroot --connect-timeout=5 "${MYSQL_PLUGIN_ARGS[@]}" -Nse \
        "SELECT balance FROM ${SCENARIO_DATABASE}.rows WHERE id = -7;"
    ) >"${LOCK_PROBE_OUTPUT}" 2>"${LOCK_PROBE_ERROR}" &
    LOCK_PROBE_PID=$!
    CLIENT_PIDS=("${PERSISTENT_CLIENT_PID}" "${LOCK_PROBE_PID}")
    sleep 2
    if pid_is_running "${LOCK_PROBE_PID}" \
      && [[ ! -s "${LOCK_PROBE_OUTPUT}" ]] && [[ ! -s "${LOCK_PROBE_ERROR}" ]]; then
      LOCK_BARRIER_READY=true
      break
    fi
    wait "${LOCK_PROBE_PID}" 2>/dev/null || true
    LOCK_PROBE_PID=
    CLIENT_PIDS=("${PERSISTENT_CLIENT_PID}")
  done
  if [[ "${LOCK_BARRIER_READY}" != true ]]; then
    echo "stock Go-MySQL probe did not block on the real ${SCENARIO_DATABASE}.rows primary prewrite" >&2
    sed -n '1,160p' "${LOCK_PROBE_OUTPUT}" >&2
    sed -n '1,160p' "${LOCK_PROBE_ERROR}" >&2
    mysql_go -Nse \
      "SELECT tidb_mvcc_info(tidb_encode_record_key('${SCENARIO_DATABASE}', 'rows', -7));" \
      >&2 || true
    mysql_go -Nse \
      "SELECT tidb_mvcc_info(tidb_encode_record_key('${SCENARIO_DATABASE}', 'lock_secondary', 1));" \
      >&2 || true
    exit 1
  fi
  if ! terminate_pid_group "primary-lock proof stock client" "${LOCK_PROBE_PID}"; then
    exit 1
  fi
  LOCK_PROBE_PID=
  CLIENT_PIDS=("${PERSISTENT_CLIENT_PID}")

  BLOCK_BEFORE_PUBLICATIONS=$(publication_count)
  BLOCK_BEFORE_TRANSPORTS=$(transport_count)
  BLOCK_BEFORE_SNAPSHOTS=$(snapshot_count)
  BLOCK_BEFORE_ACTIVITIES=$(grep -c -F '"event":"query_activity"' "${RUST_LOG}" 2>/dev/null || true)
  BLOCK_BEFORE_OUTPUT_LINES=$(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}")
  BLOCK_BEFORE_ERROR_LINES=$(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_ERROR}")
  printf '%s\n' "${SCENARIO_BLOCK_QUERY}" >&9
  if [[ "${SCENARIO_EXPECTS_QUERY_SNAPSHOT}" == true ]]; then
    wait_for_new_event_count query_snapshot "${BLOCK_BEFORE_SNAPSHOTS}" "${BLOCK_BEFORE_ERROR_LINES}"
  fi
  wait_for_new_event_count query_transport_published "${BLOCK_BEFORE_PUBLICATIONS}" "${BLOCK_BEFORE_ERROR_LINES}"
  BLOCK_PUBLICATION=$(grep -F '"event":"query_transport_published"' "${RUST_LOG}" \
    | tail -n +$((BLOCK_BEFORE_PUBLICATIONS + 1)) | head -1)
  if ! printf '%s\n' "${BLOCK_PUBLICATION}" | jq -e \
    --arg region "${REGION_ID}" --arg address "${ADDRESS_B}" \
    --arg connection "${PERSISTENT_CONNECTION_ID}" \
    --arg authority "${AUTHORITY_ID}" --arg session "${PERSISTENT_SESSION_ID}" \
    '(.region_id | tostring) == $region and .physical_address == $address
     and (.connection_id | tostring) == $connection
     and (.authority_id | tostring) == $authority
     and (.session_id | tostring) == $session
     and (.query_id | type) == "number" and .query_id > 0
     and .forwarded_host == null' >/dev/null; then
    echo "blocked query publication did not preserve the persistent B identity" >&2
    printf '%s\n' "${BLOCK_PUBLICATION}" >&2
    exit 1
  fi
  BLOCK_QUERY_ID=$(printf '%s\n' "${BLOCK_PUBLICATION}" | jq -r '.query_id')
  BLOCK_SNAPSHOT=
  if [[ "${SCENARIO_EXPECTS_QUERY_SNAPSHOT}" == true ]]; then
    BLOCK_SNAPSHOT=$(grep -F '"event":"query_snapshot"' "${RUST_LOG}" \
      | tail -n +$((BLOCK_BEFORE_SNAPSHOTS + 1)) \
      | jq -c --arg connection "${PERSISTENT_CONNECTION_ID}" --arg query "${BLOCK_QUERY_ID}" \
        'select((.connection_id | tostring) == $connection and (.query_id | tostring) == $query)' \
      | tail -1)
  fi
  if ! scenario_validate_block_snapshot "${BLOCK_SNAPSHOT}"; then
    echo "blocked query did not publish the scenario-owned immutable plan before waiting" >&2
    printf '%s\n' "${BLOCK_SNAPSHOT}" >&2
    exit 1
  fi
  BLOCK_ACTIVITY_READY=false
  BLOCK_ACTIVITY_DEADLINE=$(( $(date +%s) + PHASE_TIMEOUT ))
  while [[ $(date +%s) -lt "${BLOCK_ACTIVITY_DEADLINE}" ]]; do
    BLOCK_ACTIVITIES=$(grep -F '"event":"query_activity"' "${RUST_LOG}" \
      | tail -n +$((BLOCK_BEFORE_ACTIVITIES + 1)) | jq -s '.')
    if printf '%s\n' "${BLOCK_ACTIVITIES}" | jq -e \
      --arg connection "${PERSISTENT_CONNECTION_ID}" --arg query "${BLOCK_QUERY_ID}" \
      '[.[] | select((.connection_id | tostring) == $connection
        and (.query_id | tostring) == $query) | .phase] == ["begin"]' >/dev/null; then
      BLOCK_ACTIVITY_READY=true
      break
    fi
    sleep 0.05
  done
  if [[ "${BLOCK_ACTIVITY_READY}" != true ]]; then
    echo "blocked query did not publish its correlated activity begin" >&2
    exit 1
  fi
  kill -STOP "${RESTART_PID}"
  STOPPED_PIDS+=("${RESTART_PID}")
  sleep 0.5
  BLOCK_TRANSPORTS=$(grep -F '"event":"query_transport"' "${RUST_LOG}" \
    | tail -n +$((BLOCK_BEFORE_TRANSPORTS + 1)) | jq -s '.')
  BLOCK_ACTIVITIES=$(grep -F '"event":"query_activity"' "${RUST_LOG}" \
    | tail -n +$((BLOCK_BEFORE_ACTIVITIES + 1)) | jq -s '.')
  if ! pid_is_running "${PERSISTENT_CLIENT_PID}" \
    || [[ $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}") -ge $((BLOCK_BEFORE_OUTPUT_LINES + 2)) ]] \
    || ! printf '%s\n' "${BLOCK_TRANSPORTS}" | jq -e \
      --arg connection "${PERSISTENT_CONNECTION_ID}" --arg query "${BLOCK_QUERY_ID}" \
      'all(.[]; (.connection_id | tostring) != $connection or (.query_id | tostring) != $query)' >/dev/null \
    || ! printf '%s\n' "${BLOCK_ACTIVITIES}" | jq -e \
      --arg connection "${PERSISTENT_CONNECTION_ID}" --arg query "${BLOCK_QUERY_ID}" \
      '[.[] | select((.connection_id | tostring) == $connection
        and (.query_id | tostring) == $query) | .phase] == ["begin"]' >/dev/null; then
    echo "real B query was not still blocked without transport/end evidence before TERM" >&2
    exit 1
  fi

  SHUTDOWN_STARTED_MS=$(now_millis)
  kill -TERM "${RUST_PID}"
  SHUTDOWN_BOUND_MS=$((SHUTDOWN_GRACE_MS + 2000))
  SHUTDOWN_WAIT_SECONDS=$(((SHUTDOWN_BOUND_MS + 999) / 1000 + 1))
  RUST_DEADLINE=$(( $(date +%s) + SHUTDOWN_WAIT_SECONDS ))
  if ! wait_for_pids_until "${RUST_DEADLINE}" "${RUST_PID}"; then
    echo "Rust SQL node exceeded advertised shutdown grace plus 2000ms" >&2
    exit 1
  fi
  set +e
  wait "${RUST_PID}"
  RUST_STATUS=$?
  set -e
  RUST_PID=
  SHUTDOWN_ELAPSED_MS=$(( $(now_millis) - SHUTDOWN_STARTED_MS ))
  for stopped_pid in "${STOPPED_PIDS[@]}"; do
    kill -CONT "${stopped_pid}" 2>/dev/null || true
  done
  STOPPED_PIDS=()
  if [[ "${RUST_STATUS}" -ne 0 ]]; then
    echo "Rust SQL node returned ${RUST_STATUS} after controlled SIGTERM" >&2
    tail -240 "${RUST_LOG}" >&2
    exit 1
  fi
  if [[ "${SHUTDOWN_ELAPSED_MS}" -gt "${SHUTDOWN_BOUND_MS}" ]]; then
    echo "Rust shutdown took ${SHUTDOWN_ELAPSED_MS}ms, beyond ${SHUTDOWN_GRACE_MS}ms grace plus 2000ms" >&2
    exit 1
  fi

  CLIENT_DEADLINE=$(( $(date +%s) + PROCESS_STOP_TIMEOUT ))
  if ! wait_for_pids_until "${CLIENT_DEADLINE}" "${PERSISTENT_CLIENT_PID}"; then
    echo "blocked stock client did not observe controlled cancellation" >&2
    exit 1
  fi
  set +e
  wait "${PERSISTENT_CLIENT_PID}"
  BLOCK_CLIENT_STATUS=$?
  set -e
  if [[ "${PERSISTENT_CLIENT_FD_OPEN}" == true ]]; then
    exec 9>&-
    PERSISTENT_CLIENT_FD_OPEN=false
  fi
  CLIENT_PIDS=()
  if [[ "${BLOCK_CLIENT_STATUS}" -eq 0 \
    || $(awk 'END { print NR + 0 }' "${PERSISTENT_CLIENT_OUTPUT}") -ge $((BLOCK_BEFORE_OUTPUT_LINES + 2)) ]]; then
    echo "blocked stock client completed successfully instead of observing cancellation" >&2
    exit 1
  fi
  if ! curl -sf --max-time 2 -X DELETE \
    "http://127.0.0.1:${GO_STATUS_PORT}/fail/tikvclient/beforeCommitSecondaries" \
    >/dev/null; then
    echo "could not retire the secondary-commit barrier" >&2
    exit 1
  fi
  PREWRITE_FAILPOINT_ENABLED=false

  SHUTDOWN_EVENTS=$(grep -F '"event":"process_shutdown_stage"' "${RUST_LOG}" | jq -s '.')
  if ! printf '%s\n' "${SHUTDOWN_EVENTS}" | jq -e \
    'length == 4
     and [.[].stage] == ["connections", "region_cache", "tikv_transport", "pd"]
     and all(.[]; .outcome == "success")
     and .[0].active == 0
     and .[0].accepted == .[0].completed
     and .[0].failed == 0
     and .[0].forced_connections == 0' >/dev/null; then
    echo "${CAMPAIGN_LABEL} shutdown stages were not successful, ordered, and balanced" >&2
    printf '%s\n' "${SHUTDOWN_EVENTS}" >&2
    exit 1
  fi
  TERMINAL_EVENTS=$(grep -E '"event":"(process_shutdown_stage|sql_node_stopped)"' \
    "${RUST_LOG}" | jq -s '.')
  if ! printf '%s\n' "${TERMINAL_EVENTS}" | jq -e \
    'length == 5
     and [.[0:4][].stage] == ["connections", "region_cache", "tikv_transport", "pd"]
     and all(.[0:4][]; .event == "process_shutdown_stage" and .outcome == "success")
     and .[4].event == "sql_node_stopped" and .[4].outcome == "success"' >/dev/null \
    || grep -F '"event":"process_shutdown_rejected"' "${RUST_LOG}" >/dev/null \
    || grep -F '"outcome":"error"' "${RUST_LOG}" >/dev/null; then
    echo "Rust process did not publish exactly one clean terminal success after PD shutdown" >&2
    printf '%s\n' "${TERMINAL_EVENTS}" >&2
    tail -240 "${RUST_LOG}" >&2
    exit 1
  fi
  FINAL_CONNECTIONS=$(printf '%s\n' "${SHUTDOWN_EVENTS}" | jq -r \
    '.[0] | "accepted=\(.accepted);completed=\(.completed);failed=\(.failed);active=\(.active)"')

  if [[ "${ORIGINAL_RUST_PID}" == 0 ]]; then
    echo "invalid original Rust PID evidence" >&2
    exit 1
  fi
  scenario_emit_success_receipt

}
