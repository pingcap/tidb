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
    if [[ "${PLAYGROUND_CHILD_DETACHED}" != true \
      || "${command}" != *"${TAG}"* || -n "${descendants}" ]]; then
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
