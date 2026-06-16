#! /usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail
export NEXT_GEN=1

starter_data_dir=""
starter_tidb_pid=""

function find_available_port() {
    local port="$1"
    while [[ "${port}" -lt 65536 ]]; do
        if ! lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
            echo "${port}"
            return 0
        fi
        port=$((port + 1))
    done
    echo "no available port found from $1" >&2
    return 1
}

function cleanup_tidb_server() {
    if [[ -n "${starter_tidb_pid:-}" ]]; then
        if kill -0 "${starter_tidb_pid}" >/dev/null 2>&1; then
            kill -TERM "${starter_tidb_pid}" >/dev/null 2>&1 || true
        fi
        wait "${starter_tidb_pid}" >/dev/null 2>&1 || true
        starter_tidb_pid=""
    fi
    if [[ -n "${starter_data_dir:-}" && -d "${starter_data_dir}" ]]; then
        rm -rf -- "${starter_data_dir}"
        starter_data_dir=""
    fi
}

function wait_for_tidb_server() {
    local status_url="$1"
    local tidb_pid="$2"
    local log_file="$3"
    local stdout_log_file="$4"

    for _ in {1..60}; do
        if curl -sf "${status_url}/status" >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "${tidb_pid}" >/dev/null 2>&1; then
            echo "tidb-server exited before becoming ready. Log tail:" >&2
            tail -200 "${log_file}" >&2 || true
            tail -200 "${stdout_log_file}" >&2 || true
            return 1
        fi
        sleep 1
    done

    echo "timed out waiting for tidb-server. Log tail:" >&2
    tail -200 "${log_file}" >&2 || true
    tail -200 "${stdout_log_file}" >&2 || true
    return 1
}

function is_true() {
    local value
    value="$(printf "%s" "$1" | tr '[:upper:]' '[:lower:]')"
    case "${value}" in
        1|true|yes|on)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

function json_escape() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//$'\b'/\\b}"
    value="${value//$'\f'/\\f}"
    value="${value//$'\n'/\\n}"
    value="${value//$'\r'/\\r}"
    value="${value//$'\t'/\\t}"
    printf "%s" "${value}"
}

function is_system_keyspace() {
    local value
    value="$(printf "%s" "$1" | tr '[:lower:]' '[:upper:]')"
    [[ "${value}" == "SYSTEM" ]]
}

function create_starter_keyspace() {
    local pd_status_url="$1"
    local keyspace_name="$2"
    local keyspace_create_body="${STARTER_KEYSPACE_CREATE_BODY:-}"

    if [[ -z "${keyspace_create_body}" ]]; then
        keyspace_create_body="$(printf '{"name":"%s","config":{}}' "$(json_escape "${keyspace_name}")")"
    fi

    echo "Creating starter keyspace ${keyspace_name} via ${pd_status_url}/pd/api/v2/keyspaces"
    curl -fsS -X POST "${pd_status_url}/pd/api/v2/keyspaces" \
        -H "Content-Type: application/json" \
        -d "${keyspace_create_body}"
    echo
}

function prepare_starter_keyspace() {
    local keyspace_name="$1"
    local pd_status_url="$2"
    local tidb_server_bin="$3"
    local self_dir="$4"

    if is_system_keyspace "${keyspace_name}"; then
        return 0
    fi
    if ! is_true "${STARTER_PREPARE_KEYSPACE:-1}"; then
        return 0
    fi

    echo "Bootstrapping SYSTEM keyspace before creating starter keyspace ${keyspace_name}"
    TIDB_SERVER_BIN="${tidb_server_bin}" \
        STARTER_KEYSPACE_NAME=SYSTEM \
        STARTER_PREPARE_KEYSPACE=0 \
        STARTER_STANDBY_MODE=0 \
        STARTER_KEYSPACE_OBSERVABILITY=0 \
        STARTER_RUN_EXIT_WAIT_TEST=0 \
        "${self_dir}/run-starter-tests-with-server.sh" \
            --under-cluster startertest "${STARTER_SYSTEM_BOOTSTRAP_TIMEOUT:-2m}" -run "^$"

    create_starter_keyspace "${pd_status_url}" "${keyspace_name}"
}

function activate_starter_server() {
    local status_url="$1"
    local tidb_pid="$2"
    local log_file="$3"
    local stdout_log_file="$4"
    local response_file="$5"
    local keyspace_name="$6"
    local export_id="$7"
    local max_idle_seconds="$8"
    local metadata_json="${9:-}"
    local activate_err_file="${response_file}.err"
    local activate_rc_file="${response_file}.rc"
    local activate_body
    local escaped_keyspace_name
    local escaped_export_id

    escaped_keyspace_name="$(json_escape "${keyspace_name}")"
    escaped_export_id="$(json_escape "${export_id}")"

    if [[ -n "${metadata_json}" ]]; then
        activate_body="$(printf '{"keyspace_name":"%s","export_id":"%s","max_idle_seconds":%s,"metadata":%s,"tidb_enable_ddl":true,"run_auto_analyze":true}' \
            "${escaped_keyspace_name}" "${escaped_export_id}" "${max_idle_seconds}" "${metadata_json}")"
    else
        activate_body="$(printf '{"keyspace_name":"%s","export_id":"%s","max_idle_seconds":%s,"tidb_enable_ddl":true,"run_auto_analyze":true}' \
            "${escaped_keyspace_name}" "${escaped_export_id}" "${max_idle_seconds}")"
    fi

    echo "Activating external starter tidb-server for keyspace ${keyspace_name}, export ID ${export_id}"
    (
        set +e
        curl -fsS --max-time 120 \
            -X POST "${status_url}/tidb-pool/activate" \
            -H "Content-Type: application/json" \
            -d "${activate_body}" \
            > "${response_file}" 2> "${activate_err_file}"
        echo "$?" > "${activate_rc_file}"
    ) &
    local activate_pid="$!"

    for _ in {1..150}; do
        if [[ -f "${activate_rc_file}" ]]; then
            local activate_rc
            activate_rc="$(cat "${activate_rc_file}")"
            wait "${activate_pid}" >/dev/null 2>&1 || true
            if [[ "${activate_rc}" != "0" ]]; then
                echo "starter activation failed. curl stderr:" >&2
                cat "${activate_err_file}" >&2 || true
                echo "tidb-server log tail:" >&2
                tail -200 "${log_file}" >&2 || true
                tail -200 "${stdout_log_file}" >&2 || true
                return "${activate_rc}"
            fi
            echo "Starter activation response:"
            cat "${response_file}" || true
            return 0
        fi
        if ! kill -0 "${tidb_pid}" >/dev/null 2>&1; then
            echo "tidb-server exited before activation completed. Log tail:" >&2
            tail -200 "${log_file}" >&2 || true
            tail -200 "${stdout_log_file}" >&2 || true
            cat "${activate_err_file}" >&2 || true
            return 1
        fi
        sleep 1
    done

    echo "timed out waiting for starter activation. Log tail:" >&2
    kill -TERM "${activate_pid}" >/dev/null 2>&1 || true
    wait "${activate_pid}" >/dev/null 2>&1 || true
    tail -200 "${log_file}" >&2 || true
    tail -200 "${stdout_log_file}" >&2 || true
    cat "${activate_err_file}" >&2 || true
    return 1
}

function prepare_tidb_server_binary() {
    local tidb_server_bin="$1"

    if [[ -z "${TIDB_SERVER_BIN:-}" ]]; then
        echo "Building next-gen tidb-server from current checkout"
        make server
    fi

    if [[ ! -x "${tidb_server_bin}" ]]; then
        echo "tidb-server binary '${tidb_server_bin}' does not exist or is not executable." >&2
        echo "Build a next-gen TiDB server first, for example: make server" >&2
        return 1
    fi
}

function run_under_cluster() {
    local test_suite="${1:-startertest}"
    local suite_timeout="${2:-40m}"
    shift || true
    shift || true
    local extra_go_test_args=("$@")

    local self_dir
    self_dir="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
    local repo_root
    repo_root="$(realpath "${self_dir}/../../../..")"
    cd "${repo_root}"

    local tidb_server_bin="${TIDB_SERVER_BIN:-bin/tidb-server}"
    prepare_tidb_server_binary "${tidb_server_bin}"

    local max_allowed_packet="${STARTER_MAX_ALLOWED_PACKET:-65536}"
    local keyspace_name="${STARTER_KEYSPACE_NAME:-SYSTEM}"
    local standby_mode="${STARTER_STANDBY_MODE:-1}"
    local run_exit_wait_test="${STARTER_RUN_EXIT_WAIT_TEST:-}"
    local activate_export_id="${STARTER_ACTIVATE_EXPORT_ID:-starter-external-export}"
    local activate_max_idle_seconds="${STARTER_ACTIVATE_MAX_IDLE_SECONDS:-60}"
    local keyspace_observability="${STARTER_KEYSPACE_OBSERVABILITY:-}"
    if [[ -z "${keyspace_observability}" ]]; then
        keyspace_observability="${standby_mode}"
    fi
    local keyspace_meta_tenant="${STARTER_KEYSPACE_META_TENANT:-starter_tenant}"
    local keyspace_meta_project="${STARTER_KEYSPACE_META_PROJECT:-starter_project}"
    if is_true "${keyspace_observability}" && ! is_true "${standby_mode}"; then
        echo "STARTER_KEYSPACE_OBSERVABILITY requires STARTER_STANDBY_MODE=1 because keyspace metadata is supplied by standby activation." >&2
        return 1
    fi
    starter_data_dir="$(mktemp -d)"
    local config_file="${starter_data_dir}/starter.toml"
    local log_file="${starter_data_dir}/tidb.log"
    local stdout_log_file="${starter_data_dir}/tidb-stdout.log"
    local activate_response_file="${starter_data_dir}/activate-response.json"
    local tidb_port
    tidb_port="$(find_available_port "${STARTER_TIDB_PORT:-4000}")"
    local status_port
    status_port="$(find_available_port "${STARTER_TIDB_STATUS_PORT:-10080}")"
    if [[ "${status_port}" == "${tidb_port}" ]]; then
        status_port="$(find_available_port "$((status_port + 1))")"
    fi
    local pd_endpoints="${STARTER_PD_ENDPOINTS:-127.0.0.1:2379,127.0.0.1:2382,127.0.0.1:2384}"
    local first_pd_endpoint="${pd_endpoints%%,*}"
    local pd_status_url="${first_pd_endpoint}"
    if [[ "${pd_status_url}" != http://* && "${pd_status_url}" != https://* ]]; then
        pd_status_url="http://${pd_status_url}"
    fi
    local tikv_worker_url="${STARTER_TIKV_WORKER_URL:-localhost:19000}"

    prepare_starter_keyspace "${keyspace_name}" "${pd_status_url}" "${tidb_server_bin}" "${self_dir}"

    cat > "${config_file}" <<EOF
deploy-mode = "starter"
max-allowed-packet = ${max_allowed_packet}
tikv-worker-url = "${tikv_worker_url}"
EOF

    local activate_metadata_json=""
    if is_true "${keyspace_observability}"; then
        cat >> "${config_file}" <<EOF

[[keyspace-observability.fields]]
source = "tenant"
metric-label = "keyspace_meta_tenant"
slow-log-field = "Keyspace_meta_tenant"
stmt-log-field = "tenant"
required = true

[[keyspace-observability.fields]]
source = "project"
metric-label = "keyspace_meta_project"
slow-log-field = "Keyspace_meta_project"
stmt-log-field = "project"
required = true
EOF
        activate_metadata_json="$(printf '{"tenant":"%s","project":"%s"}' \
            "$(json_escape "${keyspace_meta_tenant}")" "$(json_escape "${keyspace_meta_project}")")"
    fi

    local tidb_server_args=(
        -P "${tidb_port}"
        -status "${status_port}"
        -host "127.0.0.1"
        -advertise-address "127.0.0.1"
        -status-host "127.0.0.1"
        -store tikv
        -path "${pd_endpoints}"
        -config "${config_file}"
        -tidb-service-scope dxf_service
        -log-file "${log_file}"
    )

    local starter_activated_from_standby="0"
    if is_true "${standby_mode}"; then
        starter_activated_from_standby="1"
        tidb_server_args+=(
            -standby=true
            -activation-timeout "${STARTER_ACTIVATION_TIMEOUT:-120}"
        )
    else
        tidb_server_args+=(
            -keyspace-name "${keyspace_name}"
        )
    fi

    echo "Starting external starter tidb-server on port ${tidb_port}, status port ${status_port}, log file ${log_file}"
    "${tidb_server_bin}" "${tidb_server_args[@]}" > "${stdout_log_file}" 2>&1 &
    starter_tidb_pid="$!"
    trap cleanup_tidb_server EXIT

    local status_url="http://127.0.0.1:${status_port}"
    wait_for_tidb_server "${status_url}" "${starter_tidb_pid}" "${log_file}" "${stdout_log_file}"
    if [[ "${starter_activated_from_standby}" == "1" ]]; then
        activate_starter_server "${status_url}" "${starter_tidb_pid}" "${log_file}" "${stdout_log_file}" \
            "${activate_response_file}" "${keyspace_name}" "${activate_export_id}" "${activate_max_idle_seconds}" "${activate_metadata_json}"
        wait_for_tidb_server "${status_url}" "${starter_tidb_pid}" "${log_file}" "${stdout_log_file}"
    fi

    export TIDB_STARTER_TEST_DSN="root@tcp(127.0.0.1:${tidb_port})/?timeout=5s&readTimeout=30s&writeTimeout=30s&multiStatements=true"
    export TIDB_STARTER_STATUS_URL="${status_url}"
    export TIDB_STARTER_PD_STATUS_URL="${pd_status_url}"
    export TIDB_STARTER_MAX_ALLOWED_PACKET="${max_allowed_packet}"
    export TIDB_STARTER_TIKV_WORKER_URL="${tikv_worker_url}"
    export TIDB_STARTER_KEYSPACE_NAME="${keyspace_name}"
    export TIDB_STARTER_ACTIVATED_FROM_STANDBY="${starter_activated_from_standby}"
    export TIDB_STARTER_ACTIVATE_EXPORT_ID="${activate_export_id}"
    if is_true "${keyspace_observability}"; then
        export TIDB_STARTER_KEYSPACE_OBSERVABILITY=1
        export TIDB_STARTER_KEYSPACE_META_TENANT="${keyspace_meta_tenant}"
        export TIDB_STARTER_KEYSPACE_META_PROJECT="${keyspace_meta_project}"
    fi

    echo "Running external starter tests: ./tests/realtikvtest/${test_suite}"
    go test "./tests/realtikvtest/${test_suite}" -v --tags=intest,nextgen -timeout "${suite_timeout}" "${extra_go_test_args[@]}"

    if [[ -z "${run_exit_wait_test}" ]]; then
        if [[ "${test_suite}" == "startertest" && "${#extra_go_test_args[@]}" -eq 0 ]]; then
            run_exit_wait_test="1"
        else
            run_exit_wait_test="0"
        fi
    fi
    if is_true "${run_exit_wait_test}" && [[ "${test_suite}" == "startertest" ]]; then
        export TIDB_STARTER_RUN_EXIT_WAIT_TEST=1
        echo "Running destructive external starter exit-wait test"
        go test "./tests/realtikvtest/${test_suite}" -v --tags=intest,nextgen \
            -timeout "${STARTER_EXIT_WAIT_TEST_TIMEOUT:-2m}" \
            -run '^TestExternalStarterExitWaitAndManagerNotifierContracts/graceful_exit_waits_for_open_connection$' \
            -count=1
        if [[ -n "${starter_tidb_pid:-}" ]] && kill -0 "${starter_tidb_pid}" >/dev/null 2>&1; then
            wait "${starter_tidb_pid}" >/dev/null 2>&1 || true
        fi
        starter_tidb_pid=""
    fi
}

function main() {
    local self_dir
    self_dir="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
    local repo_root
    repo_root="$(realpath "${self_dir}/../../../..")"
    cd "${repo_root}"

    "${self_dir}/bootstrap-test-with-cluster.sh" "${self_dir}/run-starter-tests-with-server.sh" --under-cluster "$@"
}

if [[ "${1:-}" == "--under-cluster" ]]; then
    shift
    run_under_cluster "$@"
else
    main "$@"
fi
