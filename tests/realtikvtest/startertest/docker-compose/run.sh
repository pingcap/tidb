#!/usr/bin/env bash
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

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(CDPATH= cd -- "${script_dir}/../../../.." && pwd)
compose_file="${script_dir}/docker-compose.yml"
wait_attempts=${COMPOSE_WAIT_ATTEMPTS:-300}

if [[ -n ${COMPOSE_PROJECT_NAME:-} ]]; then
    project=${COMPOSE_PROJECT_NAME}
else
    user_name=${USER:-user}
    sanitized_user=$(printf '%s' "${user_name}" | tr '[:upper:]' '[:lower:]' | tr -c 'abcdefghijklmnopqrstuvwxyz0123456789_-' '-')
    [[ -n ${sanitized_user} ]] || sanitized_user=user
    random_suffix=$(od -An -N4 -tu4 /dev/urandom | tr -d '[:space:]')
    project="tidb-txn-file-${sanitized_user}-$(date +%Y%m%d%H%M%S)-${random_suffix}"
fi

run_tmp=$(mktemp -d "${TMPDIR:-/tmp}/tidb-txn-file-compose.XXXXXX")
test_output="${run_tmp}/test-output.log"
cleanup_active=0

compose() {
    docker compose --project-name "${project}" --file "${compose_file}" --project-directory "${script_dir}" "$@"
}

print_file_log() {
    local service=$1
    local log_path=$2
    local container

    printf '\n===== %s file log: %s =====\n' "${service}" "${log_path}" >&2
    if compose exec -T "${service}" sh -c 'cat "$1"' sh "${log_path}"; then
        return 0
    fi

    container=$(compose ps --all --quiet "${service}" 2>/dev/null || true)
    if [[ -z ${container} ]]; then
        printf 'no container found for %s\n' "${service}" >&2
        return 0
    fi
    if ! docker cp "${container}:${log_path}" - | tar -xOf -; then
        printf 'could not collect %s from stopped %s container %s\n' "${log_path}" "${service}" "${container}" >&2
    fi
}

print_service_output() {
    local service=$1

    printf '\n===== %s container output =====\n' "${service}" >&2
    compose logs --no-color "${service}" || true
}

diagnostics() {
    printf '\n===== Compose diagnostics for %s =====\n' "${project}" >&2
    printf '\n===== compose ps =====\n' >&2
    compose ps --all || true
    printf '\n===== compose logs --no-color =====\n' >&2
    compose logs --no-color || true
    print_file_log pd /var/log/pd/pd.log
    print_file_log tikv-1 /var/log/tikv/tikv.log
    print_file_log tikv-2 /var/log/tikv/tikv.log
    print_file_log tikv-3 /var/log/tikv/tikv.log
    print_file_log tikv-worker /var/log/tikv-worker/tikv-worker.log
    print_service_output bootstrap-tidb
    print_file_log bootstrap-tidb /var/log/tidb/bootstrap-system.stdout.log
    print_file_log bootstrap-tidb /var/log/tidb/bootstrap-system.log
    print_service_output tidb
    print_file_log tidb /var/log/tidb/tidb.log
    print_service_output activate-tidb
    printf '\n===== test host capture =====\n' >&2
    if [[ -s ${test_output} ]]; then
        cat "${test_output}" >&2
    else
        printf 'no captured test output\n' >&2
    fi
    print_service_output test
    print_file_log test /tmp/startertest.log
}

cleanup() {
    local status=$?
    local down_status=0

    if [[ ${cleanup_active} -ne 0 ]]; then
        return
    fi
    cleanup_active=1
    trap - EXIT HUP INT TERM
    set +e
    if [[ ${status} -ne 0 ]]; then
        diagnostics
    fi
    compose down -v --remove-orphans --rmi local
    down_status=$?
    rm -rf -- "${run_tmp}"
    if [[ ${status} -eq 0 && ${down_status} -ne 0 ]]; then
        status=${down_status}
    fi
    exit "${status}"
}

on_signal() {
    local status=$1
    trap - HUP INT TERM
    exit "${status}"
}

trap cleanup EXIT
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

if ! compose_version=$(docker compose version --short 2>/dev/null); then
    echo "Docker Compose v2 or newer is required" >&2
    exit 1
fi
normalized_compose_version=${compose_version#v}
compose_major=${normalized_compose_version%%.*}
if [[ ! ${compose_major} =~ ^[0-9]+$ || ${compose_major} -lt 2 ]]; then
    printf 'Docker Compose v2 or newer is required (found: %s)\n' "${compose_version}" >&2
    exit 1
fi
if [[ ! ${wait_attempts} =~ ^[1-9][0-9]*$ ]]; then
    printf 'COMPOSE_WAIT_ATTEMPTS must be a positive integer (found: %s)\n' "${wait_attempts}" >&2
    exit 1
fi

wait_healthy() {
    local service=$1
    local container state exit_code health

    for ((attempt = 1; attempt <= wait_attempts; attempt++)); do
        container=$(compose ps --all --quiet "${service}" 2>/dev/null || true)
        if [[ -n ${container} ]]; then
            state=$(docker inspect --format '{{.State.Status}}' "${container}" 2>/dev/null || true)
            exit_code=$(docker inspect --format '{{.State.ExitCode}}' "${container}" 2>/dev/null || true)
            health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${container}" 2>/dev/null || true)
            case "${state}" in
                exited | dead)
                    printf '%s entered state %s with exit code %s while waiting for health (health: %s)\n' "${service}" "${state}" "${exit_code}" "${health}" >&2
                    return 1
                    ;;
            esac
            case "${health}" in
                healthy) return 0 ;;
                unhealthy)
                    printf '%s became unhealthy\n' "${service}" >&2
                    return 1
                    ;;
            esac
        fi
        sleep 1
    done
    printf 'timed out after %s seconds waiting for %s to become healthy\n' "${wait_attempts}" "${service}" >&2
    return 1
}

wait_completed() {
    local service=$1
    local container state exit_code

    for ((attempt = 1; attempt <= wait_attempts; attempt++)); do
        container=$(compose ps --all --quiet "${service}" 2>/dev/null || true)
        if [[ -n ${container} ]]; then
            state=$(docker inspect --format '{{.State.Status}}' "${container}" 2>/dev/null || true)
            exit_code=$(docker inspect --format '{{.State.ExitCode}}' "${container}" 2>/dev/null || true)
            case "${state}" in
                exited)
                    if [[ ${exit_code} == 0 ]]; then
                        return 0
                    fi
                    printf '%s exited with exit code %s\n' "${service}" "${exit_code:-unknown}" >&2
                    return 1
                    ;;
                dead)
                    printf '%s entered a dead state with exit code %s\n' "${service}" "${exit_code}" >&2
                    return 1
                    ;;
            esac
        fi
        sleep 1
    done
    printf 'timed out after %s seconds waiting for %s to complete\n' "${wait_attempts}" "${service}" >&2
    return 1
}

cd "${repo_root}"
compose config --quiet

compose up -d --no-build pd minio
wait_healthy pd
wait_healthy minio

compose up -d --no-deps --no-build minio-init
wait_completed minio-init

compose up -d --no-deps --no-build tikv-1 tikv-2 tikv-3
wait_healthy tikv-1
wait_healthy tikv-2
wait_healthy tikv-3

compose build tidb

compose up -d --no-deps --no-build bootstrap-tidb
wait_completed bootstrap-tidb

compose up -d --no-deps --no-build create-keyspace
wait_completed create-keyspace

compose up -d --no-deps --no-build tikv-worker
wait_healthy tikv-worker

compose up -d --no-deps --no-build tidb
wait_healthy tidb

compose up -d --no-deps --no-build activate-tidb
wait_completed activate-tidb

set +e
compose up --no-deps --no-build --abort-on-container-exit --exit-code-from test test 2>&1 | tee "${test_output}"
test_status=${PIPESTATUS[0]}
set -e
exit "${test_status}"
