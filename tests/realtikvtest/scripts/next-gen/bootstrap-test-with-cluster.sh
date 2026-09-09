#! /usr/bin/env bash
#
# Copyright 2025 PingCAP, Inc.
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

# It need TCP ports:
# - pd: 2379, 2380, 2381, 2383, 2384
# - tikv: 20160, 20161, 20162, 20180, 20181, 20182
# - tikv-worker: 19000
tiflash_compute_pid=""
schema_manager_dir="/tmp/tidb-realtikvtest-schemas"
cluster_start_timeout_seconds="${NEXT_GEN_CLUSTER_START_TIMEOUT_SECONDS:-120}"

function print_log_tail() {
    local service_name="$1"
    local log_file="$2"

    if [[ -f "${log_file}" ]]; then
        echo "Last 50 lines of ${service_name} log (${log_file}):" >&2
        tail -n 50 "${log_file}" >&2
    fi
}

function require_process_alive() {
    local service_name="$1"
    local pid="$2"
    local log_file="$3"

    if kill -0 "${pid}" >/dev/null 2>&1; then
        return 0
    fi

    echo "${service_name} exited before it became ready." >&2
    print_log_tail "${service_name}" "${log_file}"
    return 1
}

function wait_for_cluster_ready() {
    local pd0_pid="$1"
    local pd1_pid="$2"
    local pd2_pid="$3"
    local tikv0_pid="$4"
    local tikv1_pid="$5"
    local tikv2_pid="$6"
    local deadline=$((SECONDS + cluster_start_timeout_seconds))
    local stores
    local up_store_count

    while ((SECONDS < deadline)); do
        require_process_alive "PD 0" "${pd0_pid}" pd0.log || return 1
        require_process_alive "PD 1" "${pd1_pid}" pd1.log || return 1
        require_process_alive "PD 2" "${pd2_pid}" pd2.log || return 1
        require_process_alive "TiKV 0" "${tikv0_pid}" tikv0.log || return 1
        require_process_alive "TiKV 1" "${tikv1_pid}" tikv1.log || return 1
        require_process_alive "TiKV 2" "${tikv2_pid}" tikv2.log || return 1

        if curl -fsS --connect-timeout 1 --max-time 2 http://127.0.0.1:20180/status >/dev/null &&
            curl -fsS --connect-timeout 1 --max-time 2 http://127.0.0.1:20181/status >/dev/null &&
            curl -fsS --connect-timeout 1 --max-time 2 http://127.0.0.1:20182/status >/dev/null &&
            stores="$(curl -fsSL --connect-timeout 1 --max-time 2 http://127.0.0.1:2379/pd/api/v1/stores)"; then
            up_store_count="$(printf '%s\n' "${stores}" | grep -oE '"state_name"[[:space:]]*:[[:space:]]*"Up"' | wc -l | tr -d '[:space:]')"
            if [[ "${up_store_count}" -ge 3 ]]; then
                echo "PD and all TiKV stores are ready."
                return 0
            fi
        fi
        sleep 1
    done

    echo "PD and TiKV did not become ready within ${cluster_start_timeout_seconds} seconds." >&2
    print_log_tail "PD 0" pd0.log
    print_log_tail "PD 1" pd1.log
    print_log_tail "PD 2" pd2.log
    print_log_tail "TiKV 0" tikv0.log
    print_log_tail "TiKV 1" tikv1.log
    print_log_tail "TiKV 2" tikv2.log
    return 1
}

function wait_for_http_service_ready() {
    local service_name="$1"
    local pid="$2"
    local health_url="$3"
    local log_file="$4"
    local deadline=$((SECONDS + cluster_start_timeout_seconds))

    while ((SECONDS < deadline)); do
        require_process_alive "${service_name}" "${pid}" "${log_file}" || return 1
        if curl -fsS --connect-timeout 1 --max-time 2 "${health_url}" >/dev/null 2>&1; then
            echo "${service_name} is ready."
            return 0
        fi
        sleep 1
    done

    echo "${service_name} did not become ready within ${cluster_start_timeout_seconds} seconds." >&2
    print_log_tail "${service_name}" "${log_file}"
    return 1
}

function main() {
    local data_base_dir
    if ! data_base_dir="$(mktemp -d)"; then
        echo "Failed to create a temporary data directory." >&2
        return 1
    fi
    rm -rf "${schema_manager_dir}"
    mkdir -pv "${schema_manager_dir}"
    mkdir -pv ${data_base_dir}/pd-{0,1,2}/data
    mkdir -pv ${data_base_dir}/tikv-{0,1,2}/data
    mkdir -pv ${data_base_dir}/tikv-worker/data

    local config_dir="$(realpath "$(dirname "${BASH_SOURCE[0]}")/../../configs/next-gen")"
    if [[ ! -d "${config_dir}" ]]; then
        echo "Error: config_dir '${config_dir}' does not exist." >&2
        exit 1
    fi

    # init a bucket "next-gen-test", the bucket will be used for testing, do not change it.
    mkdir -pv ${data_base_dir}/minio/data/next-gen-test
    if ! start_minio ${data_base_dir}/minio/data; then
        return 1
    fi

    # start the servers.
    bin/pd-server --name=pd-0 --config=${config_dir}/pd.toml --data-dir=${data_base_dir}/pd-0/data --peer-urls=http://127.0.0.1:2380 --advertise-peer-urls=http://127.0.0.1:2380 --client-urls=http://127.0.0.1:2379 --advertise-client-urls=http://127.0.0.1:2379 --initial-cluster=pd-0=http://127.0.0.1:2380,pd-1=http://127.0.0.1:2381,pd-2=http://127.0.0.1:2383 --force-new-cluster --log-file=pd0.log &
    local pd0_pid="$!"
    bin/pd-server --name=pd-1 --config=${config_dir}/pd.toml --data-dir=${data_base_dir}/pd-1/data --peer-urls=http://127.0.0.1:2381 --advertise-peer-urls=http://127.0.0.1:2381 --client-urls=http://127.0.0.1:2382 --advertise-client-urls=http://127.0.0.1:2382 --initial-cluster=pd-0=http://127.0.0.1:2380,pd-1=http://127.0.0.1:2381,pd-2=http://127.0.0.1:2383 --force-new-cluster --log-file=pd1.log &
    local pd1_pid="$!"
    bin/pd-server --name=pd-2 --config=${config_dir}/pd.toml --data-dir=${data_base_dir}/pd-2/data --peer-urls=http://127.0.0.1:2383 --advertise-peer-urls=http://127.0.0.1:2383 --client-urls=http://127.0.0.1:2384 --advertise-client-urls=http://127.0.0.1:2384 --initial-cluster=pd-0=http://127.0.0.1:2380,pd-1=http://127.0.0.1:2381,pd-2=http://127.0.0.1:2383 --force-new-cluster --log-file=pd2.log &
    local pd2_pid="$!"
    bin/tikv-server --config=${config_dir}/tikv.toml --data-dir=${data_base_dir}/tikv-0/data --addr=127.0.0.1:20160 --advertise-addr=127.0.0.1:20160 --status-addr=127.0.0.1:20180 --pd=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv0.log &
    local tikv0_pid="$!"
    bin/tikv-server --config=${config_dir}/tikv.toml --data-dir=${data_base_dir}/tikv-1/data --addr=127.0.0.1:20161 --advertise-addr=127.0.0.1:20161 --status-addr=127.0.0.1:20181 --pd=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv1.log &
    local tikv1_pid="$!"
    bin/tikv-server --config=${config_dir}/tikv.toml --data-dir=${data_base_dir}/tikv-2/data --addr=127.0.0.1:20162 --advertise-addr=127.0.0.1:20162 --status-addr=127.0.0.1:20182 --pd=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv2.log &
    local tikv2_pid="$!"

    if ! wait_for_cluster_ready "${pd0_pid}" "${pd1_pid}" "${pd2_pid}" "${tikv0_pid}" "${tikv1_pid}" "${tikv2_pid}"; then
        return 1
    fi

    bin/tikv-worker --config=${config_dir}/tikv-worker.toml --data-dir=${data_base_dir}/tikv-worker/data --addr=127.0.0.1:19000 --pd-endpoints=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv-worker.log &
    local tikv_worker_pid="$!"

    if ! wait_for_http_service_ready "TiKV worker" "${tikv_worker_pid}" "http://127.0.0.1:19000/healthz" tikv-worker.log; then
        return 1
    fi

    if is_true "${STARTER_COLUMNAR_AP:-}"; then
        start_tiflash_compute "${data_base_dir}"
        sleep 10
    fi

    NEXT_GEN=1 "$@"
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

function start_tiflash_compute() {
    local data_base_dir="$1"
    local tiflash_bin="${TIFLASH_BIN_PATH:-bin/tiflash}"
    local tiflash_dir="${data_base_dir}/tiflash-compute"
    local tiflash_config="${tiflash_dir}/tiflash.toml"
    local tiflash_proxy_config="${tiflash_dir}/tiflash-proxy.toml"

    if [[ ! -x "${tiflash_bin}" ]]; then
        echo "TiFlash compute requires executable '${tiflash_bin}'." >&2
        exit 1
    fi

    mkdir -pv "${tiflash_dir}/data" "${tiflash_dir}/cache" "${tiflash_dir}/proxy"
    cat > "${tiflash_proxy_config}" <<EOF
[storage]
reserve-space = "0"
api-version = 2
enable-ttl = true

[raftstore]
capacity = "100GB"

[dfs]
prefix = "tikv"
s3-endpoint = "http://127.0.0.1:9000"
s3-key-id = "minioadmin"
s3-secret-key = "minioadmin"
s3-bucket = "next-gen-test"
s3-region = "local"
remote-compactor-addr = "http://127.0.0.1:19000/compact"
EOF
    cat > "${tiflash_config}" <<EOF
tcp_port = 9001

[flash]
disaggregated_mode = "tiflash_compute"
service_addr = "127.0.0.1:3930"
use_columnar = true

[flash.proxy]
addr = "127.0.0.1:20170"
advertise-addr = "127.0.0.1:20170"
engine-addr = "127.0.0.1:3930"
status-addr = "127.0.0.1:20292"
advertise-status-addr = "127.0.0.1:20292"
data-dir = "${tiflash_dir}/proxy"
config = "${tiflash_proxy_config}"

[storage]
api_version = 2

[storage.main]
dir = ["${tiflash_dir}/data"]
capacity = [5368709120]

[storage.remote.cache]
dir = "${tiflash_dir}/cache"
capacity = 5368709120

[storage.s3]
endpoint = "http://127.0.0.1:9000"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
bucket = "next-gen-test"
root = "/tiflash"

[raft]
pd_addr = "127.0.0.1:2379"

[logger]
level = "debug"
log = "tiflash-compute.log"
errorlog = "tiflash-compute-error.log"
EOF

    TIFLASH_COLUMNAR=true "${tiflash_bin}" server --config-file="${tiflash_config}" > tiflash-compute-stdout.log 2>&1 &
    tiflash_compute_pid="$!"
}

function start_minio() {
    local data_base_dir="$1"

    # Ensure MinIO binary exists, download if not exists.
    MINIO_BIN_PATH=${MINIO_BIN_PATH:-$(which minio)}
    if [[ -z "$MINIO_BIN_PATH" || ! -x "$MINIO_BIN_PATH" ]]; then
        echo "MinIO binary not found, downloading..."
        MINIO_BIN_PATH="bin/minio"
        # Determine OS and ARCH for MinIO download URL
        OS=$(uname | tr '[:upper:]' '[:lower:]')
        ARCH=$(uname -m)
        case "$ARCH" in
            x86_64) ARCH="amd64" ;;
            aarch64 | arm64) ARCH="arm64" ;;
            *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
        esac
        curl -sSL -o "$MINIO_BIN_PATH" "https://dl.min.io/server/minio/release/${OS}-${ARCH}/minio"
        chmod +x "$MINIO_BIN_PATH"
    fi

    MINIO_PORT=${MINIO_PORT:-9000}
    MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
    MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}

    # Start MinIO server in background
    export MINIO_ROOT_USER="$MINIO_ACCESS_KEY"
    export MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY"
    echo "🚀 Starting MinIO server..."
    "$MINIO_BIN_PATH" server "$data_base_dir" --address ":$MINIO_PORT" > minio.log 2>&1 &
    MINIO_PID=$!

    wait_for_http_service_ready "MinIO" "${MINIO_PID}" "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" minio.log
}

function cleanup() {
    if [[ -n "${tiflash_compute_pid}" ]] && kill -0 "${tiflash_compute_pid}" >/dev/null 2>&1; then
        kill -9 "${tiflash_compute_pid}" || true
        wait "${tiflash_compute_pid}" >/dev/null 2>&1 || true
    fi
    rm -rf "${schema_manager_dir}"

    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS: no -r option
        killall -9 -q tikv-worker || true
        killall -9 -q tikv-server || true
        killall -9 -q pd-server || true
        killall -9 -q minio || true
    else
        # Linux: supports -r for regex
        killall -9 -r -q tikv-worker || true
        killall -9 -r -q tikv-server || true
        killall -9 -r -q pd-server || true
        killall -9 -r -q minio || true
    fi

    make failpoint-disable
}

exit_code=0
{ # try block
    main "$@"
} || { # catch block
   exit_code="$?"  # exit code of last command which is 44
}
# finally block:
cleanup

if [[ "$exit_code" != '0' ]]; then
   exit ${exit_code}
fi
