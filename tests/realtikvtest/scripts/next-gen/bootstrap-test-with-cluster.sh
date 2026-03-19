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
function main() {
    local data_base_dir=$(mktemp -d)
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
    start_minio ${data_base_dir}/minio/data

    # start the servers.
    bin/pd-server --name=pd-0 --config=${config_dir}/pd.toml --data-dir=${data_base_dir}/pd-0/data --peer-urls=http://127.0.0.1:2380 --advertise-peer-urls=http://127.0.0.1:2380 --client-urls=http://127.0.0.1:2379 --advertise-client-urls=http://127.0.0.1:2379 --initial-cluster=pd-0=http://127.0.0.1:2380,pd-1=http://127.0.0.1:2381,pd-2=http://127.0.0.1:2383 --force-new-cluster --log-file=pd0.log &
    bin/pd-server --name=pd-1 --config=${config_dir}/pd.toml --data-dir=${data_base_dir}/pd-1/data --peer-urls=http://127.0.0.1:2381 --advertise-peer-urls=http://127.0.0.1:2381 --client-urls=http://127.0.0.1:2382 --advertise-client-urls=http://127.0.0.1:2382 --initial-cluster=pd-0=http://127.0.0.1:2380,pd-1=http://127.0.0.1:2381,pd-2=http://127.0.0.1:2383 --force-new-cluster --log-file=pd1.log &
    bin/pd-server --name=pd-2 --config=${config_dir}/pd.toml --data-dir=${data_base_dir}/pd-2/data --peer-urls=http://127.0.0.1:2383 --advertise-peer-urls=http://127.0.0.1:2383 --client-urls=http://127.0.0.1:2384 --advertise-client-urls=http://127.0.0.1:2384 --initial-cluster=pd-0=http://127.0.0.1:2380,pd-1=http://127.0.0.1:2381,pd-2=http://127.0.0.1:2383 --force-new-cluster --log-file=pd2.log &
    bin/tikv-server --config=${config_dir}/tikv.toml --data-dir=${data_base_dir}tikv-0/data --addr=127.0.0.1:20160 --advertise-addr=127.0.0.1:20160 --status-addr=127.0.0.1:20180 --pd=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv0.log &
    bin/tikv-server --config=${config_dir}/tikv.toml --data-dir=${data_base_dir}tikv-1/data --addr=127.0.0.1:20161 --advertise-addr=127.0.0.1:20161 --status-addr=127.0.0.1:20181 --pd=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv1.log &
    bin/tikv-server --config=${config_dir}/tikv.toml --data-dir=${data_base_dir}tikv-2/data --addr=127.0.0.1:20162 --advertise-addr=127.0.0.1:20162 --status-addr=127.0.0.1:20182 --pd=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv2.log &
    bin/tikv-worker --config=${config_dir}/tikv-worker.toml --data-dir=${data_base_dir}/tikv-worker/data --addr=127.0.0.1:19000 --pd-endpoints=http://127.0.0.1:2379,http://127.0.0.1:2382,http://127.0.0.1:2384 --log-file=tikv-worker.log &

    sleep 10
    NEXT_GEN=1 "$@"
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

    # Wait for MinIO to be ready (simple check)
    for i in {1..10}; do
        if curl -s "http://127.0.0.1:$MINIO_PORT/minio/health/ready" | grep -q "OK"; then
            echo "MinIO is up"
            break
        fi
        sleep 1
    done
    echo "🎉 MinIO server started successfully"
}

function cleanup() {
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
