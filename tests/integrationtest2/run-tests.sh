#!/usr/bin/env bash
# Copyright 2024 PingCAP, Inc.
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

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

set -x

# Variables to set paths for each server binary
TIDB_BIN="./third_bin/tidb-server"
TIKV_BIN="./third_bin/tikv-server"
PD_BIN="./third_bin/pd-server"
DUMPLING_BIN="./third_bin/dumpling"
TICDC_BIN="./third_bin/cdc"
TIFLASH_BIN_DEFAULT="./third_bin/tiflash"
TICI_BIN_DEFAULT="./third_bin/tici-server"
MINIO_BIN_DEFAULT="./third_bin/minio"
MINIO_MC_BIN_DEFAULT="./third_bin/mc"

# Variables to set data directories
PD_DATA_DIR="./data/pd_data"
PD_DATA_DIR2="./data/pd_data2"
TIKV_DATA_DIR="./data/tikv_data"
TIKV_DATA_DIR2="./data/tikv_data2"
TIFLASH_DATA_DIR="./data/tiflash_data"
TIFLASH_DATA_DIR2="./data/tiflash_data2"
TICDC_DATA_DIR="./data/ticdc_data"
MINIO_DATA_DIR="./data/minio_data"

rm -rf ./data
mkdir ./data

TIDB_LOG_FILE="./logs/tidb.log"
TIDB_LOG_FILE2="./logs/tidb2.log"
TIKV_LOG_FILE="./logs/tikv.log"
TIKV_LOG_FILE2="./logs/tikv2.log"
TIFLASH_LOG_FILE="./logs/tiflash.log"
TIFLASH_LOG_FILE2="./logs/tiflash2.log"
PD_LOG_FILE="./logs/pd.log"
PD_LOG_FILE2="./logs/pd2.log"
TICDC_LOG_FILE="./logs/ticdc.log"
TICI_LOG_FILE="./logs/tici.log"
TICI_META_LOG_FILE="${TICI_META_LOG_FILE:-./logs/tici-meta.log}"
TICI_WORKER_LOG_FILE="${TICI_WORKER_LOG_FILE:-./logs/tici-worker.log}"
MINIO_LOG_FILE="./logs/minio.log"

rm -rf ./logs
mkdir ./logs

build=1
mysql_tester="./mysql_tester"
tidb_server=""
ticdc_server=$TICDC_BIN
portgenerator=""
mysql_tester_log="./integration-test.out"
tests=""
record=0
record_case=""
stats="s"

set -eu

PIDS_TO_KILL=()
function register_pid() {
    local pid=$1
    if [ -n "$pid" ]; then
        PIDS_TO_KILL+=("$pid")
    fi
}

function cleanup() {
    set +e
    local pid
    for pid in "${PIDS_TO_KILL[@]}"; do
        kill -15 "$pid" 2>/dev/null || true
    done
    local job_pids
    job_pids=$(jobs -p 2>/dev/null || true)
    for pid in $job_pids; do
        kill -15 "$pid" 2>/dev/null || true
    done
    sleep 1
    for pid in "${PIDS_TO_KILL[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    for pid in $job_pids; do
        kill -9 "$pid" 2>/dev/null || true
    done
}

trap cleanup EXIT INT TERM
# make tests stable time zone wise
export TZ="Asia/Shanghai"

TICI_BIN="${TICI_BIN:-$TICI_BIN_DEFAULT}"
MINIO_BIN="${MINIO_BIN:-$MINIO_BIN_DEFAULT}"
MINIO_MC_BIN="${MINIO_MC_BIN:-$MINIO_MC_BIN_DEFAULT}"
TIFLASH_BIN="${TIFLASH_BIN:-$TIFLASH_BIN_DEFAULT}"
TICI_ARGS="${TICI_ARGS:-}"
TICI_META_CONFIG="${TICI_META_CONFIG:-}"
TICI_WORKER_CONFIG="${TICI_WORKER_CONFIG:-}"
TICI_META_ARGS="${TICI_META_ARGS:-$TICI_ARGS}"
TICI_WORKER_ARGS="${TICI_WORKER_ARGS:-$TICI_ARGS}"
TIFLASH_CONFIG="${TIFLASH_CONFIG:-}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-ticidefaultbucket}"
MINIO_PREFIX="${MINIO_PREFIX:-tici_default_prefix/cdc}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:${MINIO_PORT}}"
TICDC_S3_SINK_URI_DEFAULT="${TICDC_S3_SINK_URI_DEFAULT:-}"
TICDC_S3_SINK_URI="${TICDC_S3_SINK_URI:-}"
TICI_CONFIG_DIR="${TICI_CONFIG_DIR:-$SCRIPT_DIR/tici/config}"
PORT_START="${PORT_START:-20000}"

if [ -d "$TIFLASH_BIN" ] && [ -x "$TIFLASH_BIN/tiflash" ]; then
    TIFLASH_BIN="$TIFLASH_BIN/tiflash"
fi
if [ ! -x "$TICDC_BIN" ] && [ -x "./third_bin/ticdc" ]; then
    TICDC_BIN="./third_bin/ticdc"
fi

if ! command -v ss >/dev/null 2>&1 && ! command -v nc >/dev/null 2>&1; then
    echo "Error: ss or nc is required for port checks but not found in PATH." >&2
    exit 1
fi

function help_message()
{
    echo "Usage: $0 [options]

    -h: Print this help message.

    -s <tidb-server-path>: Use tidb-server in <tidb-server-path> for testing.
                           eg. \"./run-tests.sh -s ./integrationtest_tidb-server\"

    -b <y|Y|n|N>: \"y\" or \"Y\" for building test binaries [default \"y\" if this option is not specified].
                  \"n\" or \"N\" for not to build.
                  The building of tidb-server will be skiped if \"-s <tidb-server-path>\" is provided.
                  The building of portgenerator will be skiped if \"-s <portgenerator-path>\" is provided.

    -r <test-name>|all: Run tests in file \"t/<test-name>.test\" and record result to file \"r/<test-name>.result\".
                        \"all\" for running all tests and record their results.

    -t <test-name>: Run tests in file \"t/<test-name>.test\".
                    This option will be ignored if \"-r <test-name>\" is provided.
                    Run all tests if this option is not provided.

    tici/minio settings (env vars):
      TICI_BIN, TICI_META_CONFIG, TICI_WORKER_CONFIG, TICI_META_ARGS, TICI_WORKER_ARGS,
      MINIO_BIN, MINIO_MC_BIN, MINIO_PORT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
      MINIO_BUCKET, MINIO_PREFIX, MINIO_ENDPOINT, TICDC_S3_SINK_URI
    tiflash settings (env vars):
      TIFLASH_BIN, TIFLASH_CONFIG
    tidb settings (env vars):
      TIDB_PORT (preferred upstream port; auto-selects another if in use)
    port settings (env vars):
      PORT_START (base port to search from; default 20000)

"
}

# Function to find an available port starting from a given port
function find_available_port() {
    local port=$1

    while :; do
        if [ "$port" -ge 65536 ]; then
            echo "Error: No available ports found below 65536." >&2
            exit 1
        fi
        if ! port_in_use "$port"; then
            echo $port
            return 0
        fi
        ((port++))
    done
}

function port_in_use() {
    local port=$1

    if command -v ss >/dev/null 2>&1; then
        if ss -ltnH "sport = :$port" 2>/dev/null | grep -q .; then
            return 0
        fi
        # Some ss versions ignore the filter; fall back to parsing all listeners.
        if ss -ltnH 2>/dev/null | awk '{print $4}' | sed -E 's/.*:([0-9]+)$/\\1/' | grep -qx "$port"; then
            return 0
        fi
    elif command -v nc >/dev/null 2>&1; then
        if nc -z -w 1 127.0.0.1 "$port" >/dev/null 2>&1; then
            return 0
        fi
    fi

    return 1
}

NEXT_PORT="${PORT_START}"
ALLOCATED_PORT=""
declare -A RESERVED_PORTS=()
function alloc_port() {
    local start=${1:-$NEXT_PORT}
    local port

    port=$(reserve_port "auto" "$start") || exit 1
    NEXT_PORT=$((port + 1))
    ALLOCATED_PORT=$port
}

function reserve_port() {
    local label=$1
    local port=$2
    local candidate=$port

    while :; do
        candidate=$(find_available_port "$candidate") || exit 1
        if [[ -n "${RESERVED_PORTS[$candidate]:-}" ]]; then
            candidate=$((candidate + 1))
            continue
        fi
        RESERVED_PORTS[$candidate]="$label"
        if [[ "$candidate" != "$port" ]]; then
            echo "$label port $port is in use; using $candidate"
        fi
        echo "$candidate"
        return 0
    done
}

function refresh_minio_vars() {
    MINIO_ENDPOINT="http://127.0.0.1:${MINIO_PORT}"
    TICDC_S3_SINK_URI_DEFAULT="s3://${MINIO_BUCKET}/${MINIO_PREFIX}?endpoint=${MINIO_ENDPOINT}&access-key=${MINIO_ACCESS_KEY}&secret-access-key=${MINIO_SECRET_KEY}&provider=minio&protocol=canal-json&enable-tidb-extension=true&output-row-key=true"
    if [ -z "$TICDC_S3_SINK_URI" ]; then
        TICDC_S3_SINK_URI="$TICDC_S3_SINK_URI_DEFAULT"
    fi
}

function ensure_minio_port() {
    MINIO_PORT=$(reserve_port "minio" "$MINIO_PORT")
    refresh_minio_vars
    echo "MinIO will use port $MINIO_PORT"
}

function resolve_bin() {
    local bin="$1"

    if [[ -x "$bin" ]]; then
        if [[ "$bin" = /* ]]; then
            echo "$bin"
        else
            echo "$(cd "$(dirname "$bin")" && pwd)/$(basename "$bin")"
        fi
        return 0
    fi
    if [[ "$bin" != /* ]]; then
        local script_path="$SCRIPT_DIR/$bin"
        if [[ -x "$script_path" ]]; then
            echo "$script_path"
            return 0
        fi
    fi
    if command -v "$bin" >/dev/null 2>&1; then
        command -v "$bin"
        return 0
    fi

    return 1
}

function resolve_path() {
    local path="$1"

    if [[ -z "$path" ]]; then
        return 1
    fi
    if [[ "$path" = /* ]]; then
        echo "$path"
        return 0
    fi
    echo "$SCRIPT_DIR/$path"
    return 0
}

function ensure_tici_ports() {
    : "${TICI_META_PORT:=8500}"
    : "${TICI_META_STATUS_PORT:=8501}"
    : "${TICI_WORKER_PORT:=8510}"
    : "${TICI_WORKER_STATUS_PORT:=8511}"
    : "${TICI_READER_PORT:=8520}"

    TICI_META_PORT=$(reserve_port "tici-meta" "$TICI_META_PORT")
    TICI_META_STATUS_PORT=$(reserve_port "tici-meta-status" "$TICI_META_STATUS_PORT")
    TICI_WORKER_PORT=$(reserve_port "tici-worker" "$TICI_WORKER_PORT")
    TICI_WORKER_STATUS_PORT=$(reserve_port "tici-worker-status" "$TICI_WORKER_STATUS_PORT")
    TICI_READER_PORT=$(reserve_port "tici-reader" "$TICI_READER_PORT")
    TICI_READER_ADDR="127.0.0.1:$TICI_READER_PORT"
}

function ensure_tiflash_ports() {
    local service_port
    local proxy_port

    : "${TIFLASH_SERVICE_PORT:=3930}"
    : "${TIFLASH_PROXY_PORT:=20170}"
    : "${TIFLASH_PROXY_STATUS_PORT:=20292}"

    service_port=$(reserve_port "tiflash-service" "$TIFLASH_SERVICE_PORT")
    TIFLASH_SERVICE_PORT="$service_port"
    TIFLASH_SERVICE_ADDR="127.0.0.1:$service_port"

    proxy_port=$(reserve_port "tiflash-proxy" "$TIFLASH_PROXY_PORT")
    TIFLASH_PROXY_PORT="$proxy_port"
    TIFLASH_PROXY_SERVER_ADDR="127.0.0.1:$proxy_port"
    TIFLASH_PROXY_ADVERTISE_ADDR="127.0.0.1:$proxy_port"

    TIFLASH_PROXY_STATUS_PORT=$(reserve_port "tiflash-proxy-status" "$TIFLASH_PROXY_STATUS_PORT")
    TIFLASH_PROXY_STATUS_ADDR="127.0.0.1:$TIFLASH_PROXY_STATUS_PORT"
    TIFLASH_PROXY_ENGINE_ADDR="127.0.0.1:$service_port"
}

function require_tici_binaries() {
    local missing=()

    if ! resolve_bin "$PD_BIN" >/dev/null; then
        missing+=("pd-server")
    fi
    if ! resolve_bin "$TIKV_BIN" >/dev/null; then
        missing+=("tikv-server")
    fi
    if ! resolve_bin "$TICDC_BIN" >/dev/null && ! resolve_bin "./third_bin/ticdc" >/dev/null; then
        missing+=("cdc/ticdc")
    fi
    if ! resolve_bin "$TICI_BIN" >/dev/null; then
        missing+=("tici-server")
    fi
    if ! resolve_bin "$MINIO_BIN" >/dev/null; then
        missing+=("minio")
    fi
    if [ -d "$TIFLASH_BIN" ]; then
        if [ ! -x "$TIFLASH_BIN/tiflash" ]; then
            missing+=("tiflash")
        fi
    else
        if ! resolve_bin "$TIFLASH_BIN" >/dev/null; then
            missing+=("tiflash")
        fi
    fi

    if [ ${#missing[@]} -ne 0 ]; then
        echo "Error: required binaries missing for TiCI tests: ${missing[*]}" >&2
        echo "Run: $SCRIPT_DIR/tici/download.sh to download them." >&2
        exit 1
    fi
}

function should_run_tici() {
    if [ $record -eq 1 ]; then
        if [ "$record_case" = "all" ] || [[ "$record_case" == tici/* ]]; then
            return 0
        fi
        return 1
    fi

    if [ -z "$tests" ]; then
        if [ -d "t/tici" ] && find t/tici -name "*.test" -print -quit | grep -q .; then
            return 0
        fi
        return 1
    fi

    read -ra test_list <<< "$tests"
    for test_name in "${test_list[@]}"; do
        case "$test_name" in
            tici|tici/*)
                return 0
                ;;
        esac
    done

    return 1
}

function build_tidb_server()
{
    tidb_server="./integrationtest_tidb-server"
    echo "building tidb-server binary: $tidb_server"
    rm -rf $tidb_server
    mkdir -p third_bin
    rm -rf third_bin/br
    rm -rf third_bin/dumpling

    cd ../..
    make server
    make build_br
    make build_dumpling

    cd -
    ln -s ../../bin/tidb-server $tidb_server
    ln -s ../../../bin/br third_bin/br
    ln -s ../../../bin/dumpling third_bin/dumpling
}

function build_mysql_tester()
{
    echo "building mysql-tester binary: $mysql_tester"
    rm -rf $mysql_tester
    GOBIN=$PWD go install github.com/bb7133/mysql-tester/src@2148bd9e5299de307244a15ed0047c953a035dc4
    mv src mysql_tester
}

while getopts "t:s:r:b:d:c:i:h" opt; do
    case $opt in
        t)
            tests="$OPTARG"
            ;;
        s)
            tidb_server="$OPTARG"
            ;;
        r)
            record=1
            record_case="$OPTARG"
            ;;
        b)
            case $OPTARG in
                y|Y)
                    build=1
                    ;;
                n|N)
                    build=0
                    ;;
                *)
                    help_message 1>&2
                    exit 1
                    ;;
            esac
            ;;
        h)
            help_message
            exit 0
            ;;
        *)
            help_message 1>&2
            exit 1
            ;;
    esac
done

if should_run_tici; then
    require_tici_binaries
fi

if [ $build -eq 1 ]; then
    if [ -z "$tidb_server" ]; then
        build_tidb_server
    else
        echo "skip building tidb-server, using existing binary: $tidb_server"
    fi
    build_mysql_tester
else
    if [ -z "$tidb_server" ]; then
        tidb_server="./integrationtest_tidb-server"
        if [[ ! -f "$tidb_server" ]]; then
            build_tidb_server
        else
            echo "skip building tidb-server, using existing binary: $tidb_server"
        fi
    fi
    if [ -z "$mysql_tester" ]; then
        mysql_tester="./mysql_tester"
        if [[ ! -f "$mysql_tester" ]]; then
            build_mysql_tester
        else
            echo "skip building mysql-tester, using existing binary: $mysql_tester"
        fi
    fi
fi

# PD Server Configuration
start_pd_server() {
    client_port=${1:-2379}
    peer_port=${2:-2380}
    data_dir=${3:-$PD_DATA_DIR}
    log_dir=${4:-$PD_LOG_FILE}

	echo "Starting PD server..."
    mkdir -p $data_dir

	$PD_BIN --name="pd" --data-dir="$data_dir" --log-file="$log_dir" \
          --client-urls="http://127.0.0.1:$client_port" \
          --peer-urls="http://127.0.0.1:$peer_port" \
          --advertise-client-urls="http://127.0.0.1:$client_port" \
          --advertise-peer-urls="http://127.0.0.1:$peer_port" &
    register_pid $!
#          --config="config/pd.toml"
	sleep 5  # Wait for PD to sart
}

# TiKV Server Configuration
start_tikv_server() {
    pd_client_port=${1:-2379}
    tikv_port=${2:-20160}
    tikv_status_port=${3:-20180}
    data_dir=${4:-$TIKV_DATA_DIR}
    log_dir=${5:-$TIKV_LOG_FILE}

	echo "Starting TiKV server..."
    mkdir -p $data_dir

    $TIKV_BIN --pd="http://127.0.0.1:$pd_client_port" \
       --addr="127.0.0.1:$tikv_port" \
       --advertise-addr="127.0.0.1:$tikv_port" \
       --status-addr="127.0.0.1:$tikv_status_port" \
       --data-dir="$data_dir" \
       --log-file="$log_dir" &
    register_pid $!
    sleep 5  # Wait for TiKV to connect to PD
}

# TiFlash Server Configuration
start_tiflash_server() {
    local tiflash_bin

    tiflash_bin=$(resolve_bin "$TIFLASH_BIN") || {
        echo "tiflash binary not found: $TIFLASH_BIN" >&2
        exit 1
    }

    echo "Starting TiFlash server..."
    mkdir -p $TIFLASH_DATA_DIR
    if [ -n "$TIFLASH_CONFIG" ]; then
        $tiflash_bin server --log-file=./logs/tiflash-server.log --config-file="$TIFLASH_CONFIG" > "$TIFLASH_LOG_FILE" 2>&1 &
    else
        $tiflash_bin server --log-file=./logs/tiflash-server.log > "$TIFLASH_LOG_FILE" 2>&1 &
    fi
    register_pid $!

    sleep 5  # Wait for TiFlash to connect
}

function start_tidb_server()
{
    pd_client_addr=${1:-127.0.0.1:2379}
    tidb_port=${2:-4000}
    tidb_status_port=${3:-10080}
    log_file=${4:-$TIDB_LOG_FILE}

    config_file="config.toml"
    echo "start tidb-server, log file: $log_file"
    $tidb_server -P "$tidb_port" \
        -status "$tidb_status_port" \
        -config $config_file \
        -store tikv \
        -path "${pd_client_addr}" > $log_file 2>&1 &
    SERVER_PID=$!
    register_pid $SERVER_PID
    echo "tidb-server(PID: $SERVER_PID) started, port: $tidb_port"
}

function wait_for_port_ready() {
    local host=$1
    local port=$2
    local label=$3
    local log_file=$4
    local timeout=${5:-60}
    local deadline=$((SECONDS + timeout))

    while [ $SECONDS -lt $deadline ]; do
        if command -v nc >/dev/null 2>&1; then
            if nc -z -w 1 "$host" "$port" >/dev/null 2>&1; then
                echo "$label is reachable at $host:$port"
                return 0
            fi
        else
            if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
                exec 3>&-
                echo "$label is reachable at $host:$port"
                return 0
            fi
        fi
        sleep 1
    done

    echo "$label did not become ready within ${timeout}s"
    if [ -n "$log_file" ] && [ -f "$log_file" ]; then
        echo "=== tail $log_file ==="
        tail -n 50 "$log_file" || true
        echo "=== end ==="
    fi
    return 1
}

function prepare_tici_config() {
    if [ -d "$TICI_CONFIG_DIR" ]; then
        if [ -f "$TICI_CONFIG_DIR/env.sh" ]; then
            source "$TICI_CONFIG_DIR/env.sh"
        fi
        ensure_tici_ports
        ensure_tiflash_ports
        export S3_USE_PATH_STYLE=true
        export S3_ENDPOINT="$MINIO_ENDPOINT"
        export S3_REGION="${S3_REGION:-us-east-1}"
        export S3_ACCESS_KEY="$MINIO_ACCESS_KEY"
        export S3_SECRET_KEY="$MINIO_SECRET_KEY"
        export S3_BUCKET="$MINIO_BUCKET"
        export S3_PREFIX="tici_default_prefix"
        export TIDB_PORT="${TIDB_PORT:-4000}"
        # TiCI may fall back to AWS default credential chain when keys are empty,
        # so keep AWS envs aligned with MinIO to avoid 403 errors.
        export AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY"
        export AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY"
        if [ -n "${upstream_pd_client_port:-}" ]; then
            export PD_ADDR="127.0.0.1:$upstream_pd_client_port"
        else
            export PD_ADDR="127.0.0.1:2379"
        fi
        if [ -z "$PD_ADDR" ]; then
            echo "PD_ADDR is empty; cannot start tici meta/worker" >&2
            exit 1
        fi
        make_tiflash_config_for_tici
        if command -v envsubst >/dev/null 2>&1; then
            if [ -f "$TICI_CONFIG_DIR/meta.toml.in" ]; then
                envsubst < "$TICI_CONFIG_DIR/meta.toml.in" > "$TICI_CONFIG_DIR/meta.toml"
                TICI_META_CONFIG="${TICI_META_CONFIG:-$TICI_CONFIG_DIR/meta.toml}"
            fi
            if [ -f "$TICI_CONFIG_DIR/worker.toml.in" ]; then
                envsubst < "$TICI_CONFIG_DIR/worker.toml.in" > "$TICI_CONFIG_DIR/worker.toml"
                TICI_WORKER_CONFIG="${TICI_WORKER_CONFIG:-$TICI_CONFIG_DIR/worker.toml}"
            fi
            if [ -f "$TICI_CONFIG_DIR/tiflash-learner.toml.in" ]; then
                envsubst < "$TICI_CONFIG_DIR/tiflash-learner.toml.in" > "$TICI_CONFIG_DIR/tiflash-learner.toml"
                TIFLASH_PROXY_CONFIG="${TIFLASH_PROXY_CONFIG:-$TICI_CONFIG_DIR/tiflash-learner.toml}"
            else
                echo "tiflash-learner.toml.in not found in $TICI_CONFIG_DIR; TiCI requires TiFlash proxy config" >&2
                exit 1
            fi
            if [ -f "$TICI_CONFIG_DIR/tiflash.toml.in" ]; then
                envsubst < "$TICI_CONFIG_DIR/tiflash.toml.in" > "$TICI_CONFIG_DIR/tiflash.toml"
                TIFLASH_CONFIG="${TIFLASH_CONFIG:-$TICI_CONFIG_DIR/tiflash.toml}"
            else
                echo "tiflash.toml.in not found in $TICI_CONFIG_DIR; TiCI requires TiFlash config" >&2
                exit 1
            fi
        else
            echo "envsubst not found; cannot render TiCI configs" >&2
            exit 1
        fi
    fi
}

function make_tiflash_config_for_tici() {
    TIFLASH_LISTEN_HOST="127.0.0.1"
    : "${TIFLASH_DATA_DIR:=./data/tiflash_data}"
    : "${TIFLASH_TMP_DIR:=$TIFLASH_DATA_DIR/tmp}"
    : "${TIFLASH_CAPACITY:=10737418240}"
    : "${TIDB_STATUS_ADDR:=127.0.0.1:10080}"
    : "${TIFLASH_LOG_PATH:=./logs/tiflash-stdout.log}"
    : "${TIFLASH_ERROR_LOG_PATH:=./logs/tiflash-stderr.log}"
    : "${TIFLASH_PROXY_LOG_FILE:=./logs/tiflash-proxy.log}"
    : "${TIFLASH_PROXY_DATA_DIR:=$TIFLASH_DATA_DIR/proxy}"
    : "${TIFLASH_PROXY_RESERVE_SPACE:=1KB}"
    : "${TIFLASH_SERVICE_PORT:=3930}"
    : "${TIFLASH_PROXY_PORT:=20170}"
    : "${TIFLASH_PROXY_STATUS_PORT:=20292}"
    : "${TICI_READER_PORT:=8520}"
    : "${TICI_READER_HEARTBEAT_INTERVAL:=3s}"
    : "${TICI_READER_MAX_HEARTBEAT_RETRIES:=3}"
    : "${TICI_READER_HEARTBEAT_WORKER_COUNT:=8}"
    TIFLASH_SERVICE_ADDR="127.0.0.1:${TIFLASH_SERVICE_PORT}"
    TIFLASH_PROXY_SERVER_ADDR="127.0.0.1:${TIFLASH_PROXY_PORT}"
    TIFLASH_PROXY_ADVERTISE_ADDR="$TIFLASH_PROXY_SERVER_ADDR"
    TIFLASH_PROXY_STATUS_ADDR="127.0.0.1:${TIFLASH_PROXY_STATUS_PORT}"
    TIFLASH_PROXY_ENGINE_ADDR="127.0.0.1:${TIFLASH_SERVICE_PORT}"
    TICI_READER_ADDR="127.0.0.1:${TICI_READER_PORT}"
    mkdir -p "$TICI_CONFIG_DIR"
    TIFLASH_CONFIG="${TIFLASH_CONFIG:-$TICI_CONFIG_DIR/tiflash.toml}"
    TIFLASH_PROXY_CONFIG="${TIFLASH_PROXY_CONFIG:-$TICI_CONFIG_DIR/tiflash-learner.toml}"

    export TIFLASH_LISTEN_HOST TIFLASH_DATA_DIR TIFLASH_TMP_DIR TIFLASH_CAPACITY \
        TIDB_STATUS_ADDR TIFLASH_LOG_PATH TIFLASH_ERROR_LOG_PATH TIFLASH_PROXY_LOG_FILE \
        TIFLASH_PROXY_DATA_DIR TIFLASH_PROXY_RESERVE_SPACE TIFLASH_PROXY_CONFIG \
        TICI_READER_HEARTBEAT_INTERVAL TICI_READER_MAX_HEARTBEAT_RETRIES \
        TICI_READER_HEARTBEAT_WORKER_COUNT TIFLASH_SERVICE_ADDR TIFLASH_PROXY_SERVER_ADDR \
        TIFLASH_PROXY_ADVERTISE_ADDR TIFLASH_PROXY_STATUS_ADDR TIFLASH_PROXY_ENGINE_ADDR \
        TICI_READER_ADDR TIFLASH_CONFIG
}

function start_tici_server() {
    meta_log_file=${1:-$TICI_META_LOG_FILE}
    worker_log_file=${2:-$TICI_WORKER_LOG_FILE}
    local tici_bin
    local meta_config
    local worker_config

    tici_bin=$(resolve_bin "$TICI_BIN") || {
        echo "tici binary not found: $TICI_BIN" >&2
        exit 1
    }

    if [ -z "$TICI_META_CONFIG" ] || [ -z "$TICI_WORKER_CONFIG" ]; then
        echo "tici meta/worker config is required: TICI_META_CONFIG, TICI_WORKER_CONFIG" >&2
        exit 1
    fi

    meta_log_file=$(resolve_path "$meta_log_file")
    worker_log_file=$(resolve_path "$worker_log_file")
    meta_config=$(resolve_path "$TICI_META_CONFIG")
    worker_config=$(resolve_path "$TICI_WORKER_CONFIG")
    mkdir -p "$SCRIPT_DIR/logs" "$(dirname "$meta_log_file")" "$(dirname "$worker_log_file")"

    if [ ! -f "$meta_config" ]; then
        echo "tici meta config not found: $meta_config" >&2
        exit 1
    fi
    if [ ! -f "$worker_config" ]; then
        echo "tici worker config not found: $worker_config" >&2
        exit 1
    fi

    local pd_addr="${PD_ADDR:-127.0.0.1:2379}"
    local meta_host="127.0.0.1"
    local meta_port="${TICI_META_PORT:-8500}"
    local meta_status_port="${TICI_META_STATUS_PORT:-8501}"
    local worker_host="127.0.0.1"
    local worker_port="${TICI_WORKER_PORT:-8510}"
    local worker_status_port="${TICI_WORKER_STATUS_PORT:-8511}"
    if [ -z "$pd_addr" ]; then
        echo "PD_ADDR is empty; cannot start tici meta/worker" >&2
        exit 1
    fi

    echo "Starting tici meta..."
    "$tici_bin" meta --config "$meta_config" \
        --host "$meta_host" \
        --port "$meta_port" \
        --status-port "$meta_status_port" \
        --advertise-host "$meta_host" \
        --pd-addr "$pd_addr" \
        $TICI_META_ARGS > "$meta_log_file" 2>&1 &
    TICI_META_PID=$!
    register_pid $TICI_META_PID
    echo "tici-meta(PID: $TICI_META_PID) started"

    echo "Starting tici worker..."
    "$tici_bin" worker --config "$worker_config" \
        --host "$worker_host" \
        --port "$worker_port" \
        --status-port "$worker_status_port" \
        --advertise-host "$worker_host" \
        --pd-addr "$pd_addr" \
        $TICI_WORKER_ARGS > "$worker_log_file" 2>&1 &
    TICI_WORKER_PID=$!
    register_pid $TICI_WORKER_PID
    echo "tici-worker(PID: $TICI_WORKER_PID) started"
}

function start_minio() {
    local minio_bin
    local mc_bin

    minio_bin=$(resolve_bin "$MINIO_BIN") || {
        echo "minio binary not found: $MINIO_BIN" >&2
        exit 1
    }
    mc_bin=$(resolve_bin "$MINIO_MC_BIN" || true)

    echo "Starting MinIO server..."
    mkdir -p "$MINIO_DATA_DIR/$MINIO_BUCKET"
    export MINIO_ROOT_USER="$MINIO_ACCESS_KEY"
    export MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY"
    "$minio_bin" server "$MINIO_DATA_DIR" --address "127.0.0.1:$MINIO_PORT" > "$MINIO_LOG_FILE" 2>&1 &
    MINIO_PID=$!
    register_pid $MINIO_PID

    for i in {1..10}; do
        if (exec 3<>"/dev/tcp/127.0.0.1/$MINIO_PORT") 2>/dev/null; then
            exec 3>&-
            echo "MinIO is up (port $MINIO_PORT reachable)"
            if [ -n "$mc_bin" ]; then
                if "$mc_bin" alias set localminio "http://127.0.0.1:$MINIO_PORT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"; then
                    "$mc_bin" mb --ignore-existing "localminio/$MINIO_BUCKET"
                else
                    echo "mc alias set failed; continuing without bucket creation"
                fi
            else
                echo "mc not found; skipping bucket creation"
            fi
            return 0
        fi
        sleep 1
    done

    echo "MinIO failed to start; tailing log:"
    tail -n 50 "$MINIO_LOG_FILE" || true
    return 1
}

function start_ticdc_server() {
    pd_client_addr=${1:-http://127.0.0.1:2379}
    ticdc_port=${2:-8300}
    data_dir=${3:-$TICDC_DATA_DIR}
    log_file=${4:-$TICDC_LOG_FILE}
    use_newarch=${5:-0}
    local newarch_flag=""

    if [ "$use_newarch" -ne 0 ]; then
        newarch_flag="--newarch=true"
    fi

    echo "Starting TiCDC server..."
    mkdir -p "$data_dir"
    $TICDC_BIN server --pd=$pd_client_addr --addr=127.0.0.1:$ticdc_port --data-dir=$data_dir $newarch_flag --log-file=$log_file &
    TICDC_PID=$!
    register_pid $TICDC_PID
    sleep 5  # Wait for TiCDC to connect
}

function create_ticdc_changefeed() {
    ticdc_port=${1:-8300}
    sink_uri=${2:-}
    changefeed_id=${3:-"simple-replication-task"}

    if [ -z "$sink_uri" ]; then
        echo "sink-uri is required for changefeed creation" >&2
        exit 1
    fi

    $TICDC_BIN cli changefeed create --server=127.0.0.1:$ticdc_port --sink-uri="$sink_uri" --changefeed-id="$changefeed_id"
    sleep 5  # Wait for changefeed to connect
}

function start_tidb_cluster()
{
    local start_downstream=${1:-1}

    alloc_port
    UP_PD_CLIENT_PORT=$ALLOCATED_PORT
    alloc_port
    UP_PD_PEER_PORT=$ALLOCATED_PORT
    upstream_pd_client_port=$UP_PD_CLIENT_PORT
	start_pd_server $UP_PD_CLIENT_PORT $UP_PD_PEER_PORT $PD_DATA_DIR $PD_LOG_FILE

	alloc_port
	UP_TIKV_PORT=$ALLOCATED_PORT
	alloc_port
	UP_TIKV_STATUS_PORT=$ALLOCATED_PORT
	start_tikv_server $UP_PD_CLIENT_PORT $UP_TIKV_PORT $UP_TIKV_STATUS_PORT $TIKV_DATA_DIR $TIKV_LOG_FILE

    if [ -n "${TIDB_PORT:-}" ]; then
        if port_in_use "$TIDB_PORT"; then
            echo "TIDB_PORT ${TIDB_PORT} is in use; selecting another port"
            alloc_port
            tidb_port=$ALLOCATED_PORT
        else
            tidb_port="${TIDB_PORT}"
            if [ "$tidb_port" -ge "$NEXT_PORT" ]; then
                NEXT_PORT=$((tidb_port + 1))
            fi
        fi
    else
        alloc_port
        tidb_port=$ALLOCATED_PORT
    fi
    export TIDB_PORT="$tidb_port"
    UPSTREAM_PORT=$tidb_port
	alloc_port
	UP_TIDB_STATUS_PORT=$ALLOCATED_PORT
	start_tidb_server "127.0.0.1:$UP_PD_CLIENT_PORT" $UPSTREAM_PORT $UP_TIDB_STATUS_PORT $TIDB_LOG_FILE

    echo "Ports (upstream): PD=$UP_PD_CLIENT_PORT/$UP_PD_PEER_PORT TiKV=$UP_TIKV_PORT/$UP_TIKV_STATUS_PORT TiDB=$UPSTREAM_PORT/$UP_TIDB_STATUS_PORT"

    if [ "$start_downstream" -ne 0 ]; then
        alloc_port
        DOWN_PD_CLIENT_PORT=$ALLOCATED_PORT
        alloc_port
        DOWN_PD_PEER_PORT=$ALLOCATED_PORT
        start_pd_server $DOWN_PD_CLIENT_PORT $DOWN_PD_PEER_PORT $PD_DATA_DIR2 $PD_LOG_FILE2

        alloc_port
        DOWN_TIKV_PORT=$ALLOCATED_PORT
        alloc_port
        DOWN_TIKV_STATUS_PORT=$ALLOCATED_PORT
        start_tikv_server $DOWN_PD_CLIENT_PORT $DOWN_TIKV_PORT $DOWN_TIKV_STATUS_PORT $TIKV_DATA_DIR2 $TIKV_LOG_FILE2

        alloc_port
        DOWNSTREAM_PORT=$ALLOCATED_PORT
        alloc_port
        DOWN_TIDB_STATUS_PORT=$ALLOCATED_PORT
        start_tidb_server "127.0.0.1:$DOWN_PD_CLIENT_PORT" $DOWNSTREAM_PORT $DOWN_TIDB_STATUS_PORT $TIDB_LOG_FILE2

        echo "Ports (downstream): PD=$DOWN_PD_CLIENT_PORT/$DOWN_PD_PEER_PORT TiKV=$DOWN_TIKV_PORT/$DOWN_TIKV_STATUS_PORT TiDB=$DOWNSTREAM_PORT/$DOWN_TIDB_STATUS_PORT"
    else
        DOWNSTREAM_PORT=""
    fi
    echo "TiDB cluster started successfully!"
}

function run_mysql_tester()
{
    local downstream_args=()

    if [ -n "${DOWNSTREAM_PORT:-}" ]; then
        downstream_args=(-downstream "root:@tcp(127.0.0.1:$DOWNSTREAM_PORT)/test")
    fi

    if [ $record -eq 1 ]; then
        echo "run & record integration test cases: $tests"
        $mysql_tester \
          -port "$UPSTREAM_PORT" \
          "${downstream_args[@]}" \
          --check-error=true \
          --path-dumpling="./third_bin/dumpling" \
          --record $@
    else
        echo "run integration test cases: $tests"
        $mysql_tester \
          -port "$UPSTREAM_PORT" \
          "${downstream_args[@]}" \
          --check-error=true \
          --path-dumpling="./third_bin/dumpling" \
          $@
    fi
}

if [ $record -eq 1 ]; then
    if [ "$record_case" = 'all' ]; then
        tests=''
    else
        tests=$record_case
    fi
fi

non_ticdc_cases=()
ticdc_cases=()
tici_cases=()

if [ -z "$tests" ]; then
    while IFS= read -r file; do
        file_name=$(basename "$file" .test)
        if [[ "$file" == t/ticdc/* ]]; then
            ticdc_cases+=("ticdc/$file_name")
        elif [[ "$file" == t/tici/* ]]; then
            tici_cases+=("tici/$file_name")
        else
            non_ticdc_cases+=("$file_name")
        fi
    done < <(find t -name "*.test")
else
    read -ra test_list <<< "$tests"
    for test_name in "${test_list[@]}"; do
        case "$test_name" in
            ticdc/*)
                ticdc_cases+=("$test_name")
                ;;
            tici/*)
                tici_cases+=("$test_name")
                ;;
            *)
                non_ticdc_cases+=("$test_name")
                ;;
        esac
    done
fi

need_downstream=0
if [ ${#ticdc_cases[@]} -ne 0 ]; then
    need_downstream=1
fi

start_tidb_cluster "$need_downstream"
wait_for_port_ready "127.0.0.1" "$UP_TIDB_STATUS_PORT" "TiDB upstream status" "$TIDB_LOG_FILE" 120 || exit 1
if [ "$need_downstream" -ne 0 ]; then
    wait_for_port_ready "127.0.0.1" "$DOWN_TIDB_STATUS_PORT" "TiDB downstream status" "$TIDB_LOG_FILE2" 120 || exit 1
fi

if [ ${#non_ticdc_cases[@]} -ne 0 ]; then
    run_mysql_tester "${non_ticdc_cases[@]}"
fi

if [ ${#tici_cases[@]} -ne 0 ]; then
    ensure_minio_port
    prepare_tici_config
    start_minio
    start_tici_server
    start_tiflash_server
fi

if [ ${#ticdc_cases[@]} -ne 0 ] || [ ${#tici_cases[@]} -ne 0 ]; then
    alloc_port
    ticdc_port=$ALLOCATED_PORT
    use_newarch=0
    if [ ${#tici_cases[@]} -ne 0 ]; then
        use_newarch=1
    fi
    start_ticdc_server "http://127.0.0.1:$upstream_pd_client_port" $ticdc_port "$TICDC_DATA_DIR" "$TICDC_LOG_FILE" "$use_newarch"

    echo "TiCDC started successfully!"
    if [ ${#ticdc_cases[@]} -ne 0 ]; then
        create_ticdc_changefeed $ticdc_port "mysql://root:@127.0.0.1:$DOWNSTREAM_PORT/" "simple-replication-task"
    fi
    if [ ${#tici_cases[@]} -ne 0 ]; then
        create_ticdc_changefeed $ticdc_port "$TICDC_S3_SINK_URI" "tici-replication-task"
    fi
fi

if [ ${#ticdc_cases[@]} -ne 0 ]; then
    run_mysql_tester "${ticdc_cases[@]}"
fi

if [ ${#tici_cases[@]} -ne 0 ]; then
    run_mysql_tester "${tici_cases[@]}"
fi

kill -15 $SERVER_PID
while ps -p $SERVER_PID > /dev/null; do
    sleep 1
done

echo "integrationtest passed!"
