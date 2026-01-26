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
MINIO_MC_BIN_DEFAULT=""

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
trap 'set +e; PIDS=$(jobs -p); for pid in $PIDS; do kill -9 $pid 2>/dev/null || true; done' EXIT
# make tests stable time zone wise
export TZ="Asia/Shanghai"

TICI_BIN="${TICI_BIN:-$TICI_BIN_DEFAULT}"
MINIO_BIN="${MINIO_BIN:-$MINIO_BIN_DEFAULT}"
MINIO_MC_BIN="${MINIO_MC_BIN:-$MINIO_MC_BIN_DEFAULT}"
TIFLASH_BIN="${TIFLASH_BIN:-$TIFLASH_BIN_DEFAULT}"
TIKV_CONFIG="${TIKV_CONFIG:-}"
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
TICDC_S3_SINK_URI_DEFAULT="s3://${MINIO_BUCKET}/${MINIO_PREFIX}?endpoint=${MINIO_ENDPOINT}&access-key=${MINIO_ACCESS_KEY}&secret-access-key=${MINIO_SECRET_KEY}&provider=minio&protocol=canal-json&enable-tidb-extension=true&output-row-key=true"
TICDC_S3_SINK_URI="${TICDC_S3_SINK_URI:-$TICDC_S3_SINK_URI_DEFAULT}"
TICI_CONFIG_DIR="${TICI_CONFIG_DIR:-$SCRIPT_DIR/tici/config}"

if [ -d "$TIFLASH_BIN" ] && [ -x "$TIFLASH_BIN/tiflash" ]; then
    TIFLASH_BIN="$TIFLASH_BIN/tiflash"
fi
if [ ! -x "$TICDC_BIN" ] && [ -x "./third_bin/ticdc" ]; then
    TICDC_BIN="./third_bin/ticdc"
fi
if [ -z "$TIKV_CONFIG" ] && [ -f "$SCRIPT_DIR/tikv.toml" ]; then
    TIKV_CONFIG="$SCRIPT_DIR/tikv.toml"
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
      TIDB_PORT (fixed upstream port; must be free)

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
        if ! lsof -i :"$port" &> /dev/null; then
            echo $port
            return 0
        fi
        ((port++))
    done
}

# Function to find multiple available ports starting from a given port
function find_multiple_available_ports() {
    local start_port=$1
    local count=$2
    local ports=()

    while [ ${#ports[@]} -lt $count ]; do
        local available_port=$(find_available_port $start_port)
        if [ $? -eq 0 ]; then
            ports+=($available_port)
            ((start_port = available_port + 1))
        else
            echo "Error: Could not find an available port." >&2
            exit 1
        fi
    done

    echo "${ports[@]}"
}

function resolve_bin() {
    local bin="$1"

    if [[ -x "$bin" ]]; then
        echo "$bin"
        return 0
    fi
    if command -v "$bin" >/dev/null 2>&1; then
        command -v "$bin"
        return 0
    fi

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

	if [ -n "$TIKV_CONFIG" ]; then
        $TIKV_BIN --config="$TIKV_CONFIG" \
            --pd="http://127.0.0.1:$pd_client_port" \
            --addr="127.0.0.1:$tikv_port" \
            --advertise-addr="127.0.0.1:$tikv_port" \
            --status-addr="127.0.0.1:$tikv_status_port" \
            --data-dir="$data_dir" \
            --log-file="$log_dir" &
    else
        $TIKV_BIN --pd="http://127.0.0.1:$pd_client_port" \
            --addr="127.0.0.1:$tikv_port" \
            --advertise-addr="127.0.0.1:$tikv_port" \
            --status-addr="127.0.0.1:$tikv_status_port" \
            --data-dir="$data_dir" \
            --log-file="$log_dir" &
    fi
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
        $tiflash_bin --data-dir=$TIFLASH_DATA_DIR --log-file=tiflash.log --config-file="$TIFLASH_CONFIG" &
    else
        $tiflash_bin --data-dir=$TIFLASH_DATA_DIR --log-file=tiflash.log &
    fi

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
        if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
            exec 3>&-
            echo "$label is reachable at $host:$port"
            return 0
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
        if [ -z "${S3_USE_PATH_STYLE:-}" ]; then export S3_USE_PATH_STYLE=true; fi
        if [ -z "${S3_ENDPOINT:-}" ]; then export S3_ENDPOINT="http://127.0.0.1:9000"; fi
        if [ -z "${S3_REGION:-}" ]; then export S3_REGION="us-east-1"; fi
        if [ -z "${S3_ACCESS_KEY:-}" ]; then export S3_ACCESS_KEY="minioadmin"; fi
        if [ -z "${S3_SECRET_KEY:-}" ]; then export S3_SECRET_KEY="minioadmin"; fi
        if [ -z "${S3_BUCKET:-}" ]; then export S3_BUCKET="ticidefaultbucket"; fi
        if [ -z "${S3_PREFIX:-}" ]; then export S3_PREFIX="tici_default_prefix"; fi
        if [ -z "${TIDB_PORT:-}" ]; then export TIDB_PORT="4000"; fi
        if [ -n "${upstream_pd_client_port:-}" ]; then
            export PD_ADDR="127.0.0.1:$upstream_pd_client_port"
        fi
        export PD_ADDR="${PD_ADDR:-127.0.0.1:2379}"
        if [ -z "$PD_ADDR" ]; then
            echo "PD_ADDR is empty; cannot start tici meta/worker" >&2
            exit 1
        fi
        if command -v envsubst >/dev/null 2>&1; then
            if [ -f "$TICI_CONFIG_DIR/meta.toml.in" ]; then
                envsubst < "$TICI_CONFIG_DIR/meta.toml.in" > "$TICI_CONFIG_DIR/meta.toml"
                TICI_META_CONFIG="${TICI_META_CONFIG:-$TICI_CONFIG_DIR/meta.toml}"
            fi
            if [ -f "$TICI_CONFIG_DIR/worker.toml.in" ]; then
                envsubst < "$TICI_CONFIG_DIR/worker.toml.in" > "$TICI_CONFIG_DIR/worker.toml"
                TICI_WORKER_CONFIG="${TICI_WORKER_CONFIG:-$TICI_CONFIG_DIR/worker.toml}"
            fi
            if [ -f "$TICI_CONFIG_DIR/tiflash.toml.in" ]; then
                envsubst < "$TICI_CONFIG_DIR/tiflash.toml.in" > "$TICI_CONFIG_DIR/tiflash.toml"
                TIFLASH_CONFIG="${TIFLASH_CONFIG:-$TICI_CONFIG_DIR/tiflash.toml}"
            fi
        else
            echo "envsubst not found; cannot render TiCI configs" >&2
            exit 1
        fi
    fi
}

function start_tici_server() {
    log_file=${1:-$TICI_LOG_FILE}
    local tici_bin

    tici_bin=$(resolve_bin "$TICI_BIN") || {
        echo "tici binary not found: $TICI_BIN" >&2
        exit 1
    }

    if [ -z "$TICI_META_CONFIG" ] || [ -z "$TICI_WORKER_CONFIG" ]; then
        echo "tici meta/worker config is required: TICI_META_CONFIG, TICI_WORKER_CONFIG" >&2
        exit 1
    fi

    local pd_addr="${PD_ADDR:-127.0.0.1:2379}"
    local meta_host="${TICI_META_HOST:-127.0.0.1}"
    local meta_port="${TICI_META_PORT:-8500}"
    local meta_status_port="${TICI_META_STATUS_PORT:-8501}"
    local worker_host="${TICI_WORKER_HOST:-127.0.0.1}"
    local worker_port="${TICI_WORKER_PORT:-8510}"
    local worker_status_port="${TICI_WORKER_STATUS_PORT:-8511}"
    if [ -z "$pd_addr" ]; then
        echo "PD_ADDR is empty; cannot start tici meta/worker" >&2
        exit 1
    fi

    echo "Starting tici meta..."
    $tici_bin meta --config "$TICI_META_CONFIG" \
        --host "$meta_host" \
        --port "$meta_port" \
        --status-port "$meta_status_port" \
        --advertise-host "$meta_host" \
        --pd-addr "$pd_addr" \
        $TICI_META_ARGS > "$log_file" 2>&1 &
    TICI_META_PID=$!
    echo "tici-meta(PID: $TICI_META_PID) started"

    echo "Starting tici worker..."
    $tici_bin worker --config "$TICI_WORKER_CONFIG" \
        --host "$worker_host" \
        --port "$worker_port" \
        --status-port "$worker_status_port" \
        --advertise-host "$worker_host" \
        --pd-addr "$pd_addr" \
        $TICI_WORKER_ARGS >> "$log_file" 2>&1 &
    TICI_WORKER_PID=$!
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

    if lsof -i ":$MINIO_PORT" &> /dev/null; then
        echo "MinIO port $MINIO_PORT already in use; assuming MinIO is running"
        return 0
    fi

    echo "Starting MinIO server..."
    mkdir -p "$MINIO_DATA_DIR/$MINIO_BUCKET"
    export MINIO_ROOT_USER="$MINIO_ACCESS_KEY"
    export MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY"
    "$minio_bin" server "$MINIO_DATA_DIR" --address ":$MINIO_PORT" > "$MINIO_LOG_FILE" 2>&1 &
    MINIO_PID=$!

    for i in {1..60}; do
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

    echo "Starting TiCDC server..."
    mkdir -p "$data_dir"
    $TICDC_BIN server --pd=$pd_client_addr --addr=127.0.0.1:$ticdc_port --data-dir=$data_dir --log-file=$log_file &
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
    local ports=($(find_multiple_available_ports 2379 2))
    if [ $? -ne 0 ]; then
        echo "Error: Could not find multiple available ports." >&2
        exit 1
    fi
    pd_client_port=${ports[0]}
	pd_peer_port=${ports[1]}
    upstream_pd_client_port=$pd_client_port
	start_pd_server $pd_client_port $pd_peer_port $PD_DATA_DIR $PD_LOG_FILE

    local ports=($(find_multiple_available_ports 20160 2))
    if [ $? -ne 0 ]; then
        echo "Error: Could not find multiple available ports." >&2
        exit 1
    fi
	tikv_port=${ports[0]}
	tikv_status_port=${ports[1]}
	start_tikv_server $pd_client_port $tikv_port $tikv_status_port $TIKV_DATA_DIR $TIKV_LOG_FILE

    if [ -n "${TIDB_PORT:-}" ]; then
        if lsof -i :"${TIDB_PORT}" &> /dev/null; then
            echo "Error: TIDB_PORT ${TIDB_PORT} is in use." >&2
            exit 1
        fi
        tidb_port="${TIDB_PORT}"
    else
        tidb_port=$(find_available_port 4000)
    fi
    UPSTREAM_PORT=$tidb_port
	tidb_status_port=$(find_available_port 10080)
	start_tidb_server "127.0.0.1:$pd_client_port" $tidb_port $tidb_status_port $TIDB_LOG_FILE

    local ports=($(find_multiple_available_ports 2379 2))
    if [ $? -ne 0 ]; then
        echo "Error: Could not find multiple available ports." >&2
        exit 1
    fi
    pd_client_port=${ports[0]}
	pd_peer_port=${ports[1]}
    start_pd_server $pd_client_port $pd_peer_port $PD_DATA_DIR2 $PD_LOG_FILE2

    local ports=($(find_multiple_available_ports 20160 2))
    if [ $? -ne 0 ]; then
        echo "Error: Could not find multiple available ports." >&2
        exit 1
    fi
	tikv_port=${ports[0]}
	tikv_status_port=${ports[1]}
    start_tikv_server $pd_client_port $tikv_port $tikv_status_port $TIKV_DATA_DIR2 $TIKV_LOG_FILE2

    tidb_port=$(find_available_port 4000)
    DOWNSTREAM_PORT=$tidb_port
	tidb_status_port=$(find_available_port 10080)
    start_tidb_server "127.0.0.1:$pd_client_port" $tidb_port $tidb_status_port $TIDB_LOG_FILE2

    echo "TiDB cluster started successfully!"
}

function run_mysql_tester()
{
    if [ $record -eq 1 ]; then
        echo "run & record integration test cases: $tests"
        $mysql_tester \
          -port "$UPSTREAM_PORT" \
          -downstream "root:@tcp(127.0.0.1:$DOWNSTREAM_PORT)/test" \
          --check-error=true \
          --path-dumpling="./third_bin/dumpling" \
          --record $@
    else
        echo "run integration test cases: $tests"
        $mysql_tester \
          -port "$UPSTREAM_PORT" \
          -downstream "root:@tcp(127.0.0.1:$DOWNSTREAM_PORT)/test" \
          --check-error=true \
          --path-dumpling="./third_bin/dumpling" \
          $@
    fi
}

start_tidb_cluster
wait_for_port_ready "127.0.0.1" "$UPSTREAM_PORT" "TiDB upstream" "$TIDB_LOG_FILE" 120 || exit 1
wait_for_port_ready "127.0.0.1" "$DOWNSTREAM_PORT" "TiDB downstream" "$TIDB_LOG_FILE2" 120 || exit 1

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

if [ ${#non_ticdc_cases[@]} -ne 0 ]; then
    run_mysql_tester "${non_ticdc_cases[@]}"
fi

if [ ${#tici_cases[@]} -ne 0 ]; then
    prepare_tici_config
    start_minio
    start_tiflash_server
    start_tici_server
fi

if [ ${#ticdc_cases[@]} -ne 0 ] || [ ${#tici_cases[@]} -ne 0 ]; then
    ticdc_port=$(find_available_port 8300)
    start_ticdc_server "http://127.0.0.1:$upstream_pd_client_port" $ticdc_port

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
