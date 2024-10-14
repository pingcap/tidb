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

set -x

# Variables to set paths for each server binary
TIDB_BIN="./third_bin/tidb-server"
TIKV_BIN="./third_bin/tikv-server"
PD_BIN="./third_bin/pd-server"
DUMPLING_BIN="./third_bin/dumpling"
TICDC_BIN="./third_bin/cdc"
TIFLASH_BIN="./third_bin/tiflash-server"

# Variables to set data directories
PD_DATA_DIR="./data/pd_data"
PD_DATA_DIR2="./data/pd_data2"
TIKV_DATA_DIR="./data/tikv_data"
TIKV_DATA_DIR2="./data/tikv_data2"
TIFLASH_DATA_DIR="./data/tiflash_data"
TIFLASH_DATA_DIR2="./data/tiflash_data2"
TICDC_DATA_DIR="./data/ticdc_data"

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
trap 'set +e; PIDS=$(jobs -p); [ -n "$PIDS" ] && kill -9 $PIDS' EXIT
# make tests stable time zone wise
export TZ="Asia/Shanghai"

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
    GOBIN=$PWD go install github.com/bb7133/mysql-tester/src@67e4fbf902623a98a859fb3d1bd39088bec8da6f
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

	$TIKV_BIN --pd="http://127.0.0.1:$pd_client_port" \
        --addr="127.0.0.1:$tikv_port" \
        --advertise-addr="127.0.0.1:$tikv_port" \
        --status-addr="127.0.0.1:$tikv_status_port" \
		--data-dir="$data_dir" \
		--log-file="$log_dir" &
    sleep 5  # Wait for TiKV to connect to PD
}

# TiFlash Server Configuration
start_tiflash_server() {
    echo "Starting TiFlash server..."
    mkdir -p $TIFLASH_DATA_DIR
    $TIFLASH_BIN --data-dir=$TIFLASH_DATA_DIR --log-file=tiflash.log &

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

function start_ticdc_server() {
    pd_client_addr=${1:-http://127.0.0.1:2379}
    ticdc_port=${2:-8300}
    downstream_port=${3:-4100}

    echo "Starting TiCDC server..."
    mkdir -p $TICDC_DATA_DIR
    $TICDC_BIN server --pd=$pd_client_addr --addr=127.0.0.1:$ticdc_port --data-dir=$TICDC_DATA_DIR --log-file=$TICDC_LOG_FILE &
    sleep 5  # Wait for TiCDC to connect

    $TICDC_BIN cli changefeed create --server=127.0.0.1:$ticdc_port --sink-uri="mysql://root:@127.0.0.1:$downstream_port/" --changefeed-id="simple-replication-task"
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

    tidb_port=$(find_available_port 4000)
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

if [ $record -eq 1 ]; then
    if [ "$record_case" = 'all' ]; then
        tests=''
    else
        tests=$record_case
    fi
fi

non_ticdc_cases=()
ticdc_cases=()

if [ -z "$tests" ]; then
    while IFS= read -r file; do
        file_name=$(basename "$file" .test)
        if [[ "$file" == t/ticdc/* ]]; then
            ticdc_cases+=("ticdc/$file_name")
        else
            non_ticdc_cases+=("$file_name")
        fi
    done < <(find t -name "*.test")
else
    ticdc_cases=($(echo "$tests" | grep 'ticdc/' || echo ""))
    non_ticdc_cases=($(echo "$tests" | grep -v 'ticdc/' || echo ""))
fi

if [ ${#non_ticdc_cases[@]} -ne 0 ]; then
    run_mysql_tester "${non_ticdc_cases[@]}"
fi

if [ ${#ticdc_cases[@]} -ne 0 ]; then
    ticdc_port=$(find_available_port 8300)
    start_ticdc_server "http://127.0.0.1:$upstream_pd_client_port" $ticdc_port $tidb_port

    echo "TiCDC started successfully!"

    run_mysql_tester "${ticdc_cases[@]}"
fi

kill -15 $SERVER_PID
while ps -p $SERVER_PID > /dev/null; do
    sleep 1
done

echo "integrationtest passed!"
