#!/usr/bin/env bash
# Copyright 2019 PingCAP, Inc.
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

TIDB_TEST_STORE_NAME=$TIDB_TEST_STORE_NAME
TIKV_PATH=$TIKV_PATH
START_TIDB_CLUSTER=$START_TIDB_CLUSTER

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
collation_opt=2

set -eu
trap 'set +e; PIDS=$(jobs -p); [ -n "$PIDS" ] && kill -9 $PIDS' EXIT
# make tests stable time zone wise
export TZ="Asia/Shanghai"

function help_message()
{
    echo "Usage: $0 [options]

    -h: Print this help message.

    -d <y|Y|n|N|b|B>: \"y\" or \"Y\" for only enabling the new collation during test.
                      \"n\" or \"N\" for only disabling the new collation during test.
                      \"b\" or \"B\" for tests the prefix is `collation`, enabling and disabling new collation during test, and for other tests, only enabling the new collation [default].
                      Enable/Disable the new collation during the integration test.

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
    if [ "${TIDB_TEST_STORE_NAME}" = "tikv" ]; then
        GO111MODULE=on go build -o $tidb_server github.com/pingcap/tidb/cmd/tidb-server
    else
        GO111MODULE=on go build -race -o $tidb_server github.com/pingcap/tidb/cmd/tidb-server
    fi
}

function build_mysql_tester()
{
    echo "building mysql-tester binary: $mysql_tester"
    rm -rf $mysql_tester
    GOBIN=$PWD go install github.com/pingcap/mysql-tester/src@314107b26aa8fce86beb0dd48e75827fb269b365
    mv src mysql_tester
}

function extract_stats()
{
    echo "extracting statistics: $stats"
    rm -rf $stats
    unzip -qq s.zip
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
        d)
            case $OPTARG in
                y|Y)
                    collation_opt=1
                    ;;
                n|N)
                    collation_opt=0
                    ;;
                b|B)
                    collation_opt=2
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

extract_stats

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

rm -rf $mysql_tester_log

ports=($(find_multiple_available_ports 4000 2))
port=${ports[0]}
status=${ports[1]}
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
    if [[ $enabled_new_collation = 0 ]]; then
        config_file="disable_new_collation.toml"
    fi
    echo "start tidb-server, log file: $log_file"
    if [ "${TIDB_TEST_STORE_NAME}" = "tikv" ]; then
        $tidb_server -P "$tidb_port" \
            -status "$tidb_status_port" \
            -config $config_file \
            -store tikv \
            -path "${pd_client_addr}" > $log_file 2>&1 &
        SERVER_PID=$!
    else
        $tidb_server -P "$tidb_port" \
            -status "$tidb_status_port" \
            -config $config_file \
            -store unistore \
            -path "" > $log_file 2>&1 &
        SERVER_PID=$!
    fi
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
    # Tricky: update `port` here.
    port=$tidb_port
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
	tidb_status_port=$(find_available_port 10080)
    start_tidb_server "127.0.0.1:$pd_client_port" $tidb_port $tidb_status_port $TIDB_LOG_FILE2

    echo "TiDB cluster started successfully!"
    read -p "Press [Enter] key to go on..."

    ticdc_port=$(find_available_port 8300)
    start_ticdc_server "http://127.0.0.1:$upstream_pd_client_port" $ticdc_port $tidb_port

    echo "TiCDC started successfully!"
    read -p "Press [Enter] key to go on..."
}

function run_mysql_tester()
{
    coll_disabled="false"
    coll_msg="enabled new collation"
    if [[ $enabled_new_collation = 0 ]]; then
        coll_disabled="true"
        coll_msg="disabled new collation"
    fi
    if [ $record -eq 1 ]; then
      if [ "$record_case" = 'all' ]; then
          echo "record all cases"
          $mysql_tester -port "$port" --check-error=true --collation-disable=$coll_disabled --record
      else
          echo "record result for case: \"$record_case\""
          $mysql_tester -port "$port" --check-error=true --collation-disable=$coll_disabled --record $record_case
      fi
    else
      if [ -z "$tests" ]; then
          echo "run all integration test cases ($coll_msg)"
      else
          echo "run integration test cases($coll_msg): $tests"
      fi
      $mysql_tester -port "$port" --check-error=true --collation-disable=$coll_disabled $tests
    fi
}

function check_data_race() {
    if [ "${TIDB_TEST_STORE_NAME}" = "tikv" ]; then
        return
    fi
    race=`grep 'DATA RACE' $mysql_tester_log || true`
    if [ ! -z "$race" ]; then
        echo "tidb-server DATA RACE!"
        cat $mysql_tester_log
        exit 1
    fi
}

enabled_new_collation=""
function check_case_name() {
    if [ $collation_opt != 2 ]; then
        return
    fi

    case=""

    if [ $record -eq 0 ]; then
        if [ -z "$tests" ]; then
            return
        fi
        case=$tests
    fi

    if [ $record -eq 1 ]; then
        if [ "$record_case" = 'all' ]; then
            return
        fi
        case=$record_case
    fi

    IFS='/' read -ra parts <<< "$case"

    last_part="${parts[${#parts[@]}-1]}"

    if [[ $last_part == collation* || $tests == collation* ]]; then
        collation_opt=2
    else
        collation_opt=1
    fi
}

check_case_name
if [[ $collation_opt = 0 || $collation_opt = 2 ]]; then
    enabled_new_collation=0
else
    enabled_new_collation=1
fi

if [ -z "$START_TIDB_CLUSTER" ]; then
    start_tidb_server
else
    start_tidb_cluster
fi

run_mysql_tester
kill -15 $SERVER_PID
while ps -p $SERVER_PID > /dev/null; do
    sleep 1
done
check_data_race

echo "integrationtest passed!"
