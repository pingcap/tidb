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

TIDB_TEST_STORE_NAME=$TIDB_TEST_STORE_NAME
TIKV_PATH=$TIKV_PATH

build=1
mysql_tester="./mysql_tester"
tidb_server=""
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
    if [[ -z $tidb_server || ! -e $tidb_server ]]; then
        echo -e "no tidb-server was found, use default binary path bin/tidbx-server"
        $tidb_server="bin/tidbx-server"
    fi

    if [ ! -x "$tidb_server" ]; then
      echo "ERROR: '$tidb_server' not found"
      exit 1
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

function start_tidb_server()
{
    config_file="config.toml"
    if [[ $enabled_new_collation = 0 ]]; then
        config_file="disable_new_collation.toml"
    fi

    rm -rf "$mysql_tester_log"
    rm -rf tidbx.log
    rm -rf ./data
    mkdir -p ./data
    touch tikv.toml pd.toml
    touch $mysql_tester_log

    # Disable pipelined pessimistic lock temporarily until tikv#11649 is resolved
    cat <<EOF > tikv.toml
[pessimistic-txn]
pipelined = false
EOF

    echo "start tidbx-server, log file: $mysql_tester_log"
    $tidb_server \
        --pd.name=pd-0 \
        --pd.client-urls=http://0.0.0.0:2379 \
        --pd.advertise-client-urls=http://127.0.0.1:2379 \
        --pd.peer-urls=http://0.0.0.0:2380 \
        --pd.advertise-peer-urls=http://127.0.0.1:2380 \
        --pd.data-dir=./data/pd0 \
        --pd.initial-cluster=pd-0=http://127.0.0.1:2380 \
        --pd.config=pd.toml \
        --pd.log-file=pd0.log \
        --tikv.addr=0.0.0.0:20160 \
        --tikv.advertise-addr=127.0.0.1:20160 \
        --tikv.status-addr=0.0.0.0:20180 \
        --tikv.advertise-status-addr=127.0.0.1:20180 \
        --tikv.pd=127.0.0.1:2379 \
        --tikv.data-dir=./data/tikv0 \
        --tikv.config=tikv.toml \
        --tikv.log-file=tikv0.log \
        --tidb.P=$port \
        --tidb.status=$status \
        --tidb.store=tikv \
        --tidb.initialize-insecure \
        --tidb.path=127.0.0.1:2379 \
        --tidb.config=${config_file} \
        --tidb.log-file=${mysql_tester_log} > tidbx.log 2>&1 &

    SERVER_PID=$!
    sleep 5
    echo "tidbx-server(PID: $SERVER_PID) started"
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
    start_tidb_server
    run_mysql_tester
    kill -15 $SERVER_PID
    while ps -p $SERVER_PID > /dev/null; do
        sleep 1
    done
    check_data_race
fi

if [[ $collation_opt = 1 || $collation_opt = 2 ]]; then
    enabled_new_collation=1
    start_tidb_server
    run_mysql_tester
    kill -15 $SERVER_PID
    while ps -p $SERVER_PID > /dev/null; do
        sleep 1
    done
    check_data_race
fi

echo "integrationtest passed!"
