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
NEXT_GEN=$NEXT_GEN

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
runs_on_port=0
diagnostic_mode=0
diagnostic_test_case="ddl/diagnostic_mode"
run_diagnostic_after_regular=0
diagnostic_store_path=""
start_in_diagnostic_mode=0
SERVER_PID=""

set -eu
function cleanup()
{
    set +e
    PIDS=$(jobs -p)
    for pid in $PIDS; do
        kill -9 "$pid" 2>/dev/null || true
    done
    if [[ -n "$diagnostic_store_path" && -d "$diagnostic_store_path" ]]; then
        rm -rf -- "$diagnostic_store_path"
    fi
}
trap cleanup EXIT
# make tests stable time zone wise
export TZ="Asia/Shanghai"

function help_message()
{
    echo "Usage: $0 [options]

    -h: Print this help message.

    -d <y|Y|n|N|b|B>: \"y\" or \"Y\" for only enabling the new collation during test.
                      \"n\" or \"N\" for only disabling the new collation during test.
                      \"b\" or \"B\" for tests the prefix is 'collation', enabling and disabling new collation during test, and for other tests, only enabling the new collation [default].
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

    -P <port>: Use tidb-server running on <port> for testing.

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
        if ! lsof -nP -i :"$port" &> /dev/null; then
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
    tidb_server="$(pwd)/integrationtest_tidb-server"
    echo "building tidb-server binary: $tidb_server"
    rm -rf $tidb_server
    if [ "${TIDB_TEST_STORE_NAME}" = "tikv" ]; then
        make -C ../.. server SERVER_OUT=$tidb_server
    else
        make -C ../.. server SERVER_OUT=$tidb_server RACE_FLAG="-race"
    fi
}

function build_mysql_tester()
{
    echo "building mysql-tester binary: $mysql_tester"
    rm -rf $mysql_tester
    GOBIN=$PWD go install github.com/pingcap/mysql-tester/src@f2d90ea9522d30c9a8e8d70cc31c7f016ca2801f
    mv src mysql_tester
}

function extract_stats()
{
    echo "extracting statistics: $stats"
    rm -rf $stats
    unzip -qq s.zip
}

while getopts "t:s:r:b:d:c:i:P:h" opt; do
    case $opt in
        P)
            runs_on_port="$OPTARG"
            port="$OPTARG"
            build=0
            ;;
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

selected_case="$tests"
if [[ $record = 1 ]]; then
    selected_case="$record_case"
fi

if [[ -z "$selected_case" || "$selected_case" = "all" ]]; then
    if [[ "${TIDB_TEST_STORE_NAME}" != "tikv" ]]; then
        run_diagnostic_after_regular=1
    fi
elif [[ "$selected_case" = "$diagnostic_test_case" ]]; then
    diagnostic_mode=1
fi

if [[ $run_diagnostic_after_regular = 1 && "$runs_on_port" -ne 0 ]]; then
    echo "Error: running all integration tests with -P cannot start the additional diagnostic-mode TiDB." >&2
    echo "Run without -P so the runner can start diagnostic TiDB and check its startup log." >&2
    exit 1
fi

if [[ $diagnostic_mode = 1 && "$runs_on_port" -ne 0 ]]; then
    echo "Error: $diagnostic_test_case does not support -P because its startup log must be checked." >&2
    exit 1
fi

extract_stats

if [ $build -eq 1 ]; then
    if [ -z "$tidb_server" ]; then
        build_tidb_server
    else
        echo "skip building tidb-server, using existing binary: $tidb_server"
    fi
    if [[ $diagnostic_mode = 0 ]]; then
        build_mysql_tester
    fi
else
    if [ -z "$tidb_server" ] && [ "$runs_on_port" -eq 0 ]; then
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

if [ "$runs_on_port" -eq 0 ]
then
    ports=($(find_multiple_available_ports 4000 2))
    port=6999
    status=${ports[1]}
    if [[ $diagnostic_mode = 1 || $run_diagnostic_after_regular = 1 ]]; then
        if [[ "${TIDB_TEST_STORE_NAME}" = "tikv" ]]; then
            echo "Error: the diagnostic integrationtest runner requires a persistent UniStore." >&2
            exit 1
        fi
        diagnostic_store_path=$(mktemp -d "${TMPDIR:-/tmp}/tidb-integrationtest-diagnostic.XXXXXX")
    fi
fi

function start_tidb_server()
{
    config_file="config.toml"
    if [[ $enabled_new_collation = 0 ]]; then
        config_file="disable_new_collation.toml"
    fi

    start_options="-P $port -status $status -config $config_file"
    if [ "${TIDB_TEST_STORE_NAME}" = "tikv" ]; then
        start_options="$start_options -store tikv -path ${TIKV_PATH}"
    elif [[ $diagnostic_mode = 1 ]]; then
        start_options="$start_options -store unistore -path $diagnostic_store_path"
    else
        start_options="$start_options -store unistore -path ''"
    fi

    if [[ $start_in_diagnostic_mode = 1 ]]; then
        start_options="$start_options --diagnostic-mode"
    fi

    if [ -n "$NEXT_GEN" ] && [ "$NEXT_GEN" != "0" ] && [ "$NEXT_GEN" != "false" ]; then
        start_options="$start_options -keyspace-name SYSTEM --tidb-service-scope dxf_service"
    fi

    echo "start tidb-server, log file: $mysql_tester_log"
    $tidb_server -V
    $tidb_server $start_options > $mysql_tester_log 2>&1 &
    SERVER_PID=$!
    echo "tidb-server(PID: $SERVER_PID) started"
}

function wait_for_tidb_server()
{
    local status_url="http://127.0.0.1:${status}/status"
    for _ in $(seq 1 120); do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "tidb-server exited before becoming ready. Log tail:" >&2
            tail -n 100 "$mysql_tester_log" >&2 || true
            return 1
        fi
        if curl -sf --max-time 2 "$status_url" >/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "timed out waiting for tidb-server. Log tail:" >&2
    tail -n 100 "$mysql_tester_log" >&2 || true
    return 1
}

function stop_tidb_server()
{
    kill -15 "$SERVER_PID"
    wait "$SERVER_PID" || true
    SERVER_PID=""
}

function bootstrap_diagnostic_store()
{
    echo "bootstrap storage before diagnostic-mode startup"
    start_in_diagnostic_mode=0
    start_tidb_server
    wait_for_tidb_server
    stop_tidb_server
    start_in_diagnostic_mode=1
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
          "$mysql_tester" -port "$port" --check-error=true --collation-disable="$coll_disabled" --record
      else
          echo "record result for case: \"$record_case\""
          "$mysql_tester" -port "$port" --check-error=true --collation-disable="$coll_disabled" --record "$record_case"
      fi
    else
      if [ -z "$tests" ]; then
          echo "run all integration test cases ($coll_msg)"
      else
          echo "run integration test cases($coll_msg): $tests"
      fi
      "$mysql_tester" -port "$port" --check-error=true --collation-disable="$coll_disabled" $tests
    fi
}

function run_diagnostic_tester()
{
    go run ./diagnostictest \
        -port "$port" \
        -test "diagnostictest/testdata/diagnostic_mode.test" \
        -result "diagnostictest/testdata/diagnostic_mode.result" \
        -record="$record"

    # Starting diagnostic TiDB truncates this file, excluding bootstrap logs.
    if [[ ! -f "$mysql_tester_log" || ! -r "$mysql_tester_log" || ! -s "$mysql_tester_log" ]]; then
        echo "Error: diagnostic TiDB log is missing, unreadable or empty: $mysql_tester_log" >&2
        return 1
    fi
    local grep_status
    if grep -nF 'start DDL' "$mysql_tester_log"; then
        echo "Error: diagnostic TiDB must not start DDL, but its log contains 'start DDL': $mysql_tester_log" >&2
        return 1
    else
        grep_status=$?
        if [[ $grep_status -ne 1 ]]; then
            echo "Error: failed to read diagnostic TiDB log: $mysql_tester_log" >&2
            return 1
        fi
    fi
}

function run_tester()
{
    if [[ $diagnostic_mode = 1 ]]; then
        run_diagnostic_tester
    else
        run_mysql_tester
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

    if [[ -z "$selected_case" || "$selected_case" = "all" ]]; then
        return
    fi

    IFS='/' read -ra parts <<< "$selected_case"

    last_part="${parts[${#parts[@]}-1]}"

    if [[ $last_part == collation* || $selected_case == collation* ]]; then
        collation_opt=2
    else
        collation_opt=1
    fi
}

check_case_name
if [[ $collation_opt = 0 || $collation_opt = 2 ]]; then
    enabled_new_collation=0
    if [ "$runs_on_port" -eq 0 ]
    then
        if [[ $diagnostic_mode = 1 ]]; then
            bootstrap_diagnostic_store
        fi
        start_tidb_server
        if [[ $diagnostic_mode = 1 ]]; then
            wait_for_tidb_server
        fi
    fi
    run_tester
    if [ "$runs_on_port" -eq 0 ]
    then
        stop_tidb_server
    fi
    check_data_race
fi

if [[ $collation_opt = 1 || $collation_opt = 2 ]]; then
    enabled_new_collation=1
    if [ "$runs_on_port" -eq 0 ]
    then
        if [[ $diagnostic_mode = 1 ]]; then
            bootstrap_diagnostic_store
        fi
        start_tidb_server
        if [[ $diagnostic_mode = 1 ]]; then
            wait_for_tidb_server
        fi
    fi
    run_tester
    if [ "$runs_on_port" -eq 0 ]
    then
        stop_tidb_server
    fi
    check_data_race
fi

if [[ $run_diagnostic_after_regular = 1 ]]; then
    diagnostic_mode=1
    bootstrap_diagnostic_store
    start_tidb_server
    wait_for_tidb_server
    run_diagnostic_tester
    stop_tidb_server
    check_data_race
fi

echo "integrationtest passed!"
