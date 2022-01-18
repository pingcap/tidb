#!/usr/bin/env bash
# Copyright 2022 PingCAP, Inc.
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

echo -e "test start\n"

# `set -eu` prevent referencing an var before setting it, so we set the env vars here
TIKV_PATH=$TIKV_PATH

COLLATION_DISABLE=$COLLATION_DISABLE

disable_collation_config="disable_new_collation.toml"

mysql_test_log="./mysql-test.out"
explain_test="./explain_test"
stats="s"

function extract_stats()
{
    echo "extracting statistics: $stats"
    rm -rf $stats
    unzip -qq s.zip
}

function build_explain_test()
{
    echo "building explain-test binary: $explain_test"
    rm -rf $explain_test
    GO111MODULE=on go build -o $explain_test
}

if [[ $COLLATION_DISABLE -eq 1 ]];then
    echo -e "COLLATION_DISABLE"
    TIDB_CONFIG=$disable_collation_config
fi

if [[ -z "${TIDB_CONFIG}" || ! -e ${TIDB_CONFIG} ]]; then
    TIDB_CONFIG="./config.toml"
fi

set -eu

rm -rf "$mysql_test_log"

extract_stats
build_explain_test

if [[ -z $TIDB_SERVER_PATH || ! -e $TIDB_SERVER_PATH ]]; then
    echo -e "no tidb-server was found, build from source code"
    # This is likely to be failed, depending on whether tidb parser is well set up
    GO111MODULE=on go build -race -o mysql_test_tidb-server github.com/pingcap/tidb/tidb-server
    TIDB_SERVER_PATH=./mysql_test_tidb-server
fi

echo "start tidb-server, log file: $mysql_test_log"
"$TIDB_SERVER_PATH" -config "$TIDB_CONFIG" -P 4001 -status 9081 -store tikv -path "$TIKV_PATH" > "$mysql_test_log" 2>&1 &
SERVER_PID=$!
echo "tidb-server(PID: $SERVER_PID) started"
trap 'echo "tidb-server(PID: $SERVER_PID) stopped"; kill -9 "$SERVER_PID" || true' EXIT

sleep 5

echo "run all test cases"

if [[ $COLLATION_DISABLE -eq 1 ]];then
    $explain_test -port 4001 -status 9081 --log-level=error --collation-disable=true
else
    $explain_test -port 4001 -status 9081 --log-level=error
fi

race=$(grep 'DATA RACE' "$mysql_test_log" || true)
if [[ -n $race ]]; then
    echo "tidb-server DATA RACE!"
    cat "$mysql_test_log"
    exit 1
fi
echo "test end"
