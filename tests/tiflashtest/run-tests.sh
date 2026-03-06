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

mysql_tester="./mysql_tester"
mysql_tester_log="./integration-test.out"
tests=""
record=0
record_case=""
runs_on_port=0
port=4000
status=10080

set -eu
trap 'set +e; PIDS=$(jobs -p); for pid in $PIDS; do kill -9 $pid 2>/dev/null || true; done' EXIT
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

function build_mysql_tester()
{
    echo "building mysql-tester binary: $mysql_tester"
    rm -rf $mysql_tester
    GOBIN=$PWD go install github.com/pingcap/mysql-tester/src@latest
    mv src mysql_tester
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

build_mysql_tester

rm -rf $mysql_tester_log
if [ "$runs_on_port" -eq 0 ]
then
    ports=4000
    status=10080
fi

function run_mysql_tester()
{
    coll_disabled="false"
    coll_msg="enabled new collation"
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

run_mysql_tester
echo "integrationtest passed!"
