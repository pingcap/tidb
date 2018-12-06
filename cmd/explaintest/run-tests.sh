#!/bin/bash
source ./_helper.sh

TIDB_TEST_STORE_NAME=$TIDB_TEST_STORE_NAME
TIKV_PATH=$TIKV_PATH

build=1
explain_test="./explain_test"
importer=""
tidb_server=""
explain_test_log="./explain-test.out"
tests=""
record=0
record_case=""
create=0
create_case=""

set -eu

function help_message()
{
    echo "Usage: $0 [options]

    -h: Print this help message.

    -s <tidb-server-path>: Use tidb-server in <tidb-server-path> for testing.
                           eg. \"./run-tests.sh -s ./explaintest_tidb-server\"

    -b <y|Y|n|N>: \"y\" or \"Y\" for building test binaries [default \"y\" if this option is not specified].
                  \"n\" or \"N\" for not to build.
                  The building of tidb-server will be skiped if \"-s <tidb-server-path>\" is provided.

    -r <test-name>|all: Run tests in file \"t/<test-name>.test\" and record result to file \"r/<test-name>.result\".
                        \"all\" for running all tests and record their results.

    -t <test-name>: Run tests in file \"t/<test-name>.test\".
                    This option will be ignored if \"-r <test-name>\" is provided.
                    Run all tests if this option is not provided.

    -v <vendor-path>: Add <vendor-path> to \$GOPATH.

    -c <test-name>|all: Create data according to creating statements in file \"t/<test-name>.test\" and save stats in \"s/<test-name>_tableName.json\".
                    <test-name> must has a suffix of '_stats'.
                    \"all\" for creating stats of all tests.

    -i <importer-path>: Use importer in <importer-path> for creating data.

"
}

function build_importer()
{
    importer="./importer"
    echo "building importer binary: $importer"
    rm -rf $importer
    go build -o $importer github.com/pingcap/tidb/cmd/importer
}

function build_tidb_server()
{
    tidb_server="./explaintest_tidb-server"
    echo "building tidb-server binary: $tidb_server"
    rm -rf $tidb_server
    go build -race -o $tidb_server github.com/pingcap/tidb/tidb-server
}

function build_explain_test()
{
    echo "building explain-test binary: $explain_test"
    rm -rf $explain_test
    go build -o $explain_test
}

while getopts "t:s:r:b:v:c:i:h" opt; do
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
                    help_messge 1>&2
                    exit 1
                    ;;
            esac
            ;;
        v)
            GOPATH="$OPTARG:$GOPATH"
            echo "GOPATH: $GOPATH"
            ;;
        h)
            help_message
            exit 0
            ;;
        c)
            create=1
            create_case="$OPTARG"
            ;;
        i)
            importer="$OPTARG"
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
    if [[ -z "$importer" && $create -eq 1 ]]; then
        build_importer
    else
        echo "skip building importer, using existing binary: $importer"
    fi
    prepare_env
    echo $GOPATH
    build_explain_test
    recover_env
else
    if [ -z "$tidb_server" ]; then
        tidb_server="./explaintest_tidb-server"
    fi
    if [ -z "$explain_test" ]; then
        explain_test="./explain_test"
    fi
    if [ -z "$importer" ]; then
        importer="./importer"
    fi
    echo "skip building tidb-server, using existing binary: $tidb_server"
    echo "skip building explaintest, using existing binary: $explain_test"
    echo "skip building importer, using existing binary: $importer"
fi

rm -rf $explain_test_log

echo "start tidb-server, log file: $explain_test_log"
if [ "${TIDB_TEST_STORE_NAME}" = "tikv" ]; then
    $tidb_server -config config.toml -store tikv -path "${TIKV_PATH}" > $explain_test_log 2>&1 &
    SERVER_PID=$!
else
    $tidb_server -config config.toml -store mocktikv -path "" > $explain_test_log 2>&1 &
    SERVER_PID=$!
fi
echo "tidb-server(PID: $SERVER_PID) started"

sleep 5

if [ $record -eq 1 ]; then
    if [ "$record_case" = 'all' ]; then
        echo "record all cases"
        $explain_test --record --log-level=error
    else
        echo "record result for case: \"$record_case\""
        $explain_test --record $record_case --log-level=error
    fi
elif [ $create -eq 1 ]; then
    if [ "$create_case" = 'all' ]; then
        echo "create all cases"
        $explain_test --create --log-level=error
    else
        echo "create result for case: \"$create_case\""
        $explain_test --create $create_case --log-level=error
    fi
else
    if [ -z "$tests" ]; then
        echo "run all explain test cases"
    else
        echo "run explain test cases: $tests"
    fi
    $explain_test --log-level=error $tests
fi

race=`grep 'DATA RACE' $explain_test_log || true`
if [ ! -z "$race" ]; then
    echo "tidb-server DATA RACE!"
    cat $explain_test_log
    exit 1
fi
echo "explaintest end"
