#!/bin/bash
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

# To avoid permission denied error, please run `chmod +x tests/_utils/*`.

DUMPLING_TEST_DIR=${DUMPLING_TEST_DIR:-"/tmp/dumpling_test_result"}
DUMPLING_TEST_USER=${DUMPLING_TEST_USER:-"root"}

export DUMPLING_TEST_DIR
export DUMPLING_TEST_USER
export DUMPLING_TEST_PORT=3306

set -eu

mkdir -p "$DUMPLING_TEST_DIR"
PATH="tests/_utils:$PATH"
. "tests/_utils/run_services"

file_should_exist bin/tidb-server
file_should_exist bin/tidb-lightning
file_should_exist bin/dumpling
file_should_exist bin/sync_diff_inspector

trap stop_services EXIT
start_services

run_case_by_fullpath() {
    script="$1"
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Running test $script..."
    DUMPLING_BASE_NAME="$(dirname "$script")"
    export DUMPLING_BASE_NAME
    TEST_NAME="$(basename "$(dirname "$script")")"
    DUMPLING_OUTPUT_DIR="$DUMPLING_TEST_DIR"/sql_res."$TEST_NAME"
    export DUMPLING_OUTPUT_DIR

    PATH="tests/_utils:$PATH" \
        bash "$script"
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST: $script Passed Cleaning up test output dir: $DUMPLING_OUTPUT_DIR"
    rm -rf "$DUMPLING_OUTPUT_DIR"
}

if [ "$#" -ge 1 ]; then
    test_case="$@"
else
    test_case="*"
fi

if [ "$test_case" == "*" ]; then
    for script in tests/*/run.sh; do
        run_case_by_fullpath "$script"
    done
else
    script="tests/$test_case/run.sh"
    run_case_by_fullpath "$script"
fi

echo "Passed integration tests."