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
PATH="dumpling/tests/_utils:$PATH"
. "dumpling/tests/_utils/run_services"

file_should_exist bin/tidb-server
file_should_exist bin/tidb-lightning
file_should_exist bin/dumpling
file_should_exist bin/sync_diff_inspector

trap stop_services EXIT
start_services

function run_case_by_fullpath() {
    script="$1"
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Running test $script..."
    DUMPLING_BASE_NAME="$(dirname "$script")"
    export DUMPLING_BASE_NAME
    TEST_NAME="$(basename "$(dirname "$script")")"
    DUMPLING_OUTPUT_DIR="$DUMPLING_TEST_DIR"/sql_res."$TEST_NAME"
    export DUMPLING_OUTPUT_DIR

    VERBOSE=${VERBOSE-}
    # run in verbose mode?
    echo "Verbose mode = $VERBOSE"
    if [ "$VERBOSE" = "true" ]; then
      PATH="dumpling/tests/_utils:$PATH" \
        PS4='+(${BASH_SOURCE}:${LINENO}): ${FUNCNAME[0]:+${FUNCNAME[0]}(): }' \
        bash -x "$script"
    else
      PATH="dumpling/tests/_utils:$PATH" \
        bash +x "$script"
    fi
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> TEST: $script Passed Cleaning up test output dir: $DUMPLING_OUTPUT_DIR"
    rm -rf "$DUMPLING_OUTPUT_DIR"
}

if [ "$#" -ge 1 ]; then
    test_case="$@"
else
    test_case="*"
fi

if [ "$test_case" == "*" ]; then
    for script in dumpling/tests/*/run.sh; do
        run_case_by_fullpath "$script"
    done
else
    script="dumpling/tests/$test_case/run.sh"
    run_case_by_fullpath "$script"
fi

echo "Passed integration tests."
