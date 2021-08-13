#!/bin/bash
#
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
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu
export PATH="tests/_utils:bin:$PATH"
export TEST_DIR=/tmp/backup_restore_test

# Reset TEST_DIR
rm -rf $TEST_DIR && mkdir -p $TEST_DIR

# Generate TLS certs
tests/_utils/generate_certs &> /dev/null

SELECTED_TEST_NAME="${TEST_NAME-$(find tests -mindepth 2 -maxdepth 2 -name run.sh | cut -d/ -f2 | sort)}"
source tests/_utils/run_services

trap stop_services EXIT
start_services

# Intermediate file needed because read can be used as a pipe target.
# https://stackoverflow.com/q/2746553/
run_curl "https://$PD_ADDR/pd/api/v1/version" | grep -o 'v[0-9.]\+' > "$TEST_DIR/cluster_version.txt"
IFS='.' read CLUSTER_VERSION_MAJOR CLUSTER_VERSION_MINOR CLUSTER_VERSION_REVISION < "$TEST_DIR/cluster_version.txt"

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
fi

echo "selected test cases: $SELECTED_TEST_NAME"

# wait for global variable cache invalid
sleep 2

for casename in $SELECTED_TEST_NAME; do
    script=tests/$casename/run.sh
    echo "*===== Running test $script... =====*"
    INTEGRATION_TEST=1 \
    TEST_DIR="$TEST_DIR" \
    TEST_NAME="$casename" \
    CLUSTER_VERSION_MAJOR="${CLUSTER_VERSION_MAJOR#v}" \
    CLUSTER_VERSION_MINOR="$CLUSTER_VERSION_MINOR" \
    CLUSTER_VERSION_REVISION="$CLUSTER_VERSION_REVISION" \
    PD_ADDR="$PD_ADDR" \
    TIDB_IP="$TIDB_IP" \
    TIDB_PORT="$TIDB_PORT" \
    TIDB_ADDR="$TIDB_ADDR" \
    TIDB_STATUS_ADDR="$TIDB_STATUS_ADDR" \
    TIKV_ADDR="$TIKV_ADDR" \
    BR_LOG_TO_TERM=1 \
    bash "$script" && echo "TEST: [$casename] success!"
done
