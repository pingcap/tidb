#!/bin/bash

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

set -eu
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export UTILS_DIR="$CUR/../../tests/_utils"
export PATH="$PATH:$CUR/../../bin:$CUR/../bin:$UTILS_DIR"
export TEST_DIR=/tmp/backup_restore_test
export COV_DIR="/tmp/group_cover"
mkdir -p $COV_DIR || true
export TIDB_CONFIG="$CUR/config/tidb.toml"
export TIKV_CONFIG="$CUR/config/tikv.toml"
export PD_CONFIG="$CUR/config/pd.toml"
export TESTS_ROOT="$CUR"
source $UTILS_DIR/run_services

# Create COV_DIR if not exists
if [ -d "$COV_DIR" ]; then
   mkdir -p $COV_DIR
fi

# Reset TEST_DIR
rm -rf $TEST_DIR && mkdir -p $TEST_DIR

# Generate TLS certs
generate_certs &> /dev/null

# Use the environment variable TEST_NAME if set, otherwise find all test cases
SELECTED_TEST_NAME="${TEST_NAME:-$(find "$CUR" -mindepth 2 -maxdepth 2 -name run.sh | awk -F'/' '{print $(NF-1)}' | sort)}"
trap stop_services EXIT
start_services $@

# Intermediate file needed because read can be used as a pipe target.
# https://stackoverflow.com/q/2746553/
run_curl "https://$PD_ADDR/pd/api/v1/version" | grep -o 'v[0-9.]\+' > "$TEST_DIR/cluster_version.txt"
IFS='.' read CLUSTER_VERSION_MAJOR CLUSTER_VERSION_MINOR CLUSTER_VERSION_REVISION < "$TEST_DIR/cluster_version.txt"

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
fi

echo "Selected test cases: $SELECTED_TEST_NAME"

run_case() {
    local case=$1
    local script=$2
    echo "*===== Running test $script... =====*"
    INTEGRATION_TEST=1 \
    TEST_DIR="$TEST_DIR" \
    TEST_NAME="$case" \
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
    bash "$script" && echo "TEST: [$case] success!"
}

# Wait for global variable cache invalid
sleep 2

for casename in $SELECTED_TEST_NAME; do
    script="$CUR/$casename/run.sh"
    run_case "$casename" "$script"
done
