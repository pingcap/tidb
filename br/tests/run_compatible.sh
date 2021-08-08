#!/bin/bash
#
# Copyright 2020 PingCAP, Inc.
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

# This test is used to test compatible for BR restore.
# It will download backup data from internal file server.
# And make sure these backup data can restore through newly BR tools to newly cluster.

set -eu

source ${BASH_SOURCE[0]%/*}/../compatibility/get_last_tags.sh
getLatestTags
echo "start test on $TAGS"

EXPECTED_KVS=1000
PD_ADDR="pd0:2379"
GCS_HOST="gcs"
GCS_PORT="20818"
TEST_DIR=/tmp/backup_restore_compatibility_test
mkdir -p "$TEST_DIR"
rm -f "$TEST_DIR"/*.log &> /dev/null

for script in tests/docker_compatible_*/${1}.sh; do
    echo "*===== Running test $script... =====*"
    TEST_DIR="$TEST_DIR" \
    PD_ADDR="$PD_ADDR" \
    GCS_HOST="$GCS_HOST" \
    GCS_PORT="$GCS_PORT" \
    TAGS="$TAGS" \
    EXPECTED_KVS="$EXPECTED_KVS" \
    PATH="tests/_utils:bin:$PATH" \
    TEST_NAME="$(basename "$(dirname "$script")")" \
    BR_LOG_TO_TERM=1 \
    bash "$script"
done
