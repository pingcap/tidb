#!/bin/sh
#
# Copyright 2024 PingCAP, Inc.
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

echo "=========================================="
echo "Running test 1: Normal table with merge_option"
echo "=========================================="
ORIG_TEST_NAME="${TEST_NAME:-br_merge_option_attributes}"
ORIG_TEST_DIR="${TEST_DIR:-/tmp/backup_restore_test}"
export TEST_NAME="${ORIG_TEST_NAME}_normal"
export TEST_DIR="${ORIG_TEST_DIR}_normal"
# Create the test directory and copy certs if they exist
mkdir -p "$TEST_DIR"
if [ -d "$ORIG_TEST_DIR/certs" ]; then
    cp -r "$ORIG_TEST_DIR/certs" "$TEST_DIR/"
fi
bash "$CUR/test_normal_table.sh"
test1_result=$?
export TEST_NAME="$ORIG_TEST_NAME"
export TEST_DIR="$ORIG_TEST_DIR"
if [ $test1_result -ne 0 ]; then
    echo "Test 1 failed!"
    exit 1
fi

echo ""
echo "=========================================="
echo "Running test 2: Partition table with merge_option"
echo "=========================================="
export TEST_NAME="${ORIG_TEST_NAME}_partition"
export TEST_DIR="${ORIG_TEST_DIR}_partition"
# Create the test directory and copy certs if they exist
mkdir -p "$TEST_DIR"
if [ -d "$ORIG_TEST_DIR/certs" ]; then
    cp -r "$ORIG_TEST_DIR/certs" "$TEST_DIR/"
fi
bash "$CUR/test_partition_table.sh"
test2_result=$?
export TEST_NAME="$ORIG_TEST_NAME"
export TEST_DIR="$ORIG_TEST_DIR"
if [ $test2_result -ne 0 ]; then
    echo "Test 2 failed!"
    exit 1
fi

echo ""
echo "=========================================="
echo "All tests passed!"
echo "=========================================="

# Cleanup backup directories created for sub-tests
echo "Cleaning up backup directories..."
rm -rf "${ORIG_TEST_DIR}_normal" "${ORIG_TEST_DIR}_partition" || true

