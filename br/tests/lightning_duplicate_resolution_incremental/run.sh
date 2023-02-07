#!/bin/bash
#
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

set -eux

check_cluster_version 5 2 0 'duplicate detection' || exit 0

LOG_FILE1="$TEST_DIR/lightning-duplicate-resolution1.log"
LOG_FILE2="$TEST_DIR/lightning-duplicate-resolution2.log"

# make sure after first lightning finishes importing, the second lightning then starts
run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_resolution_incremental.sorted1" \
  --enable-checkpoint=1 --log-file "$LOG_FILE1" --config "tests/$TEST_NAME/config1.toml"
run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_resolution_incremental.sorted2" \
  --enable-checkpoint=1 --log-file "$LOG_FILE2" --config "tests/$TEST_NAME/config2.toml"

# Ensure table is consistent.
run_sql 'admin check table dup_resolve_detect.ta'

# Check data correctness
run_sql 'select count(*), sum(id) from dup_resolve_detect.ta where id < 100'
check_contains 'count(*): 5'
check_contains 'sum(id): 80'

run_sql 'select count(*), sum(id) from dup_resolve_detect.ta where id > 100'
check_contains 'count(*): 16'
check_contains 'sum(id): 1896'
