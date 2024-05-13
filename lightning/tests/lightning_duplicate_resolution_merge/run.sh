#!/bin/bash
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

set -eux

check_cluster_version 5 2 0 'duplicate detection' || exit 0

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

LOG_FILE1="$TEST_DIR/lightning_duplicate_resolution_merge1.log"
LOG_FILE2="$TEST_DIR/lightning_duplicate_resolution_merge2.log"

# let lightning run a bit slow to avoid some table in the first lightning finish too fast.
export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/SlowDownImport=sleep(1000)"

run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_resolution_merge.sorted1" \
  -d "$CUR/data1" --log-file "$LOG_FILE1" --config "$CUR/config.toml" &
pid1="$!"

run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_resolution_merge.sorted2" \
  -d "$CUR/data2" --log-file "$LOG_FILE2" --config "$CUR/config.toml" &
pid2="$!"

wait "$pid1" "$pid2"

# Ensure all tables are consistent.
run_sql 'admin check table dup_resolve.a'

run_sql 'select count(*) from dup_resolve.a'
check_contains 'count(*): 10'

run_sql 'select count(*) from lightning_task_info.conflict_records'
check_contains 'count(*): 16'

run_sql 'select count(*) from lightning_task_info.conflict_error_v3'
check_contains 'count(*): 4'

run_sql 'select count(*) from lightning_task_info.conflict_view'
check_contains 'count(*): 20'

run_sql 'select count(*) from lightning_task_info.conflict_view where is_precheck_conflict = 1'
check_contains 'count(*): 16'

run_sql 'select count(*) from lightning_task_info.conflict_view where is_precheck_conflict = 0'
check_contains 'count(*): 4'
