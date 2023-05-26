#!/bin/bash
#
# Copyright 2023 PingCAP, Inc.
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

LOG_FILE="$TEST_DIR/lightning-duplicate-detection-new.log"

cleanup() {
  run_sql 'DROP DATABASE IF EXISTS test'
  run_sql 'DROP DATABASE IF EXISTS lightning_task_info'
  rm -f "$LOG_FILE"
}

# 1. Test replace strategy.
cleanup
run_lightning --backend tidb --config "tests/$TEST_NAME/tidb-replace.toml" --log-file "$LOG_FILE"
expected_rows=$(run_sql "SELECT count(*) AS total_count FROM test.dup_detect" | grep "total_count" | awk '{print $2}')
expected_pks=$(run_sql "SELECT group_concat(col1 ORDER BY col1) AS pks FROM test.dup_detect" | grep "pks" | awk '{print $2}')

cleanup
run_lightning --backend local --config "tests/$TEST_NAME/local-replace.toml" --log-file "$LOG_FILE"
actual_rows=$(run_sql "SELECT count(*) AS total_count FROM test.dup_detect" | grep "total_count" | awk '{print $2}')
actual_pks=$(run_sql "SELECT group_concat(col1 ORDER BY col1) AS pks FROM test.dup_detect" | grep "pks" | awk '{print $2}')
if [ "$expected_rows" != "$actual_rows" ] || [ "$expected_pks" != "$actual_pks" ]; then
  echo "local backend replace strategy result is not equal to tidb backend"
  exit 1
fi
run_sql "SELECT count(*) FROM lightning_task_info.conflict_error_v2"
check_contains "count(*): 227"

# 2. Test ignore strategy.
cleanup
run_lightning --backend tidb --config "tests/$TEST_NAME/tidb-ignore.toml" --log-file "$LOG_FILE"
expected_rows=$(run_sql "SELECT count(*) AS total_count FROM test.dup_detect" | grep "total_count" | awk '{print $2}')
expected_pks=$(run_sql "SELECT group_concat(col1 ORDER BY col1) AS pks FROM test.dup_detect" | grep "pks" | awk '{print $2}')

cleanup
run_lightning --backend local --config "tests/$TEST_NAME/local-ignore.toml" --log-file "$LOG_FILE"
actual_rows=$(run_sql "SELECT count(*) AS total_count FROM test.dup_detect" | grep "total_count" | awk '{print $2}')
actual_pks=$(run_sql "SELECT group_concat(col1 ORDER BY col1) AS pks FROM test.dup_detect" | grep "pks" | awk '{print $2}')
if [ "$expected_rows" != "$actual_rows" ] || [ "$expected_pks" != "$actual_pks" ]; then
  echo "local backend ignore strategy result is not equal to tidb backend"
  exit 1
fi
run_sql "SELECT count(*) FROM lightning_task_info.conflict_error_v2"
check_contains "count(*): 227"

# 3. Test error strategy.
cleanup
run_lightning --backend local --config "tests/$TEST_NAME/local-error.toml" --log-file "$LOG_FILE" || true
check_contains "duplicate key detected" "$LOG_FILE"

# 4. Test limit error records.
cleanup
run_lightning --backend local --config "tests/$TEST_NAME/local-limit-error-records.toml" --log-file "$LOG_FILE"
run_sql "SELECT count(*) FROM test.dup_detect"
check_contains "count(*): 173"
run_sql "SELECT count(*) FROM lightning_task_info.conflict_error_v2"
check_contains "count(*): 50"

# 5. Test fail after duplicate detection.
cleanup

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/lightning/importer/FailAfterDuplicateDetection=return()"
run_lightning --enable-checkpoint=1 --backend local --config "tests/$TEST_NAME/local-replace.toml" --log-file "$LOG_FILE" || true

unset GO_FAILPOINTS
rm -f "$LOG_FILE"
run_lightning_ctl --enable-checkpoint=1 --backend local --config "tests/$TEST_NAME/local-replace.toml" --checkpoint-error-ignore="\`test\`.\`dup_detect\`"
run_lightning --enable-checkpoint=1 --backend local --config "tests/$TEST_NAME/local-replace.toml" --log-file "$LOG_FILE"
run_sql "SELECT count(*) FROM test.dup_detect"
check_contains "count(*): 173"
run_sql "SELECT count(*) FROM lightning_task_info.conflict_error_v2"
check_contains "count(*): 227"
check_not_contains "duplicate detection start" "$LOG_FILE"
