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

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

LOG_FILE="$TEST_DIR/lightning-duplicate-detection-new.log"

cleanup() {
  run_sql 'DROP DATABASE IF EXISTS test'
  run_sql 'DROP DATABASE IF EXISTS lightning_task_info'
  rm -f "$LOG_FILE"
}

# 1. Test replace strategy.
cleanup
run_lightning --backend tidb --config "$CUR/tidb-replace.toml" --log-file "$LOG_FILE"
expected_rows=$(run_sql "SELECT count(*) AS total_count FROM test.dup_detect" | grep "total_count" | awk '{print $2}')
expected_pks=$(run_sql "SELECT group_concat(col1 ORDER BY col1) AS pks FROM test.dup_detect" | grep "pks" | awk '{print $2}')

cleanup
run_lightning --backend local --config "$CUR/local-replace.toml" --log-file "$LOG_FILE"
actual_rows=$(run_sql "SELECT count(*) AS total_count FROM test.dup_detect" | grep "total_count" | awk '{print $2}')
actual_pks=$(run_sql "SELECT group_concat(col1 ORDER BY col1) AS pks FROM test.dup_detect" | grep "pks" | awk '{print $2}')
if [ "$expected_rows" != "$actual_rows" ] || [ "$expected_pks" != "$actual_pks" ]; then
  echo "local backend replace strategy result is not equal to tidb backend"
  exit 1
fi
run_sql "SELECT count(*) FROM lightning_task_info.conflict_records"
check_contains "count(*): 227"
run_sql "SELECT count(*) FROM lightning_task_info.conflict_records WHERE error = ''"
check_contains "count(*): 0"
run_sql "SELECT * FROM lightning_task_info.conflict_records WHERE row_id = 12"
check_contains "(171,'yRxZE',9201592769833450947,'xs3d',5,4,283270321)"
check_contains "[kv:1062]Duplicate entry '171' for key 'dup_detect.PRIMARY'"
run_sql "SELECT * FROM lightning_task_info.conflict_records WHERE row_id = 1"
check_contains "(87,'nEoKu',7836621565948506759,'y6',48,0,177543185)"
check_contains "[kv:1062]Duplicate entry '0-177543185' for key 'dup_detect.uniq_col6_col7'"

# 2. Test ignore strategy.
cleanup
run_lightning --backend tidb --config "$CUR/tidb-ignore.toml" --log-file "$LOG_FILE"
expected_rows=$(run_sql "SELECT count(*) AS total_count FROM test.dup_detect" | grep "total_count" | awk '{print $2}')
expected_pks=$(run_sql "SELECT group_concat(col1 ORDER BY col1) AS pks FROM test.dup_detect" | grep "pks" | awk '{print $2}')

# 3. Test error strategy.
cleanup
run_lightning --backend local --config "$CUR/local-error.toml" --log-file "$LOG_FILE" 2>&1 | grep -q "duplicate key in table \`test\`.\`dup_detect\` caused by index .*, but because checkpoint is off we can't have more details"
grep -q "duplicate key in table \`test\`.\`dup_detect\` caused by index .*, but because checkpoint is off we can't have more details" "$LOG_FILE"
run_sql "SELECT * FROM lightning_task_info.conflict_records"
check_contains "error: duplicate key in table \`test\`.\`dup_detect\`"
run_lightning --backend local --config "$CUR/local-error.toml" --log-file "$LOG_FILE" --enable-checkpoint=1 2>&1 | grep -q "duplicate entry for key 'uniq_col6_col7', a pair of conflicting rows are (row 1 counting from offset 0 in file test.dup_detect.1.sql, row 101 counting from offset 0 in file test.dup_detect.4.sql)"
grep -q "duplicate entry for key 'uniq_col6_col7', a pair of conflicting rows are (row 1 counting from offset 0 in file test.dup_detect.1.sql, row 101 counting from offset 0 in file test.dup_detect.4.sql)" "$LOG_FILE"
run_sql "SELECT * FROM lightning_task_info.conflict_records"
check_contains "error: duplicate entry for key 'uniq_col6_col7', a pair of conflicting rows are"
check_contains "restore table \`test\`.\`dup_detect\` failed: duplicate entry for key 'uniq_col6_col7', a pair of conflicting rows are (row 1 counting from offset 0 in file test.dup_detect.1.sql, row 101 counting from offset 0 in file test.dup_detect.4.sql)" "$LOG_FILE"
run_lightning_ctl --enable-checkpoint=1 --backend local --config "$CUR/local-error.toml" --checkpoint-error-destroy="\`test\`.\`dup_detect\`"
files_left=$(ls "$TEST_DIR/$TEST_NAME.sorted" | wc -l)
if [ "$files_left" -ne "0" ];then
    echo "checkpoint-error-destroy has left some files"
    ls "$TEST_DIR/$TEST_NAME.sorted"
    exit 1
fi
rm -rf "$TEST_DIR/$TEST_NAME.sorted"

# 4. Test fail after duplicate detection.
cleanup

export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/FailAfterDuplicateDetection=return()"
run_lightning --enable-checkpoint=1 --backend local --config "$CUR/local-replace.toml" --log-file "$LOG_FILE" || true

unset GO_FAILPOINTS
rm -f "$LOG_FILE"
run_lightning_ctl --enable-checkpoint=1 --backend local --config "$CUR/local-replace.toml" --checkpoint-error-ignore="\`test\`.\`dup_detect\`"
run_lightning --enable-checkpoint=1 --backend local --config "$CUR/local-replace.toml" --log-file "$LOG_FILE"
run_sql "SELECT count(*) FROM test.dup_detect"
check_contains "count(*): 174"
run_sql "SELECT count(*) FROM lightning_task_info.conflict_records"
check_contains "count(*): 227"
check_not_contains "duplicate detection start" "$LOG_FILE"
