#!/bin/sh
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

check_cluster_version 4 0 0 'local backend' || exit 0

mydir=$(dirname "${BASH_SOURCE[0]}")

data_file="${mydir}/data/mytest.testtbl.csv"

total_row_count=$( sed '1d' "${data_file}" | wc -l | xargs echo )
uniq_row_count=$( sed '1d' "${data_file}" | awk -F, '{print $1}' | sort | uniq -c | awk '{print $1}' | grep -c '1' | xargs echo )
duplicated_row_count=$(( ${total_row_count} - ${uniq_row_count} ))
remaining_row_count=$(( ${uniq_row_count} + ${duplicated_row_count}/2 ))

run_sql 'DROP TABLE IF EXISTS mytest.testtbl'
run_sql 'DROP TABLE IF EXISTS lightning_task_info.conflict_error_v3'
run_sql 'DROP VIEW IF EXISTS lightning_task_info.conflict_view'

stderr_file="/tmp/${TEST_NAME}.stderr"

set +e
if run_lightning --backend local --config "${mydir}/err_config.toml" 2> "${stderr_file}"; then
    echo "The lightning import doesn't fail as expected" >&2
    exit 1
fi
set -e

err_msg=$( cat << EOF
tidb lightning encountered error: collect local duplicate rows failed: The number of conflict errors exceeds the threshold configured by \`conflict.threshold\`: '4'
EOF
)
cat "${stderr_file}"
grep -q "${err_msg}" "${stderr_file}"

run_sql 'SELECT COUNT(*) FROM lightning_task_info.conflict_error_v3'
# Although conflict error number exceeds the max-error limit, 
# all the conflict errors are recorded, 
# because recording of conflict errors are executed batch by batch (batch size 1024), 
# this batch of conflict errors are all recorded
check_contains "COUNT(*): ${duplicated_row_count}"

# import a second time

run_sql 'DROP TABLE IF EXISTS mytest.testtbl'
run_sql 'DROP TABLE IF EXISTS lightning_task_info.conflict_error_v3'
run_sql 'DROP VIEW IF EXISTS lightning_task_info.conflict_view'

run_lightning --backend local --config "${mydir}/normal_config.toml"

run_sql 'SELECT COUNT(*) FROM lightning_task_info.conflict_error_v3'
check_contains "COUNT(*): ${duplicated_row_count}"

# Check remaining records in the target table
run_sql 'SELECT COUNT(*) FROM mytest.testtbl'
check_contains "COUNT(*): ${remaining_row_count}"

# import a third time

run_sql 'DROP TABLE IF EXISTS mytest.testtbl'
run_sql 'DROP TABLE IF EXISTS lightning_task_info.conflict_error_v3'
run_sql 'DROP VIEW IF EXISTS lightning_task_info.conflict_view'

run_lightning --backend local --config "${mydir}/normal_config_old_style.toml"

run_sql 'SELECT COUNT(*) FROM lightning_task_info.conflict_error_v3'
check_contains "COUNT(*): ${duplicated_row_count}"

# Check remaining records in the target table
run_sql 'SELECT COUNT(*) FROM mytest.testtbl'
check_contains "COUNT(*): ${remaining_row_count}"

# import a fourth time
run_sql 'DROP TABLE IF EXISTS lightning_task_info.conflict_records'
run_sql 'DROP VIEW IF EXISTS lightning_task_info.conflict_view'
! run_lightning --backend local --config "${mydir}/ignore_config.toml"
[ $? -eq 0 ]
tail -n 10 $TEST_DIR/lightning.log | grep "ERROR" | tail -n 1 | grep -Fq "[Lightning:Config:ErrInvalidConfig]conflict.strategy cannot be set to \\\"ignore\\\" when use tikv-importer.backend = \\\"local\\\""

# Check tidb backend record duplicate entry in conflict_records table
run_sql 'DROP TABLE IF EXISTS lightning_task_info.conflict_records'
run_sql 'DROP VIEW IF EXISTS lightning_task_info.conflict_view'
run_lightning --backend tidb --config "${mydir}/tidb.toml"
run_sql 'SELECT COUNT(*) FROM lightning_task_info.conflict_records'
check_contains "COUNT(*): 15"
run_sql 'SELECT * FROM lightning_task_info.conflict_records WHERE offset = 149'
check_contains "error: Error 1062 (23000): Duplicate entry '5' for key 'testtbl.PRIMARY'"
check_contains "row_data: ('5','bbb05')"

# Check max-error-record can limit the size of conflict_records table
run_sql 'DROP DATABASE IF EXISTS lightning_task_info'
run_sql 'DROP DATABASE IF EXISTS mytest'
run_lightning --backend tidb --config "${mydir}/tidb-limit-record.toml" 2>&1 | grep "\`lightning_task_info\`.\`conflict_view\`" | grep -q "5"
run_sql 'SELECT COUNT(*) FROM lightning_task_info.conflict_records'
check_contains "COUNT(*): 5"

# Check conflict.threshold
run_sql 'DROP DATABASE IF EXISTS lightning_task_info'
run_sql 'DROP DATABASE IF EXISTS mytest'
cp "${mydir}/tidb-limit-record.toml" "${TEST_DIR}/tidb-limit-record.toml"
sed -i.bak "s/threshold = 5/threshold = 4/g" "${TEST_DIR}/tidb-limit-record.toml"
run_lightning --backend tidb --config "${TEST_DIR}/tidb-limit-record.toml" 2>&1 | grep -q "The number of conflict errors exceeds the threshold"

# Check when strategy is "error", the stderr, log and duplicate record table all contains the error message
run_sql 'DROP DATABASE IF EXISTS lightning_task_info'
run_sql 'DROP DATABASE IF EXISTS mytest'
rm "${TEST_DIR}/lightning.log"
run_lightning --backend tidb --config "${mydir}/tidb-error.toml" 2>&1 | grep -q "Error 1062 (23000): Duplicate entry '1' for key 'testtbl.PRIMARY'"
check_contains "Error 1062 (23000): Duplicate entry '1' for key 'testtbl.PRIMARY'" "${TEST_DIR}/lightning.log"
run_sql 'SELECT COUNT(*) FROM lightning_task_info.conflict_records'
check_contains "COUNT(*): 1"
run_sql 'SELECT * FROM lightning_task_info.conflict_records'
check_contains "error: Error 1062 (23000): Duplicate entry '1' for key 'testtbl.PRIMARY'"
check_contains "row_data: ('1','bbb01')"
