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

run_sql 'DROP TABLE IF EXISTS mytest.testtbl'
run_sql 'DROP TABLE IF EXISTS lightning_task_info.conflict_error_v1'

stderr_file="/tmp/${TEST_NAME}.stderr"

set +e
if run_lightning --backend local --config "${mydir}/err_config.toml" 2> "${stderr_file}"; then
    echo "The lightning import doesn't fail as expected" >&2
    exit 1
fi
set -e

err_msg=$( cat << EOF
tidb lightning encountered error: collect local duplicate rows failed: The number of conflict errors exceeds the threshold configured by \`max-error.conflict\`: '4'
EOF
)
cat "${stderr_file}"
grep -q "${err_msg}" "${stderr_file}"

run_sql 'SELECT COUNT(*) FROM lightning_task_info.conflict_error_v1'
# Although conflict error number exceeds the max-error limit, 
# all the conflict errors are recorded, 
# because recording of conflict errors are executed batch by batch (batch size 1024), 
# this batch of conflict errors are all recorded
check_contains "COUNT(*): ${duplicated_row_count}"

# import a second time

run_sql 'DROP TABLE IF EXISTS mytest.testtbl'
run_sql 'DROP TABLE IF EXISTS lightning_task_info.conflict_error_v1'

run_lightning --backend local --config "${mydir}/normal_config.toml"

run_sql 'SELECT COUNT(*) FROM lightning_task_info.conflict_error_v1'
check_contains "COUNT(*): ${duplicated_row_count}"

# Check remaining records in the target table
run_sql 'SELECT COUNT(*) FROM mytest.testtbl'
check_contains "COUNT(*): ${uniq_row_count}"

# import a third time

run_sql 'DROP TABLE IF EXISTS mytest.testtbl'
run_sql 'DROP TABLE IF EXISTS lightning_task_info.conflict_error_v1'

run_lightning --backend local --config "${mydir}/normal_config_old_style.toml"

run_sql 'SELECT COUNT(*) FROM lightning_task_info.conflict_error_v1'
check_contains "COUNT(*): ${duplicated_row_count}"

# Check remaining records in the target table
run_sql 'SELECT COUNT(*) FROM mytest.testtbl'
check_contains "COUNT(*): ${uniq_row_count}"
