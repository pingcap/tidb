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

mydir=$(dirname "${BASH_SOURCE[0]}")

data_file="${mydir}/data/mytest.testtbl.csv"

total_row_count=$( sed '1d' "${data_file}" | wc -l | xargs echo )

run_sql 'DROP TABLE IF EXISTS mytest.testtbl'

run_lightning --backend local --config "${mydir}/err_config.toml"

run_sql 'SELECT * FROM mytest.testtbl'
run_sql 'SELECT COUNT(*) FROM mytest.testtbl'
check_contains "COUNT(*): $(( total_row_count + 1 ))"

# import a second time

run_sql 'DROP TABLE IF EXISTS mytest.testtbl'

run_lightning --backend local --config "${mydir}/normal_config.toml"

run_sql 'SELECT * FROM mytest.testtbl'
run_sql 'SELECT COUNT(*) FROM mytest.testtbl'
check_contains "COUNT(*): ${total_row_count}"
run_sql 'SELECT COUNT(*) FROM mytest.testtbl WHERE id > 0'
check_contains "COUNT(*): ${total_row_count}"
