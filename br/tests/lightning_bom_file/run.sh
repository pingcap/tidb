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

row_count=$( sed '1d' "${data_file}" | wc -l | xargs echo )

run_lightning --backend tidb

# Check that everything is correctly imported
run_sql 'SELECT count(*) FROM mytest.testtbl'
check_contains "count(*): ${row_count}"

check_cluster_version 4 0 0 'local backend' || exit 0
run_sql "DROP TABLE mytest.testtbl"

run_lightning --backend local

# Check that everything is correctly imported
run_sql 'SELECT count(*) FROM mytest.testtbl'
check_contains "count(*): ${row_count}"
