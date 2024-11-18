#!/bin/sh
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

# Basic check for correctness when row format v2 is active.
# (There's no way to verify if the rows are really in format v2 though...)

set -eux

run_sql 'show variables like "%tidb_row_format_version%";'
row_format=$(grep 'Value: [0-9]' "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk '{print $2}')

if [ "$row_format" -ne "2" ]; then
run_sql 'SET @@global.tidb_row_format_version = 2;' || { echo 'TiDB does not support changing row format version! skipping test'; exit 0; }
fi

run_sql 'DROP DATABASE IF EXISTS rowformatv2;'

run_lightning

run_sql 'SELECT count(1) FROM rowformatv2.t1;'
check_contains 'count(1): 50'

run_sql 'SELECT DISTINCT col14 FROM rowformatv2.t1;'
check_contains 'col14: NULL'
check_contains 'col14: 39'

if [ "$row_format" -ne "2" ]; then
run_sql "SET @@global.tidb_row_format_version = $row_format;"
fi
