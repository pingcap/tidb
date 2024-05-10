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

set -eu
DB="$TEST_NAME"

run_sql "create database if not exists ${DB}"
run_sql "create table $DB.issue46093 (a int primary key nonclustered auto_increment, b int) auto_id_cache = 1;"
run_sql "insert into $DB.issue46093 (b) values (1), (2), (3);"
run_sql "show table $DB.issue46093 next_row_id;"
check_contains "NEXT_GLOBAL_ROW_ID: 30001"
check_contains "NEXT_GLOBAL_ROW_ID: 4"

run_sql "backup table $DB.issue46093 to 'local://$TEST_DIR/$DB'";
run_sql "drop table $DB.issue46093;"
run_sql "restore table $DB.issue46093 from 'local://$TEST_DIR/$DB';"

run_sql "show table $DB.issue46093 next_row_id;"
check_contains "NEXT_GLOBAL_ROW_ID: 30001"
check_contains "NEXT_GLOBAL_ROW_ID: 4001"
run_sql "insert into $DB.issue46093 (b) values (4), (5), (6);"
run_sql "insert into $DB.issue46093 (b) values (7), (8), (9);"
run_sql "select * from $DB.issue46093;"
check_contains "a: 1"
check_contains "a: 2"
check_contains "a: 3"
check_contains "a: 4001"
check_contains "a: 4002"
check_contains "a: 4003"
check_contains "a: 4004"
check_contains "a: 4005"
check_contains "a: 4006"
check_contains "b: 4"
check_contains "b: 5"
check_contains "b: 6"
check_contains "b: 7"
check_contains "b: 8"
check_contains "b: 9"
