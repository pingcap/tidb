#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
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

run_sql 'DROP DATABASE IF EXISTS lightning_auto_cols;'
run_lightning

run_sql "SELECT CONCAT_WS(':', id, c) AS row_data FROM lightning_auto_cols.t_auto_incr;"
check_contains "row_data: 1:normal_pk_01"
check_contains "row_data: 2:null_pk_02"
check_contains "row_data: 3:null_pk_03"
check_contains "row_data: 4:normal_pk_04"
run_sql "SELECT COUNT(*) AS row_count FROM lightning_auto_cols.t_auto_incr;"
check_contains "row_count: 4"

run_sql "SELECT CONCAT_WS(':', id, c) AS row_data FROM lightning_auto_cols.t_auto_random;"
check_contains "row_data: 1:normal_pk_01"
check_contains ":null_pk_02"
check_not_contains "row_data: 0:null_pk_02"
check_contains ":null_pk_03"
check_not_contains "row_data: 0:null_pk_03"
check_contains "row_data: 4:normal_pk_04"
run_sql "SELECT COUNT(*) AS row_count FROM lightning_auto_cols.t_auto_random;"
check_contains "row_count: 4"
