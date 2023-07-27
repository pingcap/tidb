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

run_sql 'DROP DATABASE IF EXISTS lightning_auto_bigint1;'
run_sql 'DROP DATABASE IF EXISTS lightning_auto_bigint;'
run_sql 'CREATE DATABASE lightning_auto_bigint1;'
run_sql 'CREATE DATABASE lightning_auto_bigint;'
run_sql 'CREATE TABLE lightning_auto_bigint1.t_bigint (id bigint unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT, c varchar(255) NOT NULL);'
run_sql 'CREATE TABLE lightning_auto_bigint.u (id bigint unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT, c varchar(255) NOT NULL);'

run_lightning

run_sql "SELECT CONCAT_WS(':', id, c) AS row_data FROM lightning_auto_bigint.u;"
check_contains "row_data: 1:normal_pk_01"
check_contains "row_data: 2:null_pk_02"
check_contains "row_data: 10942694589135710585:test0"
check_contains "row_data: 10942694589135710586:test1"
run_sql "SELECT COUNT(*) AS row_count FROM lightning_auto_bigint.u;"
check_contains "row_count: 4"
