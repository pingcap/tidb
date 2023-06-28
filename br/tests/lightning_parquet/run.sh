#!/bin/bash
#
# Copyright 2019 PingCAP, Inc.
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

set -euE

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

check_row_count() {
    run_sql "select count(*) from test.$1;"
    check_contains "count(*): $2"
}

for BACKEND in local tidb; do
    if [ "$BACKEND" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi
    run_sql 'DROP DATABASE IF EXISTS test'
    run_sql 'CREATE DATABASE test'
    run_sql "source $CUR/db.sql;" -D test

    run_lightning --backend $BACKEND

    check_row_count customer 20
    check_row_count district 10
    check_row_count history 100
    check_row_count item 100
    check_row_count new_order 100
    check_row_count order_line 100
    check_row_count orders 100
    check_row_count stock 50
    check_row_count warehouse 1
    check_row_count special_col_name 1

    run_sql 'select sum(c_id) from test.customer;'
    check_contains "sum(c_id): 210"

    run_sql 'select w_name from test.warehouse;'
    check_contains "w_name: eLNEDIW"
    run_sql 'select w_bool from test.warehouse;'
    check_contains "w_bool: 1"

    run_sql 'select c_since, c_discount from test.customer where c_id = 20;'
    check_contains "c_since: 2020-09-10 20:17:16"
    check_contains "c_discount: 0.0585"

    run_sql 'select CONVERT_TZ(ts, "+8:00", "+0:00") as ts from test.test_time;'
    check_contains "ts: 2022-09-10 09:09:00"
    check_contains "ts: 1997-08-11 02:01:10"
    check_contains "ts: 1995-12-31 23:00:01"
    check_contains "ts: 2020-02-29 23:00:00"
    check_contains "ts: 2038-01-19 00:00:00"
done
