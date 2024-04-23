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

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# FIXME: auto-random is only stable on master currently.
check_cluster_version 4 0 0 AUTO_RANDOM || exit 0

for backend in tidb local; do
    if [ "$backend" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi

    run_sql 'DROP DATABASE IF EXISTS auto_random;'
    run_lightning --backend $backend

    run_sql "SELECT count(*) from auto_random.t"
    check_contains "count(*): 6"

    run_sql "SELECT id & b'000001111111111111111111111111111111111111111111111111111111111' as inc FROM auto_random.t"
    check_contains 'inc: 1'
    check_contains 'inc: 2'
    check_contains 'inc: 3'
    if [ "$backend" = 'tidb' ]; then
      check_contains 'inc: 4'
      check_contains 'inc: 5'
      check_contains 'inc: 6'
      NEXT_AUTO_RAND_VAL=7
    else
      check_contains 'inc: 25'
      check_contains 'inc: 26'
      check_contains 'inc: 27'
      NEXT_AUTO_RAND_VAL=28
    fi

    # tidb backend randomly generate the auto-random bit for each statement, so with 2 statements,
    # the distinct auto_random prefix values can be 1 or 2, so we skip this check with tidb backend
    if [ "$backend" != 'tidb' ]; then
      run_sql "select count(distinct id >> 58) as count from auto_random.t"
      check_contains "count: 2"
    fi

    # auto random base is 4
    run_sql "SELECT max(id & b'000001111111111111111111111111111111111111111111111111111111111') >= $NEXT_AUTO_RAND_VAL as ge FROM auto_random.t"
    check_contains 'ge: 0'
    run_sql "INSERT INTO auto_random.t VALUES ();"
    run_sql "SELECT max(id & b'000001111111111111111111111111111111111111111111111111111111111') >= $NEXT_AUTO_RAND_VAL as ge FROM auto_random.t"
    check_contains 'ge: 1'
done

function run_for_auro_random_data2() {
    create_table=$1
    run_sql 'DROP DATABASE IF EXISTS auto_random;'
    run_sql 'CREATE DATABASE IF NOT EXISTS auto_random;'
    run_sql "$create_table"
    run_lightning --backend $backend -d "$CUR/data2"
    run_sql 'select count(*) as count from auto_random.t where c > 0'
    check_contains "count: 2"
    run_sql 'select count(*) as count from auto_random.t where a=1 and b=11'
    check_contains "count: 1"
    run_sql 'select count(*) as count from auto_random.t where a=2 and b=22'
    check_contains "count: 1"
}

for backend in tidb local; do
    if [ "$backend" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi

    run_for_auro_random_data2 'create table auto_random.t(c bigint auto_random primary key, a int, b int)'
    run_for_auro_random_data2 'create table auto_random.t(a int, b int, c bigint auto_random primary key)'
    # composite key and auto_random is the first column
    run_for_auro_random_data2 'create table auto_random.t(c bigint auto_random, a int, b int, primary key(c, a))'
    # composite key and auto_random is not the first column
    run_for_auro_random_data2 'create table auto_random.t(a int, b int, c bigint auto_random, primary key(c, a))'
done
