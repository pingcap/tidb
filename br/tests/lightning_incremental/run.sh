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

check_cluster_version 4 0 0 "incremental restore" || exit 0

DB_NAME=incr

for backend in importer local; do
  run_sql "DROP DATABASE IF EXISTS incr;"
  run_sql "DROP DATABASE IF EXISTS lightning_metadata;"
  run_lightning --backend $backend

  # check metadata table is not exist
  run_sql "SHOW DATABASES like 'lightning_metadata';"
  check_not_contains "Database: lightning_metadata"

  for tbl in auto_random pk_auto_inc rowid_uk_inc uk_auto_inc; do
    run_sql "SELECT count(*) from incr.$tbl"
    check_contains "count(*): 3"
  done

  for tbl in auto_random pk_auto_inc rowid_uk_inc uk_auto_inc; do
    if [ "$tbl" = "auto_random" ]; then
      run_sql "SELECT id & b'000001111111111111111111111111111111111111111111111111111111111' as inc FROM incr.$tbl"
    else
      run_sql "SELECT id as inc FROM incr.$tbl"
    fi
    check_contains 'inc: 1'
    check_contains 'inc: 2'
    check_contains 'inc: 3'
  done

  for tbl in pk_auto_inc rowid_uk_inc; do
    run_sql "SELECT group_concat(v) from incr.$tbl group by 'all';"
    check_contains "group_concat(v): a,b,c"
  done

  run_sql "SELECT sum(pk) from incr.uk_auto_inc;"
  check_contains "sum(pk): 6"

  # incrementally import all data in data1
  run_lightning --backend $backend -d "tests/$TEST_NAME/data1"

  # check metadata table is not exist
  run_sql "SHOW DATABASES like 'lightning_metadata';"
  check_not_contains "Database: lightning_metadata"

  for tbl in auto_random pk_auto_inc rowid_uk_inc uk_auto_inc; do
    run_sql "SELECT count(*) from incr.$tbl"
    check_contains "count(*): 6"
  done

  for tbl in auto_random pk_auto_inc rowid_uk_inc uk_auto_inc; do
    if [ "$tbl" = "auto_random" ]; then
      run_sql "SELECT id & b'000001111111111111111111111111111111111111111111111111111111111' as inc FROM incr.$tbl"
    else
      run_sql "SELECT id as inc FROM incr.$tbl"
    fi
    check_contains 'inc: 4'
    check_contains 'inc: 5'
    check_contains 'inc: 6'
  done

  for tbl in pk_auto_inc rowid_uk_inc; do
    run_sql "SELECT group_concat(v) from incr.$tbl group by 'all';"
    check_contains "group_concat(v): a,b,c,d,e,f"
  done

  run_sql "SELECT sum(pk) from incr.uk_auto_inc;"
  check_contains "sum(pk): 21"
done
