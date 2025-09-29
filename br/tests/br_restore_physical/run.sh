#!/bin/sh
#
# Copyright 2025 PingCAP, Inc.
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
. run_services
CUR=$(cd `dirname $0`; pwd)

# const value
PREFIX="pitr_restore_physical" # NOTICE: don't start with 'br' because `restart services` would remove file/directory br*.
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"
TASK_NAME="br_pitr_restore_physical"

echo "=== Starting restore_physical test ==="

restart_services

# prepare the data
echo "=== Preparing test data ==="
## statistic tables
run_sql "create database test_stats"
run_sql "create table test_stats.t1 (id int, a int, b int, primary key (id), key ia (a))"
run_sql "insert into test_stats.t1 (id, a, b) with recursive nums(n) AS (select 1 union all select n+1 from nums where n < 1000) select n, n * 1000, n * 1000000 as id from nums"
run_sql "analyze table test_stats.t1"
run_sql "explain select * from test_stats.t1 where a = 3"
run_sql "explain select * from test_stats.t1 where a = 4"

## privilege tables
run_sql "create user 'test'@'%' identified by ''"
run_sql "create database test_priv"
run_sql "create table test_priv.t1 (id int, a int, b int, primary key (id), key ia (a))"
run_sql "insert into test_priv.t1 values (1, 10, 100), (2, 20, 200), (3, 30, 300)"
run_sql "grant select on test_priv.t1 to 'test'"
run_sql "create database test_priv_db"
run_sql "create table test_priv_db.t1 (id int, a int, b int, primary key (id), key ia (a))"
run_sql "insert into test_priv_db.t1 values (1, 10, 100), (2, 20, 200), (3, 30, 300)"
run_sql "grant select on test_priv_db.* to 'test'"
run_sql "flush privileges"

## binding
run_sql "create database test_bind_info"
run_sql "create table test_bind_info.t1 (id int, a int, b int, primary key (id), key ia (a))"
run_sql "insert into test_bind_info.t1 values (1, 10, 100), (2, 20, 200), (3, 30, 300)"
run_sql "create global binding for select * from test_bind_info.t1 where a = 123 using select * from test_bind_info.t1 ignore index (ia) where a = 123"

# run snapshot backup
echo "=== Running snapshot backup ==="
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full"

restart_services

# run snapshot restore
echo "=== Running snapshot restore ==="
echo "run snapshot restore"
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$PREFIX/full" --fast-load-sys-tables > $res_file 2>&1
check_not_contains "Fallback to logically load "

# check
echo "=== Verifying restore results ==="
## check statistic data
run_sql "SELECT * FROM mysql.stats_buckets buckets JOIN INFORMATION_SCHEMA.TABLES tables ON buckets.table_id = tables.TIDB_TABLE_ID WHERE tables.TABLE_SCHEMA = 'test_stats' and tables.TABLE_NAME = 't1' limit 5;"
check_contains "1. row"
### TODO: remove if tidb support reload stats
###run_sql "explain select * from test_stats.t1 where a = 3"
###run_sql "explain select * from test_stats.t1 where a = 3"
###check_not_contains "stats:pseudo"

## check privilege tables
run_user_sql test "select a, b from test_priv.t1 where a = 10;"
check_contains "1. row"
run_user_sql test "select * from test_priv_db.t1 where a = 10"
check_contains "1. row"

## check binding
run_sql "admin reload bindings"
run_sql "explain select * from test_bind_info.t1 where a = 123"
check_contains "TableFullScan"

echo "=== Test completed successfully ==="
