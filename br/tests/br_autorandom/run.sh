#!/bin/bash
#
# Copyright 2024 PingCAP, Inc.
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
PREFIX="autorandom" # NOTICE: don't start with 'br' because `restart services` would remove file/directory br*.
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"

# start a new cluster
echo "restart a services"
restart_services

# prepare the data
echo "prepare the data"
run_sql "CREATE TABLE test.common (a BIGINT UNSIGNED AUTO_RANDOM(1), b VARCHAR(255), uid INT, c VARCHAR(255) DEFAULT 'c', PRIMARY KEY (a, b), UNIQUE INDEX (uid));"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 1, 'a');"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 2, 'a');"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 3, 'a');"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 4, 'a');"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 5, 'a');"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 6, 'a');"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 7, 'a');"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 8, 'a');"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 9, 'a');"
run_sql "INSERT INTO test.common (b, uid, c) values ('a', 10, 'a');"

run_sql "CREATE TABLE test.pk (a BIGINT UNSIGNED AUTO_RANDOM(1), uid INT, c VARCHAR(255) DEFAULT 'c', PRIMARY KEY (a), UNIQUE INDEX (uid));"
run_sql "INSERT INTO test.pk (uid, c) values (1, 'a');"
run_sql "INSERT INTO test.pk (uid, c) values (2, 'a');"
run_sql "INSERT INTO test.pk (uid, c) values (3, 'a');"
run_sql "INSERT INTO test.pk (uid, c) values (4, 'a');"
run_sql "INSERT INTO test.pk (uid, c) values (5, 'a');"
run_sql "INSERT INTO test.pk (uid, c) values (6, 'a');"
run_sql "INSERT INTO test.pk (uid, c) values (7, 'a');"
run_sql "INSERT INTO test.pk (uid, c) values (8, 'a');"
run_sql "INSERT INTO test.pk (uid, c) values (9, 'a');"
run_sql "INSERT INTO test.pk (uid, c) values (10, 'a');"

# backup & restore
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full"
echo "restart a services"
restart_services
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$PREFIX/full"

# new workload
for i in `seq 1 9`; do
  run_sql "INSERT INTO test.common (b, uid) values ('a', 10) on duplicate key update c = 'b';"
  run_sql "INSERT INTO test.pk (uid) values (10) on duplicate key update c = 'b';"
done

# check consistency
run_sql "SELECT COUNT(*) AS RESCNT FROM test.common WHERE uid < 10 AND c = 'b';"
check_contains "RESCNT: 0"
run_sql "SELECT COUNT(*) AS RESCNT FROM test.pk WHERE uid < 10 AND c = 'b';"
check_contains "RESCNT: 0"
