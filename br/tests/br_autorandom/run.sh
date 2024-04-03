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
DB="$TEST_NAME"

# prepare the data
echo "prepare the data"
run_sql "CREATE DATABASE ${DB};"
run_sql "CREATE TABLE ${DB}.common (a BIGINT UNSIGNED AUTO_RANDOM(1), b VARCHAR(255), uid INT, c VARCHAR(255) DEFAULT 'c', PRIMARY KEY (a, b), UNIQUE INDEX (uid));"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 1, 'a');"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 2, 'a');"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 3, 'a');"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 4, 'a');"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 5, 'a');"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 6, 'a');"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 7, 'a');"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 8, 'a');"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 9, 'a');"
run_sql "INSERT INTO ${DB}.common (b, uid, c) values ('a', 10, 'a');"

run_sql "CREATE TABLE ${DB}.pk (a BIGINT UNSIGNED AUTO_RANDOM(1), uid INT, c VARCHAR(255) DEFAULT 'c', PRIMARY KEY (a), UNIQUE INDEX (uid));"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (1, 'a');"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (2, 'a');"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (3, 'a');"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (4, 'a');"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (5, 'a');"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (6, 'a');"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (7, 'a');"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (8, 'a');"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (9, 'a');"
run_sql "INSERT INTO ${DB}.pk (uid, c) values (10, 'a');"

# backup & restore
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB-full"
run_sql "DROP DATABASE $DB;"
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB-full"

# new workload
for i in `seq 1 9`; do
  run_sql "INSERT INTO ${DB}.common (b, uid) values ('a', 10) on duplicate key update c = 'b';"
  run_sql "INSERT INTO ${DB}.pk (uid) values (10) on duplicate key update c = 'b';"
done

# check consistency
run_sql "SELECT COUNT(*) AS RESCNT FROM ${DB}.common WHERE uid < 10 AND c = 'b';"
check_contains "RESCNT: 0"
run_sql "SELECT COUNT(*) AS RESCNT FROM ${DB}.pk WHERE uid < 10 AND c = 'b';"
check_contains "RESCNT: 0"

# clean the data
run_sql "DROP DATABASE $DB;"
