#!/bin/sh
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

set -eu
DB="$TEST_NAME"
TABLE="usertable"
ROW_COUNT=100
PATH="tests/$TEST_NAME:bin:$PATH"

echo "load data..."
# create database
run_sql "CREATE DATABASE IF NOT EXISTS $DB;"
# create table
run_sql "CREATE TABLE IF NOT EXISTS ${DB}.${TABLE} (c1 INT);"
# insert records
for i in $(seq $ROW_COUNT); do
    run_sql "INSERT INTO ${DB}.${TABLE}(c1) VALUES ($i);"
done

# full backup
echo "full backup start..."
run_br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB/full" --db $DB -t $TABLE
# run ddls
echo "run ddls..."
run_sql "RENAME TABLE ${DB}.${TABLE} to ${DB}.${TABLE}1;"
run_sql "DROP TABLE ${DB}.${TABLE}1;"
run_sql "DROP DATABASE ${DB};"
run_sql "CREATE DATABASE ${DB};"
run_sql "CREATE TABLE ${DB}.${TABLE}1 (c2 CHAR(255));"
run_sql "RENAME TABLE ${DB}.${TABLE}1 to ${DB}.${TABLE};"
run_sql "TRUNCATE TABLE ${DB}.${TABLE};"

# create new table to test alter succeed after rename ddl executed.
run_sql "CREATE TABLE IF NOT EXISTS ${DB}.${TABLE}_rename (c CHAR(255));"
run_sql "RENAME TABLE ${DB}.${TABLE}_rename to ${DB}.${TABLE}_rename2;"
# insert records
for i in $(seq $ROW_COUNT); do
    run_sql "INSERT INTO ${DB}.${TABLE}(c2) VALUES ('$i');"
    run_sql "INSERT INTO ${DB}.${TABLE}_rename2(c) VALUES ('$i');"
done
# incremental backup
echo "incremental backup start..."
last_backup_ts=$(run_br validate decode --field="end-version" -s "local://$TEST_DIR/$DB/full" | grep -oE "^[0-9]+")
run_br --pd $PD_ADDR backup db -s "local://$TEST_DIR/$DB/inc" --db $DB --lastbackupts $last_backup_ts

run_sql "DROP DATABASE $DB;"
# full restore
echo "full restore start..."
run_br restore table --db $DB --table $TABLE -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR
row_count_full=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')
# check full restore
if [ "${row_count_full}" != "${ROW_COUNT}" ];then
    echo "TEST: [$TEST_NAME] full restore fail on database $DB"
    exit 1
fi
# incremental restore
echo "incremental restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB/inc" --pd $PD_ADDR
row_count_inc=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')
# check full restore
if [ "${row_count_inc}" != "${ROW_COUNT}" ];then
    echo "TEST: [$TEST_NAME] incremental restore fail on database $DB"
    exit 1
fi
run_sql "INSERT INTO ${DB}.${TABLE}(c2) VALUES ('1');"
run_sql "INSERT INTO ${DB}.${TABLE}_rename2(c) VALUES ('1');"

run_sql "DROP DATABASE $DB;"
