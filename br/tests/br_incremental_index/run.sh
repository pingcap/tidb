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
    run_sql "INSERT INTO ${DB}.${TABLE} VALUES ($i);"
done

# full backup
echo "backup full start..."
run_sql "CREATE INDEX idx_c1 ON ${DB}.${TABLE}(c1)"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/full"
wait
# run ddls
echo "run ddls..."
run_sql "ALTER TABLE ${DB}.${TABLE} ADD COLUMN c2 INT NOT NULL;";
run_sql "ALTER TABLE ${DB}.${TABLE} ADD COLUMN c3 INT NOT NULL;";
run_sql "ALTER TABLE ${DB}.${TABLE} DROP COLUMN c3;";
# incremental backup
echo "incremental backup start..."
last_backup_ts=$(run_br validate decode --field="end-version" -s "local://$TEST_DIR/$DB/full" | grep -oE "^[0-9]+")
run_br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB/inc" --db $DB -t $TABLE --lastbackupts $last_backup_ts

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
run_br restore table --db $DB --table $TABLE -s "local://$TEST_DIR/$DB/inc" --pd $PD_ADDR
row_count_inc=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')
# check full restore
if [ "${row_count_inc}" != "${ROW_COUNT}" ];then
    echo "TEST: [$TEST_NAME] incremental restore fail on database $DB"
    exit 1
fi
run_sql "INSERT INTO ${DB}.${TABLE} VALUES (1, 1);"
row_count_insert=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')
# check insert count
if [ "${row_count_insert}" != "$(expr $row_count_inc + 1)" ];then
    echo "TEST: [$TEST_NAME] insert record fail on database $DB"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
