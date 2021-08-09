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
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu
DB="$TEST_NAME"
TABLE="usertable"
ROW_COUNT=10
PATH="tests/$TEST_NAME:bin:$PATH"

insertRecords() {
    for i in $(seq $1); do
        run_sql "INSERT INTO $DB.$TABLE VALUES ('$i');"
    done
}

createTable() {
    run_sql "CREATE TABLE IF NOT EXISTS $DB.$TABLE (c1 CHAR(255));"
}

echo "load data..."
echo "create database"
run_sql "CREATE DATABASE IF NOT EXISTS $DB;"
echo "create table"
createTable
echo "insert records"
insertRecords $ROW_COUNT

row_count_ori=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

# backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB"

run_sql "DROP DATABASE $DB;"

# restore full
echo "restore start..."
run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

row_count_new=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

fail=false
if [ "${row_count_ori}" != "${row_count_new}" ];then
    fail=true
    echo "TEST: [$TEST_NAME] fail on database $DB"
fi
echo "database $DB [original] row count: ${row_count_ori}, [after br] row count: ${row_count_new}"

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

# insert records
insertRecords $ROW_COUNT
row_count_insert=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')
fail=false
if [ "${row_count_insert}" != "$(expr $row_count_new \* 2)" ];then
    fail=true
    echo "TEST: [$TEST_NAME] fail on inserting records to database $DB after restore: ${row_count_insert}"
fi

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
