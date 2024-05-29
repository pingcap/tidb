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
DB="$TEST_NAME"
DB_COUNT=10

# backup empty.
echo "backup empty cluster start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/empty_cluster"
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on backup empty cluster!"
    exit 1
fi

# restore empty.
echo "restore empty cluster start..."
run_br restore full -s "local://$TEST_DIR/empty_cluster" --pd $PD_ADDR --ratelimit 1024
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on restore empty cluster!"
    exit 1
fi

# backup and restore empty tables.
i=1
while [ $i -le $DB_COUNT ]; do
    run_sql "CREATE DATABASE $DB$i;"
    i=$(($i+1))
done

echo "backup empty db start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/empty_db"
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on backup empty cluster!"
    exit 1
fi

while [ $i -le $DB_COUNT ]; do
    run_sql "DROP DATABASE $DB$i;"
    i=$(($i+1))
done

# restore empty.
echo "restore empty db start..."
run_br restore full -s "local://$TEST_DIR/empty_db" --pd $PD_ADDR --ratelimit 1024
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on restore empty cluster!"
    exit 1
fi

run_sql "CREATE TABLE ${DB}1.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

echo "backup empty table start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/empty_table"

while [ $i -le $DB_COUNT ]; do
    run_sql "DROP DATABASE $DB$i;"
    i=$(($i+1))
done

echo "restore empty table start..."
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/empty_table"

# insert one row to make sure table is restored.
run_sql "INSERT INTO ${DB}1.usertable1 VALUES (\"a\", \"b\");"

while [ $i -le $DB_COUNT ]; do
    run_sql "DROP DATABASE $DB$i;"
    i=$(($i+1))
done
