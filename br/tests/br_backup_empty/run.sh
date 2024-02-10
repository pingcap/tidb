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

# backup empty.
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/empty_db"
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on backup empty cluster!"
    exit 1
fi

# restore empty.
echo "restore start..."
run_br restore full -s "local://$TEST_DIR/empty_db" --pd $PD_ADDR --ratelimit 1024
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on restore empty cluster!"
    exit 1
fi

# backup and restore empty tables.
run_sql "CREATE DATABASE $DB;"
run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

<<<<<<< HEAD
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/empty_table"

run_sql "DROP DATABASE $DB;"
echo "restore start..."
=======
# backup empty table
echo "backup empty table start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/empty_table"

while [ $i -le $DB_COUNT ]; do
    run_sql "DROP DATABASE $DB$i;"
    i=$(($i+1))
done

# restore empty table.
echo "restore empty table start..."
>>>>>>> 7f88e73af6 (br: skip template system table __TiDB_BR_Temporary_mysql when backup (#41000))
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/empty_table"

# insert one row to make sure table is restored.
run_sql "INSERT INTO $DB.usertable1 VALUES (\"a\", \"b\");"

<<<<<<< HEAD
run_sql "DROP DATABASE $DB"
=======
while [ $i -le $DB_COUNT ]; do
    run_sql "DROP DATABASE $DB$i;"
    i=$(($i+1))
done


# backup, skip temporary system database(__TiDB_BR_Temporary_mysql) when backup
run_sql "CREATE DATABASE __TiDB_BR_Temporary_mysql";
run_sql "CREATE TABLE __TiDB_BR_Temporary_mysql.tables_priv(id int);";
echo "backup and skip __TiDB_BR_Temporary_mysql start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/skip_temporary_mysql"

# restore successfully without panic.
run_sql "DROP DATABASE __TiDB_BR_Temporary_mysql";
echo "restore the data start..."
run_br restore full -s "local://$TEST_DIR/skip_temporary_mysql" --pd $PD_ADDR --ratelimit 1024
>>>>>>> 7f88e73af6 (br: skip template system table __TiDB_BR_Temporary_mysql when backup (#41000))
