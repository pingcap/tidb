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

run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.USERTABLE1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.USERTABLE1 VALUES (\"a\", \"b\");"
run_sql "INSERT INTO $DB.USERTABLE1 VALUES (\"aa\", \"b\");"

# backup table with upper name
echo "backup start..."
run_br --pd $PD_ADDR backup table --db "$DB" --table "USERTABLE1" -s "local://$TEST_DIR/$DB"

run_sql "DROP DATABASE $DB;"

# restore table with upper name success
echo "restore start..."
run_br --pd $PD_ADDR restore table --db "$DB" --table "USERTABLE1" -s "local://$TEST_DIR/$DB"

table_count=$(run_sql "use $DB; show tables;" | grep "Tables_in" | wc -l)
if [ "$table_count" -ne "1" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
