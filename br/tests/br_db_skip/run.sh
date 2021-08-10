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

run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable1 VALUES (\"a\", \"b\");"
run_sql "INSERT INTO $DB.usertable1 VALUES (\"aa\", \"b\");"

# backup db
echo "backup start..."
run_br --pd $PD_ADDR backup db --db "$DB" -s "local://$TEST_DIR/$DB"

run_sql "DROP DATABASE $DB;"

run_sql "CREATE DATABASE $DB;"
# restore db with skip-create-sql must failed
echo "restore start but must failed"
fail=false
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --no-schema=true || fail=true
if $fail; then
    # Error: [schema:1146]Table 'br_db_skip.usertable1' doesn't exist
    echo "TEST: [$TEST_NAME] restore $DB with no-schema must failed"
else
    echo "TEST: [$TEST_NAME] restore $DB with no-schema not failed"
    exit 1
fi


run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

echo "restore start must succeed"
fail=false
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --no-schema=true || fail=true
if $fail; then
    echo "TEST: [$TEST_NAME] restore $DB with no-schema failed"
    exit 1
else
    echo "TEST: [$TEST_NAME] restore $DB with no-schema succeed"
fi

table_count=$(run_sql "use $DB; show tables;" | grep "Tables_in" | wc -l)
if [ "$table_count" -ne "1" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
